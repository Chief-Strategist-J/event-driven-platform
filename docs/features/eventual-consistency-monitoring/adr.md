# Architectural Decision Record: Eventual Consistency Monitoring, Drift Detection & Self-Healing Verification (WP-14)

## 1. Title & Metadata
* **Status**: PROPOSED
* **Date**: 2026-11-06
* **Decision Makers**: Platform Architecture Guild, Distributed Systems Reliability Board, Chief Data Architect
* **Governing Specification**: [Eventual Consistency Monitoring — Business Logic Decision Tree (v2.1)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/eventual-consistency-monitoring/business-logic.md)

---

## 2. Context & Problem Statement
In an asynchronous event-driven system, projections and materialized views (such as those maintained by `WP-13`) decouple read models from transactional write engines. While eventual consistency provides high throughput and fault tolerance, it creates a dangerous operational blind spot: **silent divergence between the write model of record and derived query views**.

Standard operational monitoring (such as Prometheus consumer lag metrics) measures only *queue ingestion transit delays*. It completely fails to detect:
1. **Silent Projection Bugs (F18.1, CORRUPT)**: A projection handler applies events without throwing exceptions, but calculates mathematical rollups or state mutations incorrectly. Consumer lag remains 0ms while the view data is corrupted.
2. **False Green Through Masked Failures (F16.3, LOSS)**: Comparison routines catching query timeouts or network partitions fail-open, marking unverified records as `MATCH`.
3. **Representation & Precision Mismatches (F17.4, DRIFT)**: Floating-point conversions, timestamp timezone normalizations, or UUID casing variations cause monitoring storms or mask genuine precision loss.
4. **XOR Checksum Collisions (F17.5, CORRUPT)**: Combining row hashes using bitwise XOR allows duplicate rows to cancel each other out ($A \oplus A = 0$), masking duplicate record insertions.
5. **Monitor Blindness & Circular Outages (F16.1, F16.2, OUTAGE)**: When the monitoring engine shares infrastructure (Kafka, DB replica, network) with the data pipeline, the outage that corrupts data also silences the monitor.
6. **Heal Cascades & Infinite Thrashing (F21.1, F21.2)**: Automated healing loops executing without version gates overwrite valid newer state or trigger feedback storms during consumer database failover.
7. **Cardinality Explosions (F23.1, OUTAGE)**: Large-scale incidents that cause millions of keys to drift exhaust memory in the monitoring service if tracked at per-key granularity.

To guarantee that read views reflect the authoritative write state within contractual SLOs, the platform requires an independent, continuous, multi-tiered consistency verification engine.

---

## 3. Decision Drivers & Architecture Invariants (EC-01 to EC-19)

### Foundational Invariants (EC-01 to EC-11)
* **EC-01: Passive Watermark Observation [Certain]**:
  Watermarks track the minimum monotonic event timestamp or sequence across all partitions for a given consumer group:
  $$\text{Watermark}(V) = \min_{p \in \text{Partitions}(V)} (\text{CommittedOffset}(p))$$
  Freshness lag is measured against real-time clock: $L_{\text{freshness}} = T_{\text{wall}} - T_{\text{event}}(\text{Watermark})$.
* **EC-02: Deterministic Drift Taxonomy [Certain]**:
  Discrepancies are categorized strictly into 5 deterministic classes:
  - `GHOST`: Record exists in derived view but absent in authoritative write model.
  - `MISSING`: Record exists in write model but absent in derived view.
  - `ATTRIBUTE_MISMATCH`: Record exists in both stores, but payload fields diverge.
  - `VERSION_LAG`: Record in view reflects an older monotonic sequence but is actively catching up within transit SLA.
  - `MANUAL_WRITE`: Mutation performed outside authorized pipeline credentials (violates EC-17).
* **EC-03: Freshness SLO Decomposition [Certain]**:
  Total view lag must be broken down into observable segments:
  $$T_{\text{total\_lag}} = T_{\text{outbox\_transit}} + T_{\text{kafka\_transit}} + T_{\text{consumer\_decode}} + T_{\text{store\_write}}$$
* **EC-04: Non-Blocking Probes [Certain]**:
  Verification probes and sampling sweeps must execute in read-committed isolation without holding table-level locks or degrading online transaction processing (OLTP) write lanes.
* **EC-05: Monotonic Clock Telemetry [Certain]**:
  All transit durations, probe timeouts, and latency measurements must use monotonic timers (`clock_gettime(CLOCK_MONOTONIC)`), preventing negative latency anomalies caused by NTP jumps or VM pauses (F17.8).
* **EC-06: Bounded Tracker Cardinality [Certain]**:
  Per-key drift tracking is strictly bounded to a hard memory limit (default 100,000 keys per view). When breached, the tracker switches automatically to bucketed range aggregation (F23.1).
* **EC-07: Stratified Sampling Distribution [Certain]**:
  Sample checks must cover every tenant tier, stream partition, and storage shard evenly, preventing hotspots from concealing isolated partition stalls (F16.5).
* **EC-08: In-Flight Tolerance Window [Certain]**:
  An observed version mismatch is quarantined within a configurable tolerance window ($T_{\text{tolerance}} = 2 \times \text{FreshnessSLO}$). Drift is only confirmed if the discrepancy persists after the tolerance window expires.
* **EC-09: Idempotent, Rate-Limited Self-Healing [Certain]**:
  Automated healing operations are strictly version-guarded (`WHERE current_version == expected_version`), bounded by a token-bucket rate limiter, and halted if repeat heals on a key exceed 2 in 24 hours (F21.1).
* **EC-10: Single-Source Topology Routing [Certain]**:
  Verification engines adhere to the active region routing topology registry, preventing false drift alerts against dormant or lagging disaster recovery (DR) replicas (F24.1).
* **EC-11: Canonical Serialization & Strict Currency Representation [Certain]**:
  Data normalization rules enforce:
  - Financial/currency values must be compared in integer minor units (e.g., cents, satoshis) — never as floating-point numbers.
  - Timestamps normalized to UTC microsecond precision (`RFC3339Nano`).
  - JSON keys sorted alphabetically with trimmed whitespace.
  - UUIDs converted to canonical lowercase without hyphen variations.

### Addendum Invariants (EC-12 to EC-19)
* **EC-12: Three-Valued Logic: MATCH | MISMATCH | UNKNOWN [Certain]**:
  A check outcome can only evaluate to `MATCH`, `MISMATCH`, or `UNKNOWN`. `UNKNOWN` is never counted as `MATCH`. Any network timeout, partial shard response, degraded replica, or comparison error immediately evaluates to `UNKNOWN` (F16.3).
* **EC-13: Verification Coverage as an SLO [Certain]**:
  Verification coverage is an explicitly tracked service level objective:
  $$\text{Coverage SLO} = \frac{\text{Verified Keyspace in Window}}{\text{Total Registered Keyspace}} \ge 99.5\%$$
  A drop in coverage triggers a P2 alert even if 0 mismatches are reported.
* **EC-14: Active Check Registration Enforcement [Certain]**:
  The view registry (`WP-13`) and monitor configuration are reconciled continuously. Any registered view lacking active probe configuration triggers a P1 alert (F16.7).
* **EC-15: External Dead-Man's Switch [Certain]**:
  The monitoring service publishes an invariant heartbeat to an independent, external alerting provider every 60 seconds. Absence of the heartbeat triggers an on-call page, guaranteeing detection even if internal networks, Kafka, or local AlertManager fail completely (F16.1).
* **EC-16: Independent Business Invariant Verification [Certain]**:
  Consistency checks verify structural equality; business invariant checks verify domain validity regardless of pipeline parity. Invariants (e.g., $\sum \text{Postings} = 0$, $\text{Paid} \le \text{Authorized}$, $\text{OnHand} \ge 0$) execute on both the write model and derived views independently (F18.1, §20).
* **EC-17: Detection of Direct Out-of-Pipeline Writes [Certain]**:
  Database audit triggers and CDC event filters flag any row modifications originating from human or non-pipeline database roles, tagging the record with drift class `MANUAL_WRITE` and blocking automated overwrites (F19.1).
* **EC-18: Canary Representative Coverage & Downstream Quarantine [Certain]**:
  Synthetic canary events must route to every partition across all active streams. Canary events carry an immutable `is_canary = true` header and isolated tenant IDs (`canary_*`) to prevent pollution of business reporting, revenue metrics, or external customer notifications (F22.1, F22.3, F22.4).
* **EC-19: Enforced Expiry on Silences and Disables [Certain]**:
  Any alert silence or disabled check requires an explicit owner identity and a mandatory time-to-live ($TTL \le 7 \text{ days}$). Expired silences cannot be auto-renewed without documented peer review (F19.2).

---

## 4. Considered Architectural Options

### Option A: Brute-Force Synchronous Full-Table Scans
* **Description**: Periodically execute batch queries (`SELECT * FROM write_model FULL OUTER JOIN view_model`) across all databases.
* **Drawbacks**: Exhausts database memory, creates read lock contention, consumes network bandwidth on multi-million row tables, and degrades user-facing query throughput.
* **Verdict**: REJECTED.

### Option B: Pure Metric-Based Queue Lag Tracking
* **Description**: Rely solely on Kafka consumer offset lag reported by Burrow or Prometheus exporter.
* **Drawbacks**: Completely blind to silent projection code bugs, partial consumer writes, outbox table stalls, manual SQL updates, and data corruption where offsets advance despite faulty logic.
* **Verdict**: REJECTED.

### Option C: Multi-Tiered Verification Engine (Chosen Architecture)
* **Tier 1 (Continuous Watermarking)**: High-frequency passive consumer offset and LSN lag tracking to enforce Freshness SLOs.
* **Tier 2 (Stratified Canary Probes)**: End-to-end active probe injection cycling synthetic events across every stream partition with immediate transit-time verification.
* **Tier 3 (Merkle Bucket Range Sweepers)**: Background range checksum verification using ordered modular sums and hash chaining to verify large datasets with $O(\log N)$ network overhead.
* **Tier 4 (Domain Invariant Assertions)**: Asynchronous domain rule execution enforcing ledger balancing and state constraints independently of event arrival.
* **Verdict**: APPROVED.

---

## 5. System Architecture & Component Design

```
+-----------------------------------------------------------------------------------+
|                   EVENTUAL CONSISTENCY MONITORING SUBSYSTEM                       |
|                                                                                   |
|  +--------------------+    +----------------------+    +-----------------------+  |
|  |   Canary Prober    |    |  Watermark Tracker   |    | Merkle Range Sweeper  |  |
|  |  (Active Synthetics|    |  (Passive Offset/LSN |    | (Bucket Hashes & Deep |  |
|  |   Across Partitions|    |   Freshness Latency) |    |  Three-Valued Comp.)  |  |
|  +---------+----------+    +----------+-----------+    +-----------+-----------+  |
|            |                          |                            |              |
|            +-------------------+      |      +---------------------+              |
|                                |      |      |                                    |
|                                v      v      v                                    |
|                  +--------------------------------------+                         |
|                  |     Three-Valued Logic Comparator    |                         |
|                  |     MATCH | MISMATCH | UNKNOWN       |                         |
|                  +------------------+-------------------+                         |
|                                     |                                             |
|                     +---------------+---------------+                             |
|                     |                               |                             |
|                     v                               v                             |
|       +---------------------------+   +---------------------------+               |
|       |   Drift Incident Engine   |   |   External Dead-Man's     |               |
|       | (Taxonomy, Quarantines,   |   |        Switch             |               |
|       |  Rate-Limited Heal Bus)   |   |   (Independent Egress)    |               |
|       +-------------+-------------+   +-------------+-------------+               |
+---------------------|-------------------------------|-----------------------------+
                      v                               v
          +-----------------------+       +-----------------------+
          | PostgreSQL Storage    |       | External Paging Alert |
          | - Watermark Ledger    |       | Service (PagerDuty/   |
          | - Drift Incidents     |       | OpsGenie Dead-Man)    |
          | - Check Run History   |       +-----------------------+
          | - Silence Registry    |
          +-----------------------+
```

### 5.1 Three-Valued Logic Truth Table
The comparator evaluates source record $S$ and target view record $T$ with strict handling of error boundaries:

| Source State | Target State | Error / Timeout / Partial | Result | Classification |
| :--- | :--- | :--- | :--- | :--- |
| Present ($v_s$) | Present ($v_t = v_s$) | None | `MATCH` | None (Consistent) |
| Present ($v_s$) | Present ($v_t < v_s$) | None (Within $T_{\text{tolerance}}$) | `MATCH` | `VERSION_LAG` (Pending catchup) |
| Present ($v_s$) | Present ($v_t < v_s$) | None (Past $T_{\text{tolerance}}$) | `MISMATCH` | `VERSION_LAG` (Stalled) |
| Present ($v_s$) | Present ($v_t \ne v_s$, payload diff) | None | `MISMATCH` | `ATTRIBUTE_MISMATCH` |
| Present | Absent | None | `MISMATCH` | `MISSING` |
| Absent | Present | None | `MISMATCH` | `GHOST` |
| Any | Any | Shard Timeout / DB Error | **`UNKNOWN`** | `UNKNOWN_EVALUATION` |
| Stale Replica | Any | Replica Lag > Max Bound | **`UNKNOWN`** | `UNKNOWN_STALE_SOURCE` |

### 5.2 Merkle Bucket Checksum Algorithm (Avoiding XOR Cancellation F17.5)
To prevent hash cancellation from duplicate rows, bucket checksums compute an ordered composite 64-bit modular sum combined with SHA-256:

$$\text{BucketHash} = \text{SHA256} \left( \sum_{i=1}^{N} \text{Hash}(K_i, V_i) \pmod{2^{64}} \;\parallel\; N \;\parallel\; \bigoplus_{i=1}^{N} \text{Hash}(K_i) \right)$$

Where $N$ is the exact record count. A duplicate record insertion alters both $N$ and the modular sum, guaranteeing detection.

### 5.3 Automated Self-Healing State Machine
To guarantee safety against heal loops (F21.1) and failover storms (F21.2), self-healing transitions through strict gates:

```
[MISMATCH DETECTED]
         |
         v
[TOLERANCE_WINDOW_WAIT] (Re-check after T_tolerance)
         |
    +----+----------------------------+
    |                                 |
 (Resolved)                     (Persistent)
    |                                 v
[DISCARD]                   [VERSION_GUARD_CHECK]
                                      |
                     +----------------+----------------+
                     |                                 |
              (Version Match)                   (Version Stale / Conflict)
                     v                                 v
          [RATE_LIMIT_TOKEN_ACQUIRE]           [QUARANTINE_FOR_MANUAL]
                     |
         +-----------+-----------+
         |                       |
     (Acquired)              (Exhausted)
         v                       v
[EXECUTE_IDEMPOTENT_HEAL]   [DEFER_TO_DEADLINE_QUEUE]
         |
         v
[RECORD_HEAL_ATTEMPT]
 (If attempts > 2 in 24h -> ESCALATE_P1_INCIDENT)
```

---

## 6. PostgreSQL Persistence Model (Migration `0017`)
The database schema introduces four dedicated relational tables supporting the consistency monitoring domain:

1. **`consistency_watermark_ledger`**:
   Tracks partition-level high-water marks, consumer commit offsets, and computed freshness latencies per view.
2. **`consistency_drift_incident_log`**:
   Records all verified discrepancies, taxonomy classification (`GHOST`, `MISSING`, `ATTRIBUTE_MISMATCH`, etc.), quarantine timestamps, and resolution states.
3. **`consistency_check_run_history`**:
   Audits verification sweep executions, duration, coverage metrics, and the three-valued outcome ratio (`match_count`, `mismatch_count`, `unknown_count`).
4. **`consistency_view_silence_registry`**:
   Enforces owner identity and mandatory non-renewable TTL on alert silences (EC-19).

---

## 7. Consequences & Trade-offs

### Positive
- **Zero Silent Data Corruption**: Independent three-valued logic and invariant checkers ensure faulty projection handlers cannot hide behind green consumer group lag metrics.
- **Resilience Against Circular Dependencies**: External dead-man's switch and independent alert egress ensure catastrophic platform outages trigger pages immediately.
- **Deterministic Drift Classification**: On-call engineers receive pinpoint diagnostics (`ATTRIBUTE_MISMATCH` with field diff vs `VERSION_LAG`) rather than generic discrepancy warnings.
- **Bounded Resource Utilization**: Range bucket hashing and bounded memory trackers prevent monitoring services from exhausting memory during mass incident storms.

### Negative & Mitigations
- **Additional Storage & Network I/O**: Range bucket sweeps and canary event injections consume secondary database IOPS.
  - *Mitigation*: Sweepers throttle execution dynamically based on target database CPU utilization and replica lag.
- **Eventual Consistency False Alarms**: In-flight events during high throughput can appear as momentary drift.
  - *Mitigation*: Enforce mandatory hysteresis tolerance windows ($T_{\text{tolerance}}$) before promoting candidate drift into confirmed incidents.

---

## 8. Go-Live Test Matrix (§25)
Before deployment, the implementation must pass 100% of the following verification scenarios:
1. **Kill AlertManager**: External dead-man's switch pages within 60s of missed heartbeat.
2. **Corrupted Shard / Timeout**: Injected query timeout evaluates strictly to `UNKNOWN`, never `MATCH`.
3. **Duplicate Row Insertion**: Verify Merkle bucket checksum changes (proving zero XOR cancellation).
4. **Direct Manual SQL Modification**: Triggers audit classifier, producing `MANUAL_WRITE` incident.
5. **Silence Without Expiry**: Registry rejects silence creation lacking owner or valid future expiration.
6. **Storm Simulation (1M drifted keys)**: Memory footprint remains bounded; tracker successfully switches to bucket mode.
7. **Repeat Healing Prevention**: Injected persistent failure aborts after 2 heal attempts and opens P1 incident.
8. **Negative Latency Invariance**: Host clock backwards skew produces `UNKNOWN` freshness status rather than negative lag.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

In asynchronous, event-driven architectures, read models (materialized views, search indexes, analytics caches) lag behind the primary transactional database by design. However, if a network partition, slow database query, or consumer deadlock causes lag to grow unboundedly, users view hours-old data, customer support takes erroneous actions, and downstream consistency contracts break silently without error logs.

### 2. Internal Algorithmic Mechanics & State Invariants

The monitoring subsystem calculates real-time Eventual Consistency Scores ($C_{\text{score}}$) by combining two metrics: (1) Broker Offset Lag ($L_{\text{offset}} = O_{\text{log\_end}} - O_{\text{committed}}$) and (2) Wall-Clock Latency Gap ($T_{\text{lag}} = T_{\text{now}} - T_{\text{event\_timestamp}}$). If $T_{\text{lag}}$ breaches service SLOs (e.g. > 5 seconds), the engine triggers an alert and updates an operational health registry. Downstream query gateways can inspect this registry to gracefully flag read models as stale.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Wall-Clock Propagation Lag SLI (Drift Quantification)
* **Design Philosophy & Epistemology**:
  Eventual consistency is a reasonable architectural trade-off; unmeasured, unbounded inconsistency is operational negligence. If a system claims to be 'eventually consistent', it must explicitly define what 'eventually' means in wall-clock seconds. The philosophy is Quantitative Drift Visibility: measure and enforce the exact time delta between physical reality and projected state.
* **Threat Vector Defeated**: Silent data divergence where downstream materialized views fall days behind the primary database without triggering platform alerts.
* **Storage & State Invariant**:
```text
DriftLatency = WallClockTime(ProjectionCommitted) - EventOccurredTimestamp
Alert(Severity_Medium) if DriftLatency > 5s
Alert(Severity_High) if DriftLatency > 30s
```
* **Implementation Mechanism**:
  Consumer projectors record the delta between the event's `occurred_at` timestamp and the current system time upon completing the local projection commit. Histograms export p95 and p99 propagation latency to Prometheus and Grafana dashboards.
* **Non-Negotiable Trade-Off**: Requires strict clock synchronization across servers via Chrony / NTP to prevent false-positive drift alerts.

---

#### Principle 2: Autonomous Reconcilers & Continuous Drift Detection
* **Design Philosophy & Epistemology**:
  Networks drop messages, consumers crash during rebalances, and edge-case bugs cause silent divergence. Assuming that an event stream perfectly synchronizes two databases forever is naive. The philosophy is Continuous Background Auditing: autonomous reconciler daemons must periodically cross-audit primary databases against projected read models to catch and fix drift.
* **Threat Vector Defeated**: Undetected state drift where read models display incorrect balances or missing orders due to dropped events or unhandled projection errors.
* **Storage & State Invariant**:
```text
Hash(PrimaryAggregateState[ID]) == Hash(ProjectedViewState[ID])
Drift detected ==> Trigger Automated Compensating Ingestion
```
* **Implementation Mechanism**:
  A background reconciler job hashes primary aggregate records and compares them against read-model projection hashes using sliding temporal windows. Upon finding a hash mismatch, the reconciler generates a corrective domain sync event to heal the projection.
* **Non-Negotiable Trade-Off**: Background database query load incurred by running continuous cross-database reconciliation audits.

---

#### Principle 3: SLA-Driven Circuit Breaking on Consumer Inconsistency
* **Design Philosophy & Epistemology**:
  Serving stale data is acceptable in certain domains; serving wildly corrupted data that is hours out of date causes catastrophic business decisions. The philosophy is Fail-Safe Read Circuit Breaking: if a read model's consistency lag breaches business SLAs, the query interface must either degrade to reading from primary storage or explicitly return HTTP 503 / Stale Warnings.
* **Threat Vector Defeated**: Business users making catastrophic real-world decisions (e.g., trading, shipping goods) based on deeply stale read-model projections.
* **Storage & State Invariant**:
```text
If ReadModelLag > MaxAcceptableSLA (e.g., 60s) ==> QueryAPI returns Status(DEGRADED) with 'X-Data-Stale: true'
```
* **Implementation Mechanism**:
  The query handler checks the read model's `last_updated_at` timestamp against current time before returning results. If lag exceeds the configured threshold, the gateway routes read traffic directly to the primary database replica or attaches a prominent stale warning header.
* **Non-Negotiable Trade-Off**: Risk of overloading the primary transactional database if read traffic fails over during severe projection outages.

---

#### Principle 4: Mathematical Convergence over Immediate Correctness
* **Design Philosophy & Epistemology**:
  In an eventual consistency model, temporary divergence is normal. Attempting to freeze writes or acquire distributed locks across systems destroys throughput. The philosophy is Deterministic Convergence: design state transitions using Conflict-Free Replicated Data Types (CRDTs) or monotonic versioning so that regardless of event arrival order, the system mathematically converges to the identical state.
* **Threat Vector Defeated**: Permanent state divergence where different read replicas arrive at conflicting final states when processing events in different orders.
* **Storage & State Invariant**:
```text
State(Consumer_A) == State(Consumer_B) after all events in stream S are consumed, regardless of permutation
```
* **Implementation Mechanism**:
  Projections utilize idempotent, commutative state mutators (e.g., Last-Write-Wins based on aggregate version, monotonic counters, set additions). State updates do not depend on intermediate states.
* **Non-Negotiable Trade-Off**: Domain logic constraints: business models must be formulated in terms of commutative deltas rather than absolute state overwrites.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **How does an engineering team determine what acceptable 'eventual' consistency latency is?**
   * *Answer*: It is determined by business impact analysis: what is the monetary or operational cost if a user sees data that is 500ms old? 5 seconds old? 5 minutes old? For e-commerce catalog views, 5 seconds is acceptable; for payment fraud detection, lag must be under 500ms.
2. **Why is running a periodic SQL reconciliation query better than relying exclusively on event stream guarantees?**
   * *Answer*: Event stream guarantees (at-least-once, FIFO per partition) protect the transit layer, but cannot protect against consumer application logic bugs (e.g., an unhandled exception inside a conditional branch) that silently skip state updates. Background reconciliation provides ground-truth validation.
3. **What should the UI display when a user updates their profile and the read model is still lagging?**
   * *Answer*: The client application should apply optimistic local UI updates, displaying the new name immediately while showing a subtle 'syncing...' status until the background projection catches up and confirms the update.

---
## Authoritative References & Production Citations

* **[Werner Vogels: Eventually Consistent - Revisited (ACM Communications 2009)](https://cacm.acm.org/magazines/2009/12/55743-eventually-consistent/fulltext)**
  - *Architectural Rationale & Implementation Grounding*: Seminal paper by Amazon CTO Werner Vogels defining consistency models, convergence guarantees, and modern trade-offs.
* **[Pat Helland: Life beyond Distributed Transactions: an Apostate's Opinion (2007)](https://queue.acm.org/detail.cfm?id=3025012)**
  - *Architectural Rationale & Implementation Grounding*: Foundational treatise arguing against two-phase commit (2PC) in favor of eventual consistency and idempotent entity partitioning.
* **[Apache Kafka KIP-430: Exposing Consumer Group Lag Metrics via Admin API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430)**
  - *Architectural Rationale & Implementation Grounding*: Kafka architectural improvement standardizing programmatic offset lag calculation and consumer group monitoring.
* **[Google SRE Book: Service Level Objectives (SLOs)](https://sre.google/sre-book/service-level-objectives/)**
  - *Architectural Rationale & Implementation Grounding*: Authoritative guidelines on establishing, measuring, and alerting on latency-based and freshness-based SLOs.

## Amendment 2026-11-07: Heal on unknown incident returns 404; canary probe no longer fakes MATCH

### Problem observed
1. `POST .../incidents/{id}/heal` for a nonexistent incident returned `200` with `healed:true`.
2. `POST .../probes/canary` always returned outcome `MATCH` and wrote nothing.

### Root cause
1. `ConsistencyService.AttemptHealing` called `EvaluateRepeatHeal(0, maxAttempts)` with a hard-coded 0, never loaded the incident, discarded the status-update error (`_ =`, RC3), and the Postgres `UpdateDriftIncidentStatus` ignored zero affected rows. Because the attempt count was always 0 and written back as the constant 1, the max-attempts / `MANUAL_INTERVENTION` branch (invariant on repeat-heal loop prevention, F21.1) was unreachable.
2. `DispatchCanaryProbe` was a stub (RC4): it measured a zero duration and returned `OutcomeMatch` with no evidence.

### Decision / fix
- New repository method `GetDriftIncident` (query `FlowSelectDriftIncidentByID`, scoped by tenant) returns `ports.ErrIncidentNotFound` when absent. `AttemptHealing` loads the incident, evaluates the policy with the persisted `heal_attempts`, writes `heal_attempts + 1` (or moves to `MANUAL_INTERVENTION` keeping the current count when the limit is reached), and returns update errors. `UpdateDriftIncidentStatus` now returns `ErrIncidentNotFound` on 0 rows affected, which also closes the race where the incident disappears between read and write.
- `DispatchCanaryProbe` now records a `CANARY_PROBE` row in `consistency_check_run_history` (`status` COMPLETED, 1 evaluated key, 1 unknown, coverage computed with 0 matched) and returns outcome `UNKNOWN`. Rationale (three-valued logic, section 5.1): without delivery evidence the honest answer is UNKNOWN, never MATCH. `cdc_canary_reconciliation_log` is deliberately not used because it has an FK to `cdc_connector_inventory.connector_name`, which a generic probe cannot satisfy.

### API-visible behavior change
- Heal unknown incident: `200 healed:true` -> `404`.
- Heal now actually advances `heal_attempts`; once the limit is hit the response is not-healed and the incident becomes `MANUAL_INTERVENTION`.
- Canary probe outcome: `MATCH` -> `UNKNOWN` (**breaking** for clients that gated on MATCH). A history row is written.

### Verification
- Live: heal on a random incident id returned 404; canary returned `UNKNOWN` and the history row was created.
- Unit tests in `src/features/eventual-consistency-monitoring/tests/eventual_consistency_monitoring_service_test.go`: `TestAttemptHealing_NotFound`, `TestAttemptHealing_UsesPersistedAttempts`, `TestDispatchCanaryProbe_RecordedAndNotClaimedMatch`.

### Remaining gaps
- The canary does not publish an event or observe it downstream; no reconciliation step turns UNKNOWN into MATCH/MISMATCH yet. The service comment says a later step must confirm arrival.
- Watermark/incidents/sweeps GET routes still return `200 data:null` without a tenant parameter (out of scope, not fixed).


## Amendment 2026-11-07 (second pass): tenant scope required on reads

`GET /consistency/views/{view}/watermark|incidents|sweeps` require `tenant_id` (400 otherwise); the silent `default` tenant fallback was removed. Open: `POST /consistency/incidents/{id}/heal` still defaults `tenant_id` to `default`.

Verified live on the compose-deployed image (`chiefj/event-driven-platform:v2`) and by unit tests; see ADR-0002 "Closed in a second pass".

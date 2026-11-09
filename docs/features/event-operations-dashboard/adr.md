# Architectural Decision Record (ADR): Event Operations Dashboard Engine

## Status
Accepted

## Context
Operators and on-call engineers require a centralized, production-grade observability and triage surface for event-driven workflows. Per `OD-01` to `OD-10`, observability systems must:
1. Distinguish between alerts (which page) and dashboards (which investigate).
2. Avoid false greens when telemetry is stale or absent (`OD-02`: "No data is never displayed as healthy").
3. Display worst-case latencies/lags rather than misleading averages (`OD-03`).
4. Prevent operational mutations directly inside Grafana/dashboards (`OD-06`: action links must route to the Control Plane `C-CTL`).
5. Ensure zero customer payload/PII exposure in telemetry stores (`OD-07`).
6. Persist operational metadata (incident hops, top-N held keys, pipeline status, and triage runbook links) in an authoritative, sharded ledger.

## Decision
1. **Pipeline Hop Architecture (L0–L4 Information Architecture)**:
   - Model the event streaming pipeline into discrete hops: H1 (Outbox/WAL), H2 (Relay), H3 (Kafka Brokers), H4 (Consumers), H5 (Authoritative Views), and H6 (Derived Stores).
   - Track health states: `OK`, `LAGGING`, `STALLED`, `STOPPED`, `PAUSED` (with explicit reason: `BREAKER_OPEN`, `BACKPRESSURE`, `OPERATOR_HALT`, `REBALANCE`), and `UNKNOWN` (when metrics exceed staleness thresholds).
2. **PostgreSQL Ledgers (`05-db-writing-rules.md`)**:
   - `ops_pipeline_hop_ledger`: Tracks per-hop operational metrics, time-lag seconds, oldest held key age, status, and linked runbooks.
   - `ops_incident_triage_log`: Append-only, tamper-evident record of triage investigations, localized broken hops, root-cause classifications, and C-CTL action link referrals.
3. **Data Protection Invariant (OD-07)**:
   - Metadata-only indexing. Customer payload bytes or sensitive PII are strictly rejected from entering operations ledgers or span attributes.
4. **Hexagonal Architecture Compliance**:
   - Outbound ports in `infrastructure/ports/`, concrete PostgreSQL adapters in `infrastructure/adapters/`, and pure domain evaluation rules in `domain/rules/`.

## Consequences
- **Positive**: Strict decoupling of metric aggregation from operational actions; fail-closed triage recommendations; complete audit trail for incident analysis.
- **Trade-off**: Requires periodic hop status reporting and metadata synchronization from running services.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Operating event-driven platforms blind to consumer lag, partition skew, and worker thread pool saturation leads to sudden, catastrophic outages. Because streaming systems decouple publishers from consumers, publishers report 100% success while consumers silently fall hours behind, violating customer SLAs without generating a single HTTP error code.

### 2. Internal Algorithmic Mechanics & State Invariants

The dashboard subsystem scrapes Prometheus metrics from the chassis admin port (:8081/metrics). It evaluates the Four Golden Signals across three distributed tiers: (1) Broker Tier (partition ISR, under-replicated partitions), (2) Pipeline Tier (consumer lag offsets, commit rates, processing duration), and (3) Infrastructure Tier (DB connection pools, memory buffers). Real-time alerts trigger on lag derivative slopes rather than static thresholds.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Multi-Dimensional Lag & Saturation Visibility
* **Design Philosophy & Epistemology**:
  Monitoring CPU and memory is completely inadequate for event-driven systems; a consumer pod can show 10% CPU usage while being completely deadlocked and falling hours behind reality. The philosophy is Consumer Lag Supremacy: operational health is measured strictly by partition lag, message consumption velocity, and processing latency.
* **Threat Vector Defeated**: Silent system failure where consumer instances appear healthy in Kubernetes but have stopped processing events, accumulating millions of backlogged messages.
* **Storage & State Invariant**:
```text
ConsumerHealth == (LagRate <= MaxAcceptableThreshold) AND (ProcessingVelocity >= ProductionVelocity)
CPU/Memory usage is secondary telemetry
```
* **Implementation Mechanism**:
  The platform runs Prometheus exporters scraping Kafka consumer group offsets (`kafka_consumergroup_lag`). The operations dashboard highlights partition-level skew, alerting when lag on any single partition exceeds 1,000 messages or age exceeds 60 seconds.
* **Non-Negotiable Trade-Off**: Scraping overhead on Kafka brokers when polling consumer group offset metrics across thousands of dynamic topics.

---

#### Principle 2: End-to-End Latency SLI Tracking (Producer-to-Consumer)
* **Design Philosophy & Epistemology**:
  Measuring only consumer execution time hides the latency that matters most to the business: how long does it take for a real-world event to propagate through the entire distributed platform? The philosophy is Wall-Clock Propagation Measurement: measure end-to-end latency from the moment the event occurred to the moment downstream read models are updated.
* **Threat Vector Defeated**: Unnoticed business SLA breaches where individual microservices report 5ms processing times but customers wait 15 minutes for order confirmations.
* **Storage & State Invariant**:
```text
EndToEndLatency = WallClockTime(Consumer_Acknowledge) - EventTimestamp(Producer_Creation)
Alert if EndToEndLatency > BusinessSLA (e.g., 2000ms)
```
* **Implementation Mechanism**:
  Every event carries a high-resolution RFC 3339 UTC timestamp stamped at initial producer creation. The consumer subtracts this timestamp upon completing its local database commit, emitting the delta as a Prometheus histogram metric (`event_e2e_latency_seconds`).
* **Non-Negotiable Trade-Off**: Requires strict Network Time Protocol (NTP) synchronization across all server nodes to prevent clock drift from skewing latency metrics.

---

#### Principle 3: Synthetic Canary Heartbeat Probing
* **Design Philosophy & Epistemology**:
  Absence of error logs is not proof of system health; it often means the event pipeline has silently stalled and no events are flowing. The philosophy is Active Probing: continuously inject synthetic canary events into the pipeline to verify end-to-end flow, alerting immediately if a canary is not consumed within SLA boundaries.
* **Threat Vector Defeated**: Undetected pipeline stoppage where broken network routes or deadlocked poll loops halt event flow without generating error logs.
* **Storage & State Invariant**:
```text
CanaryFrequency == 30s
CanaryAlert fires if CanaryLatency > 60s OR CanaryLost == TRUE
```
* **Implementation Mechanism**:
  A background canary service emits synthetic heartbeat events (`SystemCanaryEmitted`) every 30 seconds into production topics. Downstream canary listeners intercept the events, calculate transit time, and touch a Prometheus gauge. If the gauge is not updated within 60 seconds, an alert fires.
* **Non-Negotiable Trade-Off**: Synthetic event noise in event topics that must be filtered out of analytical reports and downstream projections.

---

#### Principle 4: Role-Based Operational Action Safety
* **Design Philosophy & Epistemology**:
  Giving operators write access to reset consumer offsets or purge topics directly through dashboards without safety checks invites catastrophic operational accidents. The philosophy is Guarded Operational Interventions: all dashboard interventions (offset rewinds, DLQ replaying, partition pausing) must be role-gated, dry-runnable, and audit-logged.
* **Threat Vector Defeated**: Catastrophic data loss or accidental mass event reprocessing caused by an operator pressing the wrong button on an operations dashboard.
* **Storage & State Invariant**:
```text
Action requires Multi-Factor Auth + Explicit Confirmation + AuditLog(Operator, Action, Target, Timestamp)
DryRun executed and displayed prior to live execution
```
* **Implementation Mechanism**:
  The dashboard backend enforces OpenID Connect (OIDC) role-based permissions. Destructive actions require entering the target topic name explicitly, displaying a preview of affected offsets and estimated lag impact before executing through the admin client.
* **Non-Negotiable Trade-Off**: Added friction for engineers during urgent production incident remediation.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is monitoring 'messages per second' alone a misleading metric for event platform health?**
   * *Answer*: A consumer processing 10,000 messages/sec looks healthy, but if the producer is generating 20,000 messages/sec, the consumer is falling behind at a rate of 10,000 messages/sec. Consumer lag and end-to-end processing age are the only true indicators of health.
2. **How does the dashboard differentiate between a consumer that is legitimately idle versus one that is crashed?**
   * *Answer*: It checks Kafka consumer group coordinator state: an idle consumer maintains active heartbeats with zero partition lag, while a crashed consumer drops out of the consumer group coordinator or accumulates rising partition lag.
3. **What is the danger of resetting a consumer group offset to 'earliest' on a 50-million-event production topic?**
   * *Answer*: It forces downstream consumers to reprocess 50 million historical events, overloading downstream databases, triggering duplicate side effects, and causing massive customer-facing latency until the backlog clears.

---
## Authoritative References & Production Citations

* **[Google Site Reliability Engineering: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)**
  - *Architectural Rationale & Implementation Grounding*: The industry-standard treatise defining the Four Golden Signals and symptom-based alerting strategies.
* **[Prometheus Metric Exposition Format and Best Practices](https://prometheus.io/docs/practices/instrumentation/)**
  - *Architectural Rationale & Implementation Grounding*: Official standard for structuring counters, gauges, and histograms for high-throughput distributed systems.
* **[Brendan Gregg: The USE Method (Utilization, Saturation, and Errors)](https://www.brendangregg.com/usemethod.html)**
  - *Architectural Rationale & Implementation Grounding*: Methodology for analyzing system and resource performance bottlenecks across CPU, memory, network, and storage.
* **[Confluent: Monitoring Apache Kafka Consumer Lag](https://www.confluent.io/blog/monitor-kafka-consumer-lag-without-impacting-performance/)**
  - *Architectural Rationale & Implementation Grounding*: Technical guide on calculating, monitoring, and alerting on consumer lag using offset commit watermarks.

## Amendment 2026-11-07: Triage localization for healthy flows

### Problem observed
`POST /api/v1/ops/triage/localize` for a flow with no degraded hop (healthy or unknown) returned `400`. The request itself was valid.

### Root cause
`rules.LocalizeBrokenHop` returned an empty C-CTL action link for the all-healthy case. The persisted triage record (`ops_incident_triage`) has constraint `ck_ops_incident_triage_link_btrim`, which rejects an empty link; the insert failed and the database error surfaced as 400 (RC2: validation delegated to a DB CHECK).

### Decision / fix
`LocalizeBrokenHop` now returns the constant `rules.HealthyCCTLActionLink` = `/control-plane/consumers/status` for the healthy case. It is a non-empty, read-only control-plane link, so the triage record satisfies the constraint and operators get a harmless pointer instead of nothing. We kept the DB constraint (it protects the "every triage points at a control action" invariant) and fixed the producer of the value.

### API-visible behavior change
Healthy/unknown flow: `400` -> `200` with `localized_hop` `NONE` and the default link above. Degraded flows are unchanged.

### Verification
Live re-test returned 200 with localized_hop NONE. Unit test updated in `src/features/event-operations-dashboard/tests/event_operations_dashboard_unit_test.go`.

### Remaining gaps
The default link is a fixed string, not checked against an actual control-plane route (unverified that the path exists).

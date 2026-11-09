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

### 3. Core Architectural Principles

* **Four Golden Signals Focus**: Monitoring prioritizes Latency, Traffic, Errors, and Saturation over vanity counters to gauge actual user-perceived platform health.
* **Leading Indicator Alerting**: Alerts trigger on leading signals (consumer lag acceleration, queue depth) before downstream error rates spike.
* **Multi-Tenant Observability**: Metrics allow slicing by tenant ID to isolate noisy-neighbor resource hogging and SLA violations immediately.
* **Zero-Contention Metrics Collection**: Telemetry counters use lock-free atomic primitives, ensuring instrumentation never degrades critical path event processing.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is consumer lag slope more critical than absolute lag count? (A consumer with 10,000 lag that is actively processing down is healthy; a consumer with 1,000 lag that is falling behind by 100 messages/sec is in active failure).**
2. **How do we observe multi-tenant resource consumption without exploding metric cardinality? (Tenant metrics are categorized by tier or hashed into bounded label buckets, preventing Prometheus memory exhaustion).**
3. **Why must monitoring endpoints run on an isolated admin port (:8081)? (If the public HTTP server (:8080) saturates or deadlocks under load, operators must still access health and metrics endpoints on an isolated port).**

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

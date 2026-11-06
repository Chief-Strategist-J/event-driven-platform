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

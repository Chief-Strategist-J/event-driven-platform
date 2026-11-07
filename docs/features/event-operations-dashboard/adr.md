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

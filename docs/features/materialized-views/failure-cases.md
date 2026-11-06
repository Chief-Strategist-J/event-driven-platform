# Failure Cases & Forensic Remediation Matrix: Materialized Views (WP-13)

This failure catalog documents all failure modes across the materialized view lifecycle, covering edge cases, race conditions, storage crashes, and data drift per the governing specification.

---

## 1. Failure Taxonomy Matrix

| Failure ID | Failure Mode | Severity | Invariant Violated | Detection Mechanism | Automated / Operational Remediation |
|---|---|---|---|---|---|
| **F2.1** | View Definition Schema Drift | MEDIUM | MV-12 | Migration checksum mismatch in CI / boot validator | Refuse boot if view registry schema hash diverges from code definitions. |
| **F2.2** | Invalid View Class Specification | LOW | MV-12 | Handler request validation schema | Reject with `400 VALIDATION_FAILED`; enforce enum whitelist (`V_STATE`, `V_AGG`, `V_JOIN`, `V_SEARCH`, `V_KV`, `V_REPORT`). |
| **F3.1** | Stale / Out-of-Order Event Arrival | HIGH | MV-01 | Monotonic sequence check (`incoming_seq <= current_version`) | If hash matches: drop as duplicate `DUPLICATE_NOOP`. If hash differs: drop as stale `STALE_DROP`. Do not mutate projection state. |
| **F3.2** | Event Gap / Sequence Jump | CRITICAL | MV-01 | Gap condition (`incoming_seq > current_version + 1`) | Divert incoming event to `materialized_view_held_events` with `held_until = NOW() + 10m`. Return `GAP_HOLD`. Trigger consumer partition catchup. |
| **F4.1** | Zero-Row Atomic Update Race | HIGH | MV-02 | Rows affected check on SQL update (`rows == 0`) | Map to `409 CONFLICT`. Re-read aggregate version and retry projection evaluation under optimistic concurrency loop. |
| **F4.2** | Concurrent Dual-Writer Race | HIGH | MV-03 | PostgreSQL unique constraint on `(tenant_id, view_name, aggregate_type, aggregate_id)` | Database constraint catches race; trigger error mapper; retry with exponential jitter. |
| **F5.1** | Primary Store Unreachable (Postgres Down) | CRITICAL | MV-04 | Database health probe & circuit breaker trip | Circuit breaker enters OPEN state; pause Kafka consumer partitions to avoid uncommitted message lag; shed non-critical view queries. |
| **F5.2** | Secondary Store Divergence (Redis/OpenSearch) | MEDIUM | MV-05 | Periodic background reconciliation worker | Wipe secondary index partition and stream re-projection directly from primary PostgreSQL view tables. |
| **F6.1** | Consistency Token Read Lag | MEDIUM | MV-06 | Token comparison (`requested_seq > current_version`) | Route query to primary database read replica or wait up to `timeout_budget_ms` before falling back to strong read with warning header. |
| **F7.1** | Non-Deterministic Projection Function | CRITICAL | MV-07 | Shadow replay divergence check | Reject impure functions (system clocks, UUID generation) in CI unit test suite via AST linting. |
| **F8.1** | Write-Back Recursion Violation | CRITICAL | MV-08 | Hexagonal boundary enforcement (no outbox emission in projection handler) | Code review gate and static import analyzer forbid importing command producers into view projection handlers. |
| **F9.1** | Freshness SLO Breach | MEDIUM | MV-09 | OpenTelemetry gauge (`lag_ms > freshness_slo_ms`) | Emit PagerDuty alert; mark view registry status as `DEGRADED`; route traffic to cached fallbacks. |
| **F10.1** | Shadow Replay Crash During Rebuild | HIGH | MV-10 | Rebuild log phase timeout monitor | State machine remains in `HISTORICAL_REPLAY`; restart cursor from last committed checkpoint; do not touch active table. |
| **F10.2** | Catchup LSN Lag Overflow | HIGH | MV-10 | LSN distance evaluation (`current_lsn - shadow_lsn > threshold`) | Throttle background replay worker to prevent saturated disk I/O; keep active table live until delta $< 100$ records. |
| **F10.3** | Atomic Swap Failure During Promotion | CRITICAL | MV-10 | Transaction rollback on table rename / pointer swap | Rollback promotion transaction; active table remains undisturbed; log failure and mark phase as `HISTORICAL_REPLAY` for retry. |
| **F11.1** | Cryptographic Shredding Data Leakage | CRITICAL | MV-11 | Tenant deletion audit scanner | Delete all rows matching `tenant_id` across registry, state, held events, and rebuild logs in a cascading transaction. |
| **F14.1** | Disk Exhaustion from Shadow Table Growth | HIGH | MV-10 | Node disk threshold monitor ($> 85\%$) | Abort rebuild; transition state machine to `DECOMMISSIONED_OLD`; truncate shadow table to reclaim disk space. |
| **F14.2** | Quarantine Queue Saturation | HIGH | MV-01 | Count query on `materialized_view_held_events` | When held events exceed $10,000$, alert on partition stall and route unresolvable gaps to Dead Letter Queue (DLQ). |

---

## 2. Forensic Scenarios & Execution Blueprints

### Scenario 1: Gap Quarantine & Recovery Lifecycle
1. Event sequence 1 arrives $\rightarrow$ Applied (`current_version = 1`).
2. Event sequence 3 arrives (Event 2 delayed in Kafka network retry) $\rightarrow$ Monotonicity guard evaluates `3 > 1 + 1`.
3. Event sequence 3 is quarantined into `materialized_view_held_events` with `expected_seq = 2`, `received_seq = 3`.
4. Event sequence 2 arrives $\rightarrow$ Applied (`current_version = 2`).
5. Application checks held events queue for `aggregate_id` where `received_seq == 3`.
6. Event sequence 3 is dequeued, evaluated (`3 == 2 + 1`), applied, and removed from the quarantine table.

### Scenario 2: Blue/Green View Rebuild Without Query Downtime
1. Operator or migration initiates rebuild via `POST /api/v1/materialized-views/rebuild`.
2. Rebuild log records `phase = INITIATED`, creates target shadow table `{view_name}_shadow`.
3. Historical worker transitions to `HISTORICAL_REPLAY`, backfilling snapshot events up to `target_lsn`.
4. Once historical replay completes, worker transitions to `STREAM_CATCHUP`, consuming delta events from Kafka.
5. When lag reaches near-zero, worker transitions to `PROMOTED`: an atomic transaction renames `{view_name}_shadow` $\rightarrow$ `{view_name}` while demoting the old table.
6. Worker decommissions and drops the old table, transitioning to `DECOMMISSIONED_OLD`.

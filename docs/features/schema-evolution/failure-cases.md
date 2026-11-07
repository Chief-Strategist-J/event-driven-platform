# Failure Cases & Forensic Remediation Matrix: Schema Evolution & Upcasting (WP-16)

This failure catalog documents all failure modes across the event schema evolution lifecycle, covering deserialization crashes, upcaster memory leaks, semantic scale drift, circular dependency chains, and dual-publishing split-brain states per the governing specification.

---

## 1. Failure Taxonomy Matrix

| Failure ID | Failure Mode | Severity | Invariant Violated | Detection Mechanism | Automated / Operational Remediation |
|---|---|---|---|---|---|
| **F1.1** | Breaking Structural Mutation on Active Subject | OUTAGE | SE-04 | CI Schema Compatibility Linter | Reject PR merge immediately; require topic forking (`.v2`) and upcaster registration. |
| **F1.2** | Producer Deployed Before Consumers under BACKWARD | OUTAGE | SE-04 | Consumer deserialization error spike | Automated canary rollback; enforce deployment order: all consumers migrate before producers switch. |
| **F1.3** | New Field Added Without Default Value | CORRUPT | SE-04 | Schema syntax validator | Enforce mandatory `default` clause on all new optional fields; reject null-default fields without explicit nullable type. |
| **F2.1** | Non-Atomic Dual-Publishing Split-Brain | CORRUPT | SE-05 | Transactional Outbox verification probe | Both v1 and v2 event payloads MUST be inserted in the identical database transaction; dual-writing via separate HTTP/Kafka calls is forbidden. |
| **F2.2** | Premature Topic Decommissioning | LOSS | SE-06 | Consumer lag monitor on legacy topic | Decommissioning script verifies 7 consecutive days of 0 consumer lag across all registered consumer groups before archiving topic. |
| **F2.3** | Infinite Dual-Publishing Resource Exhaustion | DEGRADE | SE-06 | Dual-publish ledger TTL scanner | Alert on dual-publishing windows exceeding 30 days; escalate unmigrated consumer groups to owning engineering teams. |
| **F3.1** | Missing Upcaster in Historical Replay | CORRUPT | SE-02 | Upcaster registry lookup on event arrival | If event version is older than projection target and no upcaster exists, route to DLQ and trigger P1 alert. |
| **F3.2** | Impure Upcaster with Blocking I/O | OUTAGE | SE-10 | AST static analysis linter in CI | Forbid database queries, HTTP calls, or clock calls inside upcasters; upcasters must be pure in-memory functions $f(V_i) \to V_{i+1}$. |
| **F3.3** | Circular Upcaster Dependency Chain | OUTAGE | SE-02 | Directed Acyclic Graph (DAG) validator | Validate upcaster graph at boot time; fail application initialization if any cycle ($v_1 \to v_2 \to v_1$) is detected. |
| **F3.4** | Upcaster Execution Exception Panic | OUTAGE | SE-08 | Panic recovery wrapper in stream decoder | Catch unhandled panics inside upcaster; isolate corrupt message to Dead-Letter Queue (`WP-3`); advance consumer cursor. |
| **F4.1** | Semantic Scale Drift (Unit Mismatch 100x) | CORRUPT | SE-09 | Value distribution anomaly monitor | Changing units (e.g. dollars to cents) requires renaming field (e.g. `amount_usd` to `amount_cents`) or major version fork. |
| **F4.2** | Tolerant Reader Missing Critical Field | CORRUPT | SE-03 | Model schema validation gate | Consumer domain validation validates presence of all core projection fields after tolerant reading; missing required fields fail fast. |
| **F4.3** | Schema Registry Network Outage at Consumer Boot | OUTAGE | SE-07 | Local schema cache with fallback | Consumers maintain local in-memory and disk-backed cache of registered schemas; continue operating offline during registry hiccups. |

---

## 2. Forensic Scenarios & Execution Blueprints

### Scenario 1: Historical Replay Over Incompatible Aggregate Redesign (F3.1, SE-02)
1. In 2024, `OrderCreated` event was emitted with schema v1:
   ```json
   { "order_id": "ord_100", "total_price": 150.00, "customer_name": "Jane Doe" }
   ```
2. In 2026, the domain refactors to schema v3:
   - v2 split `customer_name` into `first_name` and `last_name`.
   - v3 converted `total_price` float into `total_price_cents` integer.
3. A new analytics read projection is deployed requiring full replay from offset 0.
4. **Recursive Upcaster Execution**:
   - Replay reader reads raw byte payload from storage ($v=1$).
   - Passes payload to `UpcastV1ToV2`:
     ```json
     { "order_id": "ord_100", "total_price": 150.00, "first_name": "Jane", "last_name": "Doe" }
     ```
   - Passes output to `UpcastV2ToV3`:
     ```json
     { "order_id": "ord_100", "total_price_cents": 15000, "first_name": "Jane", "last_name": "Doe" }
     ```
   - Feed resulting v3 object into the modern projection handler.
   - **Underlying physical event log remains 100% immutable and untouched**.

### Scenario 2: Trapping Semantic Unit Shift (F4.1, SE-09)
1. Producer refactors `ExecutionLatency` from seconds to milliseconds.
2. If the field remains named `latency: float`, Avro/Protobuf linters approve the change (type is identical).
3. Downstream consumer calculates SLA breaches: a 2-second task (now emitted as `2000.0`) is interpreted as a 2000-second outage.
4. **Remediation & Guard Protocol**:
   - Semantic policy mandates renaming to `latency_ms: int64`.
   - CI linter identifies removal of `latency` and addition of `latency_ms` without default $\rightarrow$ flags as breaking change.
   - Producer creates `v2` topic with dual-publishing and provides explicit upcaster:
     ```go
     func UpcastLatencyV1ToV2(v1 EventV1) EventV2 {
         return EventV2{
             LatencyMs: int64(v1.Latency * 1000),
         }
     }
     ```


## 3. Amendment 2026-11-07: Failure Modes Added

| Failure ID | Failure Mode | Severity | Invariant Violated | Detection Mechanism | Response / Operator Action |
|---|---|---|---|---|---|
| **F-SE-A1** | Breaking schema registered unchecked (fixed) | OUTAGE | SE-04 | `CheckSchemaCompatibility` at `POST /api/v1/schemas/register` | `409` listing violations such as `BACKWARD: new required field "x" has no default (vs version N)`. Fix the schema (add a default / keep the field) or fork the subject. |
| **F-SE-A2** | Non-monotonic version | DEGRADE | SE-04 | `ErrNonMonotonicVersion` | `409`; re-register with a higher version. |
| **F-SE-A3** | Duplicate fingerprint | LOW | SE-04 | `ErrDuplicateSchemaFingerprint` | `409`; the schema already exists in the lineage. |
| **F-SE-A4** | Unknown compatibility mode | LOW | n/a | `ErrInvalidCompatibilityMode` | `400`; use one of the six modes. `NONE` is not accepted. |
| **F-SE-A5** | Unparseable / non-JSON definition | MEDIUM | SE-04 | Not detected: comparison is skipped | Registration passes without semantic check; review such schemas manually (open gap). |
| **F-SE-A6** | Type widening flagged as break | LOW | n/a | Strict canonical-JSON type equality | `409`; add a new field or fork the subject instead of changing a type. |

Migration 0028 rollback: run `0028_..._per_tenant.rollback.sql`; it fails if two tenants already share a topic_name, migration_id or rebuild_id (resolve duplicates first).

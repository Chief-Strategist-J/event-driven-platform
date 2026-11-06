# CDC Integration — Failure Catalog & Mitigation Strategy

This catalog documents failure modes across the Change Data Capture (CDC) lifecycle, derived from the business logic decision tree and platform invariants (`CDC-01` to `CDC-12`).

---

## 1. Severity Taxonomy

- **LOSS**: Irreversible loss of committed events, historical audit records, or snapshot data.
- **CORRUPT**: Silent data divergence, schema contract violation, outbox snapshot re-emission, or numeric precision loss.
- **OUTAGE**: PostgreSQL primary crash due to WAL disk exhaustion, or Kafka Connect cluster task failures.
- **DEGRADE**: Elevated replication lag, logical decoding memory spill to disk, or network socket timeouts.
- **COMPLIANCE**: Leaking PII to raw change streams, or re-emitting cryptographically shredded data via CDC snapshots.
- **SECURITY**: Unauthorized connector reconfiguration via exposed REST APIs, or privilege escalation using superuser replication accounts.

---

## 2. Failure Nodes Catalog

### Category 2: Use-Case Selection & Domain Isolation

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F2.1** | Raw table topics adopted as integration contracts | **CORRUPT** | External service teams consume raw database change streams (`M_RAWTABLE`), coupling to internal column layouts. | Enforce `M_OUTBOX` for cross-team integration contracts. Restrict raw table consumer ACLs strictly to the data owner team. | Architecture review and Kafka ACL enforcement. |

---

### Category 3: Source Database Prerequisites

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F3.1** | Slot or sender limits too low | **OUTAGE** | `max_replication_slots` or `max_wal_senders` saturated during failover or node restart. | Size parameters explicitly: $\text{connectors} \times 2 + \text{standbys} + \text{failover headroom}$. Provision alerts when $> 80\%$ utilized. | PostgreSQL connection attempt alerts and slot count gauge. |
| **F3.2** | Replication role created with superuser | **SECURITY** | Over-privileged database user credentials leaked or compromised. | Grant strictly `REPLICATION` and explicit `SELECT` privileges only on published tables. Superuser is banned. | IAM credential audit and database privilege scanner. |
| **F3.3** | Captured table without primary key | **CORRUPT** | Table lacks a primary key, preventing unique row identification during updates and incremental snapshots. | Enforce `REPLICA IDENTITY FULL` or mandate explicit primary key / unique index before adding to publication. | Pre-deployment migration schema validator. |

---

### Category 4: Connector Provisioning & Configuration

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F4.1** | Snapshot mode left at default for outbox | **CORRUPT** | Connector boots with default snapshot mode, re-reading entire outbox table and flooding Kafka with duplicate events. | Enforce `snapshot.mode = no_data` for all `M_OUTBOX` connectors (`CDC-03`). | Connector configuration validator at registration. |
| **F4.2** | Connector renamed in configuration | **CORRUPT** | Renaming connector resets offset tracking in Kafka Connect, causing fresh snapshot or streaming from obsolete LSN. | Immutable connector names. Connector migrations require explicit state migration runbook. | Connector registry name immutability check constraint. |
| **F4.3** | Decimal fields parsed as double | **CORRUPT** | Floating-point conversion truncates decimal precision for financial/monetary values. | Enforce `decimal.handling.mode = precise` or `string` in Debezium configuration. | Schema registry type validator. |
| **F4.4** | Config drift between runtime and repository | **CORRUPT** | Manual REST API edits modify running connector config without updating source control. | Enforce `CDC-10`: Configuration is Code. Periodic reconciliation job detects and alerts on config drift. | Automated configuration drift detector. |
| **F4.5** | Topic auto-created with broker defaults | **LOSS** | Destination topic not pre-provisioned, inheriting low replication factor (`rf=1`) or small retention. | Disable broker topic auto-creation (`auto.create.topics.enable=false`). Pre-provision all topics via IaC. | Kafka broker configuration audit. |

---

### Category 5: Snapshot Strategies & Data Bootstrap

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F5.1** | Snapshot used to "recover" lost events | **LOSS** | Operator triggers snapshot to recover lost outbox records, losing event delete operations and interim versions. | Enforce `CDC-01`: Recovery never uses snapshots. Lost events are replayed exclusively from `C-AUD` audit store. | Recovery runbook compliance check. |
| **F5.2** | Long initial snapshot on multi-terabyte table | **OUTAGE** | Table locks or prolonged read transactions bloat WAL and saturate source DB I/O. | Use incremental chunked snapshots via signaling table (`C-SIG`) with watermarking. | Active transaction duration monitor. |
| **F5.3** | Incremental snapshot executed on outbox table | **CORRUPT** | Signaling snapshot against outbox re-emits processed domain records out of order. | Domain rule strictly rejects snapshot signals when mode is `M_OUTBOX` (`ErrOutboxSnapshotForbidden`). | Domain rule validation in `cdc.rules.go`. |
| **F5.4** | Signaling table not published or read-only | **DEGRADE** | Signal table missing from publication or lacks write permissions, causing snapshot requests to be ignored. | Automated provisioning script adds `cdc_signal_log` to publication and grants write privileges. | Signal dispatch timeout monitor. |
| **F5.5** | Snapshot reads treated as creates by consumer | **CORRUPT** | Consumers interpret backfilled snapshot records as real-time new business actions. | Set `snapshot: true` header on backfill events. Consumers handle snapshot records with idempotency guards. | Consumer header validation. |

---

### Category 6: Outbox Event Routing & Delivery

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F6.1** | Routing by event type instead of aggregate ID | **CORRUPT** | Splitting events of the same aggregate across topics/partitions breaks ordering guarantees. | Enforce partition key = `aggregate_id`. Events for an aggregate always land on the same partition. | Kafka partition key distribution validator. |
| **F6.2** | Router misconfiguration drops events | **LOSS** | Mismatched payload field names cause event router SMT to drop messages without error. | Enforce schema contracts and integration test suite with canary payload validation. | Canary reconciliation failure alert. |
| **F6.3** | Outbox row mutated after insertion | **CORRUPT** | Service executes an `UPDATE` on an outbox row, emitting an unexpected update event. | Database trigger `trg_prevent_outbox_update` forbids updates on outbox table (append-only). | PostgreSQL trigger exception log. |

---

### Category 7: Raw Table Capture & Data Integrity

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F7.1** | TOAST placeholder written as real value | **CORRUPT** | Unchanged TOAST columns are decoded as null or placeholder values in Debezium. | Set `unavailable.value.placeholder` handling or enforce `REPLICA IDENTITY FULL` on tables with large fields. | Schema validation test suite. |
| **F7.2** | Deletes ignored by downstream consumers | **CORRUPT** | Consumer does not handle tombstone records, retaining deleted rows in downstream read models. | Mandate delete and tombstone handling in consumer contract specifications. | Read model consistency audit. |
| **F7.3** | Consumer processes partial transaction | **CORRUPT** | Consumer reads records from a multi-row transaction before all changes are published. | Debezium transaction metadata topic tracks transaction boundaries (`BEGIN`/`END`). | Transaction boundary validator. |
| **F7.4** | Column rename on source breaks consumers | **OUTAGE** | Unannounced DDL column rename causes deserialization errors in downstream services. | Pre-deployment schema evolution policy: add new column, dual-write, migrate consumers, drop old column. | Schema Registry compatibility check. |
| **F7.5** | Table truncation skipped by logical replication | **CORRUPT** | `TRUNCATE` command executed on source does not emit individual row deletes. | Handle `TRUNCATE` messages explicitly or forbid manual truncations via database grants. | DDL audit event monitor. |
| **F7.6** | PII columns captured to readable topics | **COMPLIANCE** | Unmasked sensitive customer data streamed to shared Kafka cluster. | Enforce column blacklisting or field-level encryption before CDC ingestion. | Data loss prevention (DLP) stream scanner. |

---

### Category 8: Schema & Primary Key Evolution

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F8.1** | Primary key update re-keys aggregate | **CORRUPT** | Updating a primary key column emits a delete on old key and create on new key, causing reordering. | Primary key updates are forbidden. Model as explicit entity retirement and new entity creation. | Database check constraint preventing PK updates. |
| **F8.2** | Stale snapshot read overwrites streamed update | **CORRUPT** | Slow snapshot chunk finishes after real-time update was already applied downstream. | Watermarking algorithm (`C-SIG`) drops snapshot records with LSN older than last applied stream update. | Version monotonicity validator. |

---

### Category 9: Replication Slot Governance & Storage Protection

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F9.1** | Fixed-size WAL alert threshold fires too late | **OUTAGE** | Static 50GB WAL threshold breached in minutes during high-throughput batch writes. | Enforce dynamic time-to-full circuit breaker (`threat_hours < 1.0` triggers emergency pause). | Dynamic WAL growth velocity monitor. |
| **F9.2** | Orphaned slot from decommissioned connector | **OUTAGE** | Connector deleted in Kafka Connect but replication slot remains active in PostgreSQL, retaining WAL. | Decommissioning state machine cleans up PostgreSQL slot upon connector termination. | Uninventoried slot scanner alert. |
| **F9.3** | Multiple slots decoding identical WAL | **DEGRADE** | Multiple connectors on same database multiply CPU, disk I/O, and decoding overhead. | Consolidate capture scope into a single publication and connector per source database. | Database resource utilization monitor. |
| **F9.4** | Large transaction spills decoding to disk | **DEGRADE** | Single giant transaction exceeds `logical_decoding_work_mem`, spilling changes to source disk. | Batch size limits on write transactions and tuned `logical_decoding_work_mem`. | Disk spill metric alert. |

---

### Category 10: High Availability & Failover Resilience

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F10.1** | Primary failover on PostgreSQL $\le 16$ drops slot | **LOSS** | Logical slots not synced to replica; promoted replica has no slot, causing missed events. | Deploy failover slot synchronization daemon or upgrade to PostgreSQL 17+ failover slots. | Standby slot sync lag monitor. |
| **F10.2** | Asynchronous standby promoted with lost data | **CORRUPT** | Unsynced transactions lost on failover, but events were already published to Kafka (ghost events). | Gate logical decoding on physical standby replication confirmation (`CDC-08`). | Replication lag discrepancy check. |
| **F10.3** | Connector reconnects to old primary after split | **CORRUPT** | Connector streams stale or divergent WAL from demoted primary due to DNS caching. | Explicit primary routing via connection pooler (e.g. PgBouncer / Patroni leader endpoint). | Primary node identity validator. |

---

### Category 11: Connect Cluster Operations & Upgrades

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F11.1** | Connector upgrade alters serialization format | **CORRUPT** | Debezium version upgrade changes header format or timestamp precision, breaking consumers. | Stage upgrades in staging environment with end-to-end schema compatibility verification. | Canary schema compatibility test. |

---

### Category 12: Security & Operational Governance

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F12.1** | Unauthenticated Connect REST API exposed | **SECURITY** | Unauthorized network client pauses, deletes, or alters connector configuration. | Wrap Connect REST API behind internal CDC service gateway with mutual TLS and RBAC. | Network perimeter security scanner. |
| **F12.2** | Failed connector task unnoticed | **OUTAGE** | Connector enters `FAILED` state silently; slot retains WAL until database storage fills. | Real-time connector task state monitoring with immediate PagerDuty alerting on state $\neq$ `RUNNING`. | Kafka Connect task status gauge. |
| **F12.3** | Internal offsets topic misconfigured | **LOSS** | Connect internal offsets topic compacted or truncated prematurely, losing stream progress. | Configure `cleanup.policy = compact` with high retention on internal Kafka Connect topics. | Kafka topic configuration auditor. |
| **F12.4** | Database credential rotation breaks connector | **OUTAGE** | Password rotated in Vault but running connector task continues with cached expired secret. | Automated credential rotation workflow restarts connector task with renewed credentials. | Authentication failure alert. |

---

### Category 13: Disaster Recovery & Event Replay

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F13.1** | Snapshot-only recovery for outbox table | **LOSS** | Recovery runbook runs snapshot on outbox after event loss, missing deletes and historical sequence. | Enforce `CDC-01`: Replay outbox events exclusively from durable audit ledger (`C-AUD`). | Recovery runbook audit validation. |

---

### Category 14: Data Privacy & Cryptographic Shredding

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F14.1** | Shredded data resurfaces via CDC snapshots | **COMPLIANCE** | Erased customer data resurrected in downstream analytics through unmanaged CDC table snapshots. | PII fields in outbox and raw tables must be ciphertext-encrypted (`MT-09`). Shredding DEK guarantees irrecoverable ciphertext. | Data privacy compliance scanner. |


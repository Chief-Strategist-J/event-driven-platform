# Replay Mechanism — Failure Catalog & Mitigation Strategy

This catalog documents failure modes across the replay lifecycle, derived from the business logic tree.

---

## 1. Severity Taxonomy

- **LOSS**: Permanent loss of business events, financial transactions, or state history.
- **CORRUPT**: Silent divergence between projection and primary event stream, invalid offsets, or mutated historical records.
- **OUTAGE**: Complete downtime of event processing, consumer groups, or ingress APIs.
- **DEGRADE**: Elevated latency, broker page-cache thrashing, or depleted rate budgets.
- **COMPLIANCE**: Violation of GDPR/CCPA data erasure or unauthorized cross-environment PII leaks.

---

## 2. Failure Nodes Catalog

### Category 3: Source Coverage and Archive Ingestion

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F3.1** | Replay from offset 0 on retention-bounded topic | **CORRUPT** | Topic truncated by Kafka retention policy. | Orchestrator validates earliest log offset against manifest start offset. Refuse run if start offset is pruned. | Orchestrator preflight log check. |
| **F3.2** | Archive written by at-least-once pipeline | **CORRUPT** | Duplicated envelopes or unsequenced segment chunks. | Deduplicate by `(topic, partition, offset)` before dispatching to replay runner. | Sequence gap and duplicate counter in reader. |
| **F3.3** | Archive converted to columnar format | **CORRUPT** | Byte payload or schema metadata stripped during Parquet conversion. | Require raw envelope archiving preserving exact binary headers and Avro schema IDs. | Schema registry deserialization check. |
| **F3.4** | Replay outruns retention during archive handoff | **LOSS** | Archive phase execution time exceeds Kafka live retention window. | Compute retention margin safety margin prior to launch; dynamically extend live topic retention during active run. | Progress ETA vs Kafka earliest offset metric. |
| **F3.5** | Tiered storage fetch latency spikes | **DEGRADE** | Remote cold object fetches block broker IO threads. | Rate-limit archive reader concurrency; separate dedicated network interface. | Broker fetch latency P99 alerts. |
| **F3.6** | Recreated topic in the past | **CORRUPT** | Topic deleted and recreated with reset epoch and conflicting offsets. | Validate cluster topic UID and epoch in manifest before mounting partition stream. | Topic UUID mismatch alert. |

---

### Category 4 & 6: Consumer Isolation & Scope

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F4.1** | Window replay without initial state snapshot | **CORRUPT** | Running bounded window without hydrating state up to window start. | Enforce mandatory snapshot hydration at offset `V` prior to starting window replay. | Manifest validation preflight. |
| **F4.2** | Window replay directly into live store | **CORRUPT** | Replaying events against live table without sidecar group. | Refuse manifests targeting live table or live group (`RP-01`). | Group ID naming validator. |
| **F6.1** | Replay shares live consumer group ID | **OUTAGE** | Replay joins live group, triggering rebalance storms. | Orchestrator synthesizes deterministic sidecar group: `replay_{feature}_{run_id}`. | Group join audit trigger. |
| **F6.2** | Replay invalidates live cache keys | **DEGRADE** | Consumer cache eviction hooks execute against production Redis cluster. | Namespace all cache keys with `replay:{run_id}:` during replay runs. | Redis key inspection metrics. |
| **F6.3** | Replay increments live business metrics | **CORRUPT** | Telemetry emitters record historical events as live throughput. | Tag metrics with `run_mode: replay` and route to ephemeral dashboard namespaces. | Metric dimension filter audit. |
| **F6.4** | Replay poison pill enters live DLQ | **DEGRADE** | Replay errors routed to primary dead-letter queue. | Route replay errors to dedicated `replay_dlq` or fail the run directly. | DLQ topic tag validator. |
| **F6.5** | Replay writes to shared non-projection tables | **CORRUPT** | Handlers mutate audit or activity logs in production schema. | Apply strict database role ACLs preventing writes outside the shadow projection table. | Postgres privilege violation alarms. |

---

### Category 7: Side-Effect Suppression & Routing

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F7.1** | Header-based replay flag missing | **CORRUPT** | Historical envelopes lack replay header and trigger live external calls. | Mode is determined by consumer runtime context, NOT per-message headers (`RP-02`). | Effect Gateway runtime mode assertion. |
| **F7.2** | Handler calls external provider directly | **LOSS** | Bypassing Effect Gateway to execute real payment/SMS. | Static analysis and CI dependency rules enforce all external calls pass via `EffectGatewayPort`. | Static analysis linter in CI. |
| **F7.3** | Consumer outbox emits during replay | **CORRUPT** | Outbox publisher sends historical events to production brokers. | Effect Gateway suppresses outbox writes or routes to isolated `shadow_outbox`. | Outbox target table assertion. |
| **F7.4** | Live enrichment calls executed during replay | **DEGRADE** | Handler queries live microservices during replay. | Prohibit live network calls; hydrate state strictly via event envelope payload. | Gateway egress firewall block. |
| **F7.5** | Disaster recovery run with blanket suppression | **LOSS** | In `RECOVER` mode, critical uncommitted mutations are silently suppressed. | Operate Effect Gateway in `RECONCILE` mode: check remote external state before dispatch. | Reconciliation discrepancy counter. |
| **F7.6** | Suppressed-effect log contains plain PII | **COMPLIANCE** | Payload logging stores unmasked sensitive user data. | Encrypt effect log payloads with per-tenant DEK or mask PII fields. | Compliance scanner log audit. |

---

### Category 8 & 9: Determinism & Operational Stability

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F8.1** | Handlers generate non-deterministic UUIDs | **CORRUPT** | Generating new random UUIDs during replay breaks foreign key joins. | Derive IDs deterministically from `(tenant_id, aggregate_id, version)` using UUIDv5. | Identity integrity validator. |
| **F8.2** | Wall-clock logic evaluated during replay | **CORRUPT** | Handlers check `time.Now()` rather than event record timestamp. | Inject virtual clock driven strictly by the event envelope timestamp. | Clock injection linter. |
| **F9.1** | Replay evicts broker page cache | **DEGRADE** | Heavy sequential reads from cold partitions evict active consumer pages. | Throttle replay consumer fetch byte rate to prevent page cache starvation. | Kafka broker page cache hit ratio alert. |
| **F9.2** | Target database replicas lag under WAL flood | **DEGRADE** | Massive bulk writes overwhelm PostgreSQL replication stream. | Apply rate limits to replay batches and monitor replication lag. | `pg_stat_replication` lag monitor. |
| **F9.3** | Search indexes left with replica count 0 | **LOSS** | Elasticsearch index created without replicas to speed up replay, never restored. | Pre-cutover checklist automatically sets replica count and verifies green cluster health. | Cutover readiness checklist check. |
| **F9.4** | Upcaster bug detected mid-run | **CORRUPT** | Schema transform produces corrupted projections half-way through run. | Replay runs write to an isolated shadow table; simply abort and drop the shadow table. | Shadow schema parity checker. |

---

### Category 10, 11 & 12: Cutover, Parity & Privacy

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F10.1** | Replay group terminated at cutover | **CORRUPT** | Replay group stopped before handing off ownership, resulting in stale reads. | Promote replay runner directly to primary consumer group or perform seamless pointer switch. | Cutover controller step verification. |
| **F10.2** | Effect ownership overlap | **CORRUPT** | Both old live group and new replay group dispatch external notifications simultaneously. | Atomic effect mode handoff: old group disabled before new group switches to `LIVE`. | Gateway concurrency mutex. |
| **F11.1** | Parity evaluated against moving live state | **DEGRADE** | Comparing shadow against live store without version pinning. | Compare parity at matching aggregate versions `V`, ignoring unconsumed offsets. | Parity version alignment validator. |
| **F12.1** | Table rename lock acquisition blocks queries | **OUTAGE** | `ALTER TABLE RENAME` holds exclusive lock while waiting on active long-running queries. | Set `lock_timeout = '2s'` and use atomic view swap or routing proxy pointer update. | Lock queue latency monitor. |
| **F14.1** | Erased customer reappears in rebuilt view | **COMPLIANCE** | Crypto-shredded records decrypted or unmasked during historical replay. | Replay runner checks crypto-shredding keystore; missing key yields erased/redacted state. | GDPR privacy scanner. |
| **F14.2** | Replay target with production PII in lower env | **COMPLIANCE** | Replaying production archive in staging without data anonymization. | Orchestrator restricts production archive access to production VPC networks only. | IAM permission guardrails. |


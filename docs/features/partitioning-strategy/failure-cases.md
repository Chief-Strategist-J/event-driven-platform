# Partitioning Strategy — Failure Catalog & Mitigation Strategy

This catalog documents failure modes across the Kafka topic partitioning and key hashing lifecycle, derived from the business logic decision tree and platform invariants (`PT-01` to `PT-10`).

---

## 1. Severity Taxonomy

- **LOSS**: Irreversible loss of historical domain events due to improper compaction or premature partition truncation.
- **CORRUPT**: Silent event reordering, key remapping, cross-language hash divergence, or corrupted stream joins.
- **OUTAGE**: Platform-wide unavailability caused by broker disk saturation or cascading consumer group rebalance storms.
- **DEGRADE**: Elevated consumer lag, broker page-cache thrashing on hot partitions, or network bandwidth depletion.
- **COMPLIANCE**: Breach of GDPR/CCPA erasure SLAs caused by immutable plaintext PII embedded in partition keys.

---

## 2. Failure Nodes Catalog

### Category 2: Key Design & Ordering Guarantees

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F2.1** | Random UUID generated per event as key | **CORRUPT** | Producer generates random UUID as key, scattering an aggregate's events across all partitions and destroying ordering. | Enforce `PT-02`: Partition key = `tenant_id:aggregate_type:aggregate_id`. Validate key format at producer boundary. | CI partition affinity tests and event ordering validator. |
| **F2.2** | Key includes event type or version suffix | **CORRUPT** | Appending sequence or event type to the key remaps subsequent versions to different partitions. | Domain rule strictly rejects compound keys containing version or event type components. | Producer lint rule and pre-commit schema validation. |
| **F2.3** | Key contains tenant ID alone | **OUTAGE** | Low key cardinality concentrates all tenant transactions onto a single partition, creating severe hot spots. | Formulate composite key: `tenant_id:aggregate_type:aggregate_id` to distribute load across high-cardinality entities. | Skew monitor alert (`skew_ratio > 3.0`). |
| **F2.4** | Key formatting differs between microservices | **CORRUPT** | Service A uses `tenant:orders:123` while Service B uses `TENANT/ORDERS/123`, causing events to land on different partitions. | Centralized Key Builder library (`C-KEY`) enforces normalized lowercase UTF-8 encoding. | Canary affinity probe across producer implementations. |
| **F2.5** | Email, phone number, or national ID used as key | **COMPLIANCE** | Plaintext PII embedded into partition key survives topic encryption, preventing GDPR/CCPA cryptographic erasure. | Enforce `PT-08`: Partition keys must use opaque UUIDs or synthetic surrogate IDs only. | Automated PII pattern scanner in producer middleware. |
| **F2.6** | JSON object key used by one service, string by another | **CORRUPT** | Differing key byte serializations produce differing Murmur2 hashes for the same aggregate ID. | Enforce raw UTF-8 string serializer across all producer clients. Structured JSON keys are banned. | Kafka message key deserialization audit. |

---

### Category 3: Partitioner Consistency Across Clients

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F3.1** | Go and JVM producers publish to same topic | **CORRUPT** | JVM defaults to Murmur2; Go librdkafka defaults to CRC32, mapping identical keys to different partitions. | Enforce `PT-01`: Configure Go, Python, and Node clients to use `partitioner = murmur2_random`. | Cross-language affinity test suite in CI pipeline. |
| **F3.2** | Partitioner re-implemented with `Math.abs()` | **CORRUPT** | In two's-complement arithmetic, `Math.abs(Integer.MIN_VALUE)` produces negative integer, causing modulo crash or negative index. | Use 31-bit bitwise positive mask: `hash & 0x7fffffff` before applying modulo arithmetic. | Unit test verifying `0x80000000` hash input handling. |
| **F3.3** | Relay / CDC connector partitioner differs from app | **CORRUPT** | Outbox relay uses custom partitioner while manual publisher uses default partitioner. | Mandate standardized Murmur2 partitioner library across all relays, connectors, and apps. | Synthetic canary reconciliation against target topic. |
| **F3.4** | Client library upgrade alters default partitioner | **CORRUPT** | Upgrading Kafka client package silently changes default partitioning algorithm. | Lock partitioner class explicitly in client configuration rather than relying on library defaults. | Upgrade verification runbook in staging environment. |

---

### Category 4: Hot Keys, Partition Skew & Starvation

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F4.1** | Sequence-suffix salting applied on ordered topic | **CORRUPT** | Attempting to spread a hot aggregate by appending `(seq % N)` spreads its events across partitions, destroying ordering. | Salting is forbidden on `K_ORDERED`. Remediate hot entities via dedicated VIP topics (`PT-05`). | Architecture review and key builder validation. |
| **F4.2** | Adding partitions to fix a single hot key | **DEGRADE** | Operator increases partition count, but since all records share the same hot key, they still land on a single partition. | In-place partition addition is ineffective for single hot keys. Isolate the hot tenant to a dedicated topic. | Skew monitor telemetry showing unchanged partition load. |
| **F4.3** | Hot partition causes consumer timeout and rebalances | **OUTAGE** | Consumer processing a saturated partition lags and exceeds `max.poll.interval.ms`, triggering cascading rebalances. | Tune consumer prefetch, isolate hot aggregates, or process independent aggregates within a partition concurrently. | Consumer group rebalance frequency metric. |
| **F4.4** | Hot partition leader saturates broker disk/NIC | **DEGRADE** | Kafka cluster places the leader replica for a hot partition on an overloaded broker node. | Execute partition reassignment to move hot partition leaders to underutilized cluster brokers. | Broker CPU and network bandwidth saturation alerts. |

---

### Category 5: Capacity Planning & Partition Sizing

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F5.1** | Generic blog formulas used for partition sizing | **DEGRADE** | Provisioning arbitrary 50 partitions on a low-throughput topic causes excessive ZooKeeper/KRaft metadata overhead. | Measure baseline throughput: $P = \max(T_{\text{write}} / 10\text{MB/s}, T_{\text{read}} / 25\text{MB/s}) \times 2.5$. | Cluster-wide partition count monitoring. |
| **F5.2** | Topic sized strictly for today's peak | **DEGRADE** | Rapid organic growth saturates partition capacity within months, forcing an emergency blue/green migration. | Enforce `PT-06`: Size partition counts for a 12-to-24-month horizon at topic provisioning time. | Long-term partition saturation forecasting model. |
| **F5.3** | Over-partitioning low-volume microservices | **DEGRADE** | Creating hundreds of partitions on low-traffic topics exhausts consumer thread memory and broker file descriptors. | Minimum partition baseline sizing rules enforced by provisioning API. | Resource utilization audit per microservice. |

---

### Category 6: Topic Compaction Invariants

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F6.1** | Topic compaction enabled on event stream | **LOSS** | Enabling `cleanup.policy = compact` on `K_ORDERED` causes Kafka to delete historical intermediate events. | Enforce `cleanup.policy = delete` for append-only domain streams. `compact` is restricted to `K_COMPACTED`. | Kafka topic configuration policy linter. |
| **F6.2** | Tombstone deletion retention shorter than downtime | **CORRUPT** | Kafka purges deletion tombstones before an offline consumer restarts, leaving deleted entities active downstream. | Set `delete.retention.ms` $\ge$ maximum consumer disaster recovery recovery window (e.g., 7 days). | Topic configuration auditor. |
| **F6.3** | Compacted topic consumer assumes one value per key | **CORRUPT** | Consumer assumes only the latest value is returned, ignoring uncompacted segments in active segment log. | Consumers must process compacted topics idempotently and handle duplicate keys across the log. | Idempotency integration tests. |

---

### Category 7: Co-Partitioning & Stream Joins

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F7.1** | One topic in a join set expanded in place | **CORRUPT** | Expanding topic A to 24 partitions while topic B remains at 12 causes Kafka Streams joins to evaluate mismatched keys. | Enforce `PT-07`: Co-partitioned topic sets must share identical partition counts and be locked together. | Stream join topology validator. |
| **F7.2** | Disaster recovery topic has different partition count | **CORRUPT** | DR cluster provisioned with 16 partitions while primary has 32, remapping keys upon failover. | Infrastructure-as-Code enforces 100% topology parity across primary and DR clusters. | Multi-region drift detection cron. |
| **F7.3** | Retry topic partition count differs from main topic | **CORRUPT** | Main topic has 16 partitions; retry topic has 8. Events sent to retry and redriven land out-of-order. | Dead-letter and retry topics must match the partition count of the originating main topic. | Retry topic provisioning validator. |

---

### Category 8: Partition Expansion & Stream Migration

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F8.1** | Producers resume with stale metadata during migration | **CORRUPT** | Stale producer caches publish to source topic while cutover is underway, interleaving old and new layouts. | Coordinated cutover state machine (`C-MIG`) disables source topic writes before consumer cutover. | Source topic write activity alert during `CUTOVER`. |
| **F8.2** | Compacted topic expanded in place | **CORRUPT** | Expanding compacted topic results in old state retained on partition A and new state written to partition B. | In-place expansion is strictly blocked (`allow_in_place_expansion = FALSE`). Migration requires state replay. | Topic topology immutability trigger. |
| **F8.3** | Operator waits for consumer lag "exactly 0" | **DEGRADE** | In high-throughput transactional topics, new events constantly arrive, preventing lag from reaching 0. | Execute producer pause / outbox buffer pause to drain lag cleanly during the draining phase. | Drain timeout monitor. |
| **F8.4** | Held-key ordering state tied to old partition ID | **CORRUPT** | Sequence enforcement buffer indexes out-of-order events by partition ID, bypassing checks after layout change. | Enforce `PT-10`: In-flight buffers and held keys are indexed strictly by `aggregate_id`. | Sequence gap detector after cutover. |
| **F8.5** | Operator accidentally increases partitions via CLI | **CORRUPT** | Admin executes `kafka-topics.sh --alter` manually on production cluster. | Revoke partition alteration permissions via Kafka broker ACLs. Enforce changes via GitOps only. | Kafka audit log SIEM alerts. |

---

### Category 9: Broker Rebalancing & Leader Distribution

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F9.1** | Unthrottled partition reassignment saturates network | **DEGRADE** | Moving partitions between brokers floods 10Gbps cluster NICs, spiking produce and consume latencies. | Apply replication throttles: `leader.replication.throttled.rate` and `follower.replication.throttled.rate`. | Inter-broker replication bandwidth metrics. |
| **F9.2** | Partition leaders concentrated on single broker | **DEGRADE** | Broker restart causes preferred leaders to fail back unevenly, concentrating traffic onto one node. | Enable `auto.leader.rebalance.enable = true` or execute `kafka-leader-election.sh` periodically. | Broker leader distribution skew monitor. |

---

### Category 10: Consumer Partition State & In-Memory Caches

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F10.1** | Consumer caches keyed by partition ID | **CORRUPT** | Consumer caches aggregate state indexed by assigned partition, serving stale state after rebalance or migration. | Consumer caches must be keyed by domain aggregate ID (`tenant_id:aggregate_id`), never by partition number. | Consumer cache validation test suite. |


## Amendment 2026-11-07: Failure Modes Added

| ID | Failure Mode | Trigger | Detection | Response | Operator Action |
|---|---|---|---|---|---|
| **F-PT-A1** | Duplicate topic topology | Same `topic_name` registered again (per tenant after migration 0028; globally before) | SQLSTATE 23505 via `mapInsertError` | `409` domain message (previously internal error with raw text) | Reuse the existing topology or choose another name. |
| **F-PT-A2** | Duplicate migration runbook | Same `migration_id` registered again | 23505 | `409` | Use a new `migration_id`. |
| **F-PT-A3** | Migration phase conflict | `AdvanceMigration` with `expectedPhase` != current phase | Service check | `409 phase conflict: expected X, current Y` | Re-read the runbook state and retry with the current phase. |
| **F-PT-A4** | Cross-tenant name collision before 0028 | Tenant B registers a `topic_name`/`migration_id` tenant A already uses | 23505 on the global constraint | `409` (misleading "for the tenant" message) | Apply migration 0028 via migration tooling; rollback fails if tenants share values. |

# Partitioning Strategy — Business Logic Decision Tree

```
PARTITIONING STRATEGY — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune / verify per version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-KEY  Key builder library — one implementation per language, identical bytes for identical keys
│   │   ├── C-PRT  Partitioner policy — one hashing algorithm across all producer clients and languages
│   │   ├── C-TOP  Topic provisioning (IaC) — partition count, durability, retention, compaction, limits
│   │   ├── C-CAP  Capacity model — measured per-partition limits, growth horizon, cluster limits
│   │   ├── C-SKW  Skew monitor — per-partition bytes/records, per-key top-N, consumer lag per partition
│   │   ├── C-MIG  Stream migration controller — new stream version, dual publish, version-based cutover
│   │   └── C-MON  Monitoring & canaries — partition-affinity canary across producers
│   │
│   ├── 0.2 Topic classes (declared per topic; drives every rule below)
│   │   ├── K-ORDERED   Keyed by aggregate; ordering contract (O-STRICT / O-STATE consumers)
│   │   ├── K-COMPACTED Keyed state topic; latest value per key (snapshots, reference data)
│   │   ├── K-COPART    Joined with other topics (stream processing) — co-partitioning contract
│   │   └── K-UNORDERED Null or arbitrary keys; no ordering contract (telemetry, logs, independent commands)
│   │
│   └── 0.3 Ownership
│       ├── Platform: partitioner policy, client templates, provisioning, capacity model, migration tooling
│       ├── Domain teams: aggregate boundaries, key choice, growth forecasts, hot-key remediation
│       └── Architecture review: partition count decisions for K-ORDERED/K-COMPACTED/K-COPART topics
│
├── 1. INVARIANTS  (with enforcement)
│   ├── PT-01 Same key → same partition from every producer, every language, every client version [C]
│   │   └── Enforced by: C-PRT (murmur2-compatible everywhere); cross-language affinity tests in CI; production canary
│   ├── PT-02 Key = ordering scope = aggregate identity [C]
│   │   └── Enforced by: key builder accepts aggregate identity only; no sequence/time/random components
│   ├── PT-03 Key bytes are stable for the aggregate's lifetime [C]
│   │   └── Enforced by: string serializer, fixed format (tenant + separator + aggregate id), normalized case/encoding
│   ├── PT-04 Partition count of K-ORDERED / K-COMPACTED / K-COPART topics never changes in place [C]
│   │   └── Enforced by: IaC policy rejects partition increases on these classes; admin ACLs
│   ├── PT-05 One aggregate is never split across partitions [C]
│   │   └── Enforced by: key builder; review rejects compound keys with sequence/bucket suffixes on K-ORDERED
│   ├── PT-06 Capacity is decided for the growth horizon at creation [C]
│   │   └── Enforced by: provisioning requires forecast + measured baselines; review sign-off
│   ├── PT-07 Co-partitioned topics share partition count AND partitioner [C]
│   │   └── Enforced by: provisioning groups co-partitioned topics; validation at deploy
│   ├── PT-08 Keys carry no personal data [C]
│   │   └── Enforced by: opaque ids only (multi-tenant tree MT-09); lint
│   ├── PT-09 Null keys only on K-UNORDERED [C]
│   │   └── Enforced by: producer library rejects null key for other classes
│   └── PT-10 Partition-local state is never authoritative across a layout change [C]
│       └── Enforced by: held keys, version state, dedupe keyed by aggregate, not by partition
│
├── 2. KEY DESIGN
│   ├── 2.1 Choosing the key
│   │   ├── Q1 What must be strictly ordered relative to each other? → that set is the aggregate → key
│   │   ├── Q2 Multi-tenant? → key = tenant id + separator + aggregate id (multi-tenant tree §4)
│   │   ├── Q3 Does the aggregate change identity over its life? → forbidden (close + create with lineage)
│   │   └── Q4 Is ordering not needed? → K-UNORDERED, null key or entity key for locality only
│   │
│   ├── 2.2 Key stability (PT-03)
│   │   ├── Same string encoding (UTF-8), same case, same separator, no whitespace
│   │   ├── Never include version, timestamp, event type, or random suffix
│   │   └── Key format documented in the stream contract (versioning tree)
│   │
│   ├── 2.3 Cardinality
│   │   ├── High-cardinality keys (aggregate ids) distribute well
│   │   ├── Low-cardinality keys (country, status, tenant alone) concentrate load → hot partitions
│   │   └── Rule [L]: distinct active keys ≥ 100× partition count for even distribution
│   │
│   ├── F2.1 Random UUID per event as key — ordering destroyed — CORRUPT [C]
│   ├── F2.2 Key includes event type or version — aggregate split across partitions — CORRUPT [C]
│   ├── F2.3 Key = tenant only — whole tenant on one partition — DEGRADE/OUTAGE [C]
│   ├── F2.4 Key format differs by service (separator, case, prefix) — split ordering — CORRUPT [C]
│   │   └── Detection: affinity canary (§13) — same aggregate observed on more than one partition
│   ├── F2.5 Email/phone/national id as key — survives encryption and shredding — COMPLIANCE [C]
│   └── F2.6 Key serialized as structured/JSON object by one producer, plain string by another — CORRUPT [C]
│       └── Handling: string key serializer everywhere (CDC tree §4.4)
│
├── 3. PARTITIONER CONSISTENCY ACROSS CLIENTS  (PT-01)
│   ├── 3.1 Reference behavior (Java client default for keyed records)
│   │   ├── Hash: murmur2 over serialized key bytes [C]
│   │   ├── Sign handling: positive mask (bitwise AND with 0x7fffffff), NOT absolute value [C]
│   │   └── Partition = masked hash modulo partition count [C]
│   │
│   ├── 3.2 Non-JVM clients
│   │   ├── librdkafka-based clients default to a CRC32-based consistent-random partitioner [C]
│   │   │   → must be configured to the murmur2-compatible partitioner on every producer
│   │   ├── Other native clients: verify default and configure explicitly [L]
│   │   └── Client templates set the partitioner explicitly; no reliance on defaults
│   │
│   ├── 3.3 Null-key behavior differs across clients (sticky vs random vs round-robin) [L]
│   │   └── Irrelevant for K-UNORDERED; null keys forbidden elsewhere (PT-09)
│   │
│   ├── 3.4 Custom partitioners
│   │   └── Forbidden for K-ORDERED unless implemented identically in every language and tested (avoid)
│   │
│   ├── F3.1 Mixed Go (librdkafka default) and JVM producers on one topic — same key, different partitions — CORRUPT [C]
│   │   ├── Impact: interleaved versions across partitions; consumers see gaps and stale events permanently
│   │   ├── Handling: 3.2; one-time audit of every producer config; affinity canary from each producer family
│   │   └── Detection: per-aggregate partition set size > 1
│   │
│   ├── F3.2 Reimplemented hash with absolute value — negative-hash keys misrouted — CORRUPT [C]
│   │   └── Handling: never reimplement; if unavoidable, golden test vectors from the Java client
│   │
│   ├── F3.3 Relay/connector partitioner differs from application producers writing the same topic — CORRUPT [C]
│   │   └── Handling: one writer per K-ORDERED topic (outbox relay) wherever possible
│   │
│   └── F3.4 Client library upgrade changes default partitioner — silent remapping — CORRUPT [L]
│       └── Handling: explicit partitioner config (no defaults); upgrade contract tests
│
├── 4. HOT SPOTS — CLASSIFICATION & REMEDIATION
│   ├── 4.1 Classify before acting
│   │   ├── H-KEY        One aggregate key dominates a partition
│   │   ├── H-COLLISION  Several warm keys hash to the same partition (low partition count / bad luck)
│   │   ├── H-TENANT     One tenant's many aggregates generate heavy load (distributed already if keyed properly)
│   │   ├── H-LOWCARD    Key cardinality too low (F2.3)
│   │   └── H-CONSUMER   Partitions balanced, but one consumer owns several heavy partitions (assignment skew)
│   │
│   ├── 4.2 Detection [L]
│   │   ├── Per-partition bytes-in and records-in vs median (sustained > 2× for 15 min)
│   │   ├── Top-N keys by volume per partition (sampled at producer or consumer)
│   │   └── Per-partition consumer time lag (hot partition lags first)
│   │
│   ├── 4.3 Remediation by class
│   │   ├── H-KEY
│   │   │   ├── Accept ceiling: single partition + single lane throughput is the max for one aggregate
│   │   │   ├── Optimize: payload size, handler cost, batching within the lane (same key)
│   │   │   ├── Re-model: split aggregate into sub-aggregates whose ordering is independent
│   │   │   │   (e.g., account → account + per-instrument positions); new stream version
│   │   │   └── Isolate: dedicated topic/consumer capacity for that tenant (still one partition per aggregate)
│   │   ├── H-COLLISION → higher partition count via new stream version (§8.3)
│   │   ├── H-TENANT → tenant quotas + dedicated placement (multi-tenant tree §2, §7)
│   │   ├── H-LOWCARD → re-key to aggregate identity (new stream version)
│   │   └── H-CONSUMER → assignment rebalance; key-hashed lanes; more consumers up to partition count
│   │
│   ├── F4.1 Sequence-suffix compound key on K-ORDERED — aggregate split — CORRUPT [C]
│   │   └── Handling: PT-05; "reassembly" is not an acceptable design for K-ORDERED
│   │
│   ├── F4.2 Hot key treated with more partitions — no effect (one key → one partition) [C]
│   │   └── Handling: classify first (4.1)
│   │
│   ├── F4.3 Hot partition causes consumer eviction (slow processing) — rebalance storm [C]
│   │   └── Handling: lanes + poll-thread separation (consumer tree §6); per-key backpressure
│   │
│   └── F4.4 Hot partition leader concentrates on one broker — broker saturation — DEGRADE [C]
│       └── Handling: leader balancing; preferred-leader distribution; broker-level monitoring (§9)
│
├── 5. CAPACITY PLANNING
│   ├── 5.1 Inputs (PT-06)
│   │   ├── Forecast peak produce rate (MB/s and records/s) at growth horizon [G: 2–3 years]
│   │   ├── Measured per-partition sustainable write rate on YOUR brokers (acks, compression, message size) [G]
│   │   ├── Measured per-consumer-lane processing rate for YOUR handlers [G]
│   │   ├── Required consumer parallelism (instances × lanes) at horizon
│   │   ├── Hottest single aggregate rate (must fit one partition + one lane)
│   │   └── Cluster constraints: partitions per broker, total partitions, replication traffic headroom
│   │
│   ├── 5.2 Partition count = max of:
│   │   ├── produce peak ÷ measured per-partition write rate
│   │   ├── consume peak ÷ measured per-instance throughput (instances ≤ partitions)
│   │   ├── desired max consumer instances (parallelism ceiling)
│   │   └── distribution floor for skew (key cardinality rule 2.3)
│   │   → then headroom for bursts [L: 1.5–2×] → round to a count with good divisibility for consumer scaling
│   │
│   ├── 5.3 Costs of too many partitions
│   │   ├── Longer leader elections and recovery after broker failure
│   │   ├── Longer rebalances; more memory per consumer (prefetch per partition — backpressure tree F5.2)
│   │   ├── More metadata, file handles, replication overhead
│   │   └── Higher end-to-end latency at low traffic (smaller batches per partition)
│   │
│   ├── F5.1 Baselines copied from blogs — mis-sized topics — DEGRADE or forced migration [C]
│   │   └── Handling: benchmark on target cluster with production message profile
│   │
│   ├── F5.2 Sized for today's peak — expansion needed within a year — stream migration cost [C]
│   │   └── Handling: growth horizon sizing for K-ORDERED
│   │
│   └── F5.3 Over-partitioned low-volume topics — latency and broker overhead — DEGRADE [C]
│       └── Handling: minimum partition count per class [L: 6–12 for T0/T1 ordered topics] with justification
│
├── 6. TOPIC CONFIGURATION BY CLASS
│   ├── 6.1 Durability (all business topics)
│   │   ├── Replication factor 3, min in-sync replicas 2, unclean leader election disabled [C]
│   │   └── Rack-aware replica placement across zones [C]
│   │
│   ├── 6.2 K-ORDERED event streams
│   │   ├── Delete cleanup policy; retention ≥ longest consumer outage + replay needs (offset tree INV)
│   │   ├── Timestamp type decided (CreateTime vs LogAppendTime) per topic
│   │   └── Max message size aligned across broker, topic, producer, consumer
│   │
│   ├── 6.3 K-COMPACTED state topics
│   │   ├── Compact (or compact+delete) cleanup; tombstones for deletes
│   │   ├── Compaction is not immediate: consumers see multiple values per key until cleaned [C]
│   │   ├── Minimum compaction lag to let consumers observe recent changes [L]
│   │   ├── Tombstone retention long enough for slow consumers to observe deletes [C]
│   │   └── Never used as event history (history destroyed by design)
│   │
│   ├── 6.4 K-UNORDERED telemetry
│   │   └── Shorter retention, sticky partitioning, separate cluster or quotas from T0 topics
│   │
│   ├── F6.1 Compaction enabled on event stream — intermediate events deleted — LOSS [C]
│   ├── F6.2 Tombstone retention shorter than consumer downtime — deletes missed — CORRUPT [C]
│   └── F6.3 Compacted topic consumers assume one value per key — stale reads — CORRUPT [C]
│
├── 7. CO-PARTITIONING & DEPENDENT TOPICS
│   ├── 7.1 Co-partitioned sets (joins): same partition count, same partitioner, same key format (PT-07)
│   ├── 7.2 Dependent topics that must mirror the main topic's partitioning
│   │   ├── Retry topics (if used) — same count and key (retry tree §8.1)
│   │   ├── Internal repartition/changelog topics of stream processors
│   │   └── Mirrored topics in DR clusters (same count, same mapping)
│   │
│   ├── F7.1 One topic in a join set expanded — join results wrong — CORRUPT [C]
│   ├── F7.2 DR mirror created with different partition count — failover remaps keys — CORRUPT [C]
│   │   └── Handling: mirrors provisioned by the same IaC definition; validation after creation
│   └── F7.3 Retry topic partition count differs from main — key moves between tiers — reorder [C]
│
├── 8. CHANGING PARTITION COUNT
│   ├── 8.1 Decision
│   │   ├── K-ORDERED / K-COMPACTED / K-COPART → never in place (PT-04) → §8.3 new stream version
│   │   └── K-UNORDERED → in place allowed with §8.2 checks
│   │
│   ├── 8.2 In-place increase (K-UNORDERED only)
│   │   ├── Verify no consumer relies on key locality (caches, dedupe by partition)
│   │   ├── Verify consumers start new partitions from their first offset, not latest (offset tree §5.5)
│   │   ├── Verify dependent topics (7.2) don't require matching counts
│   │   └── Partitions can never be decreased — confirm target count deliberately [C]
│   │
│   ├── 8.3 New stream version (K-ORDERED / K-COMPACTED / K-COPART)
│   │   ├── Step 1 Provision "<stream>.v<N+1>" with new partition count (and dependent topics)
│   │   ├── Step 2 Producer dual-publishes v<N> and v<N+1> from the SAME outbox transaction
│   │   │          (or derived-stream transformer from v<N>; versioning tree §11.2)
│   │   ├── Step 3 Consumers of v<N+1> start in shadow; bootstrap from snapshot or from v<N+1> start
│   │   ├── Step 4 Per-aggregate cutover by aggregate version (apply on v<N+1> only versions > last applied from v<N>)
│   │   ├── Step 5 Parity check; promote; dual-run observation window
│   │   ├── Step 6 Stop dual publish; decommission v<N> after all consumers moved (consumer registry)
│   │   └── K-COMPACTED: rebuild v<N+1> by republishing latest value per key (not by copying partitions)
│   │
│   ├── 8.4 If an in-place increase on an ordered topic is unavoidable (emergency) — high-risk procedure
│   │   ├── Stop ALL producers (including relays); drain to consumers' last stable offsets
│   │   ├── Alter partition count
│   │   ├── Restart EVERY producer so all hold the new partition count before any send [C]
│   │   ├── Consumers: held keys / version state keyed by aggregate (PT-10) — verify before resume
│   │   ├── Co-partitioned and compacted topics: not recoverable this way (use 8.3)
│   │   └── Reconciliation mandatory before declaring done
│   │
│   ├── F8.1 Producers resume with stale metadata — old and new layouts interleave — CORRUPT [C]
│   ├── F8.2 Compacted topic expanded — two live values per key across partitions — CORRUPT [C]
│   ├── F8.3 Waiting for lag "exactly 0" on transactional topic — never completes [C]
│   │   └── Handling: drain criterion = consumer position ≥ last stable offset at stop time
│   ├── F8.4 Held keys/parking tied to old partition owner — later versions bypass hold — CORRUPT [C]
│   │   └── Handling: PT-10
│   └── F8.5 Partition count increased by mistake — irreversible — forced migration [C]
│       └── Handling: IaC policy + admin ACLs (PT-04)
│
├── 9. PARTITION PLACEMENT & BROKER OPERATIONS
│   ├── 9.1 Leader distribution balanced across brokers; automatic preferred-leader rebalancing [C]
│   ├── 9.2 Replica reassignment (moving partitions between brokers) does not change key mapping [C]
│   │   └── Throttled reassignment to protect live traffic
│   ├── 9.3 Broker failure: leader elections per partition → recovery time grows with partition count [C]
│   ├── 9.4 Rack awareness for replicas; consumer fetch-from-follower where latency/cost benefits [L]
│   ├── F9.1 Unthrottled reassignment saturates network — produce latency — DEGRADE [C]
│   └── F9.2 Leaders concentrated after broker restart — hot broker — DEGRADE [C]
│       └── Handling: preferred-leader election automation; alert on leader skew
│
├── 10. CONSUMER-SIDE IMPLICATIONS
│   ├── 10.1 Max parallel consumers = partitions; beyond that, key-hashed lanes inside consumers
│   ├── 10.2 Assignment skew: heavy partitions co-located on one consumer → monitor per-instance load
│   ├── 10.3 State keyed by aggregate (version guard, inbox, held keys) — survives any reassignment (PT-10)
│   └── F10.1 Consumer caches keyed by partition — stale after reassignment — CORRUPT [C]
│       └── Handling: caches keyed by aggregate, invalidated on assignment change
│
├── 11. MULTI-TENANT CONSIDERATIONS
│   ├── Key = tenant + aggregate (distribution across partitions within a tenant)
│   ├── Dedicated topics for large tenants sized independently (multi-tenant tree §2)
│   └── Tenant moves between topics = stream migration (8.3)
│
├── 12. OBSERVABILITY
│   ├── Per partition: bytes/records in, leader broker, consumer time lag, owning consumer
│   ├── Skew index per topic (max/median), sustained-skew alert
│   ├── Top-N keys per partition (sampled), hot-key classification output
│   ├── Partition count changes (any change on non-K-UNORDERED → page)
│   ├── Producer client inventory: library, version, configured partitioner per producer
│   ├── Affinity canary result (§13)
│   ├── Leader distribution per broker; under-replicated partitions
│   └── Absence alerts on all of the above
│
├── 13. VERIFICATION & CANARIES
│   ├── Golden vectors: fixed key set → expected partition for counts used; every language library tested in CI
│   ├── Production affinity canary: each producer family emits canary events for shared canary keys;
│   │   consumer asserts each key appears on exactly one partition
│   ├── Per-aggregate partition-set audit (sampled): aggregate observed on > 1 partition → page
│   └── Config audit: every producer's effective partitioner and key serializer, continuously
│
├── 14. RUNBOOKS
│   ├── RB-01 Affinity canary failure → identify producer family/config → fix partitioner → assess affected window →
│   │         resync affected aggregates (ordering tree §8.4)
│   ├── RB-02 Hot partition → classify (4.1) → apply class remediation
│   ├── RB-03 Capacity ceiling reached on ordered topic → plan new stream version (8.3)
│   ├── RB-04 Emergency in-place expansion (last resort) → 8.4 with reconciliation
│   ├── RB-05 Leader skew → preferred-leader election → verify balance
│   └── RB-06 Co-partitioned set mismatch detected → stop dependent processors → realign via new versions
│
└── 15. GO-LIVE TEST MATRIX
    ├── Go and JVM producers send the same 10,000 keys → identical partition for every key
    ├── Golden vectors including negative-hash keys → match Java client
    ├── Key builder in every language → identical bytes (case, separator, encoding)
    ├── Null key on K-ORDERED topic → rejected by producer library
    ├── IaC partition increase on K-ORDERED → rejected
    ├── Stream migration v1→v2 with higher partition count → zero reorder, zero loss, reconciliation clean
    ├── Compacted topic migration → latest value per key correct in v2
    ├── Hot single aggregate at 3× normal → handled within single-partition ceiling or flagged for re-model
    ├── Broker failure with production partition count → recovery time within SLO
    ├── Rebalance with production partition count → duration within SLO
    ├── DR mirror partition counts → identical to source
    └── Metrics pipeline down → absence alerts fire
```

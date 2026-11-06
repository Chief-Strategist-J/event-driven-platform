# TODO — Event-Driven Platform (Senior/Staff System Architect prep)

Topics: Kafka, CQRS, Event Sourcing, CDC, schema evolution, replay.
Primary company fit: **Google** (Pub/Sub, Dataflow, large-scale data pipelines), **Stripe** (webhooks, event-driven money movement), **Anthropic** (streaming inference events, usage telemetry, eval pipelines), **Neo4j** (CDC into graph; Kafka Connect sink).

Current state: scaffold + 26 feature `context.yml`; only `src/features/event-schema-registry/index.go` has code. `messaging/engine/{client,producers,consumers,pipeline}` are empty `.gitkeep`.

Every WP has an **AC** (acceptance criteria). Not done until demonstrated by test/benchmark/artifact.

---

## 0. Definition of done for "interview-ready"
- [ ] Can design and defend an event-driven order/payment/usage system in 35 min including ordering, replay, schema evolution, and failure handling
- [ ] Can explain Kafka internals without notes (section 3)
- [ ] Can say clearly when NOT to use events / CQRS / event sourcing
- [ ] Benchmarks recorded (section 5) and used to justify partition/consumer sizing

## 1. Architecture artifacts (`docs/`)
- [x] `00-requirements.md`: functional + NFR numbers (e.g. 50k events/s peak, p99 produce->consume < 200 ms, 7-day retention, full replay in < 4 h, consumer lag SLO < 30 s at p99)
- [x] `01-c4.md`: Context/Container/Component diagrams
- [x] `02-event-catalog.md`: every event, owner, schema version, key, partition count, consumers, PII flag
- [x] `03-topic-design.md`: naming (`<domain>.<aggregate>.<event>.v<N>`), partitions per topic with math (target throughput / per-partition throughput, headroom 2-3x), RF=3, `min.insync.replicas=2`, retention, compaction rules
- [x] `04-consistency-contract.md`: what readers of the read model may assume (staleness bound, read-your-writes mechanism)
- [x] `05-capacity-model.md`: events/s x avg size x RF x retention = disk; network fan-out; consumer CPU; broker count
- [x] `06-threat-model.md`: ACLs, spoofed producers, PII in events, replay of malicious events, schema-registry poisoning
- [ ] `07-runbooks/`: consumer lag, rebalance storm, poison message, broker loss, projection rebuild, schema rollback
- [ ] ADRs:
  - [ ] ADR-001 Event sourcing vs state-stored + domain events
  - [ ] ADR-002 CQRS read store choice (Postgres/Elastic/Redis/Neo4j) per query shape
  - [ ] ADR-003 One topic per aggregate vs per event type (ordering vs consumer filtering)
  - [ ] ADR-004 Partition key and ordering guarantees; hot-key mitigation
  - [ ] ADR-005 Serialization: Avro vs Protobuf vs JSON Schema; compatibility mode (BACKWARD_TRANSITIVE vs FULL_TRANSITIVE)
  - [ ] ADR-006 CDC (Debezium) vs application outbox; handling schema change and snapshots
  - [ ] ADR-007 Delivery semantics: at-least-once + idempotent consumer vs Kafka transactions/EOS
  - [ ] ADR-008 Kafka vs Pub/Sub vs Pulsar vs Kinesis vs RabbitMQ vs SQS (decision matrix)
  - [ ] ADR-009 Multi-region: MirrorMaker 2 / Cluster Linking, active-passive vs active-active, offset translation

## 2. Work packages

### WP-1 Event design (`domain-event-design`, `event-driven-communication`)
- [ ] Envelope: `event_id` (UUIDv7), `type`, `schema_version`, `aggregate_id`, `aggregate_seq`, `occurred_at`, `causation_id`, `correlation_id`, `tenant_id`, `trace_context`
- [ ] Rules: past-tense business facts, no CRUD names, no leaking internal DB shapes, thin vs fat events decision (event-carried state transfer vs notification)
- [ ] **AC:** linter that rejects events violating the envelope/naming rules in CI

### WP-2 Schema registry + evolution (`event-schema-registry`, `schema-evolution`, `event-versioning`)
- [ ] Extend `index.go`: register, fetch by id/subject/version, compatibility check endpoint, soft delete
- [ ] Upcasters chain (v1->v2->v3) for replay; tolerant reader on consumers
- [ ] CI gate: PR fails on incompatible schema
- [ ] **AC:** matrix test of old/new producers x old/new consumers passes under chosen compatibility mode; documented rule for breaking change = new topic version + dual publish + migration window

### WP-3 Partitioning and ordering (`partitioning-strategy`, `event-ordering-protection`)
- [ ] Key by aggregate id; consistent hashing awareness; hot-partition detection metric; plan for increasing partitions (key->partition remap breaks ordering — document the procedure)
- [ ] Per-aggregate sequence numbers; consumer detects gaps/duplicates/out-of-order; buffer-and-wait vs park-to-retry decision
- [ ] **AC:** simulated reordering/duplication test; projection ends correct

### WP-4 Producers (`messaging/engine/producers`)
- [ ] Idempotent producer (`enable.idempotence=true`, `acks=all`, bounded `max.in.flight`), batching (`linger.ms`, `batch.size`), compression (zstd vs lz4 measured), retries with delivery timeout
- [ ] **AC:** broker-kill test shows zero loss and zero reordering for a key

### WP-5 Consumers (`consumer-groups`, `offset-management`, `idempotent-consumers`)
- [ ] Manual commit after processing; cooperative-sticky assignor; static membership to avoid rebalance on restart
- [ ] Handler timeout < `max.poll.interval.ms`; pause/resume partitions; graceful shutdown commits and leaves group
- [ ] Idempotency: dedupe table keyed by `event_id` written in same tx as effect
- [ ] Offset reset tooling: by timestamp, by offset, to latest, with dry-run and audit
- [ ] **AC:** kill consumers repeatedly under load: no lost effect, duplicates absorbed, rebalance duration measured

### WP-6 Outbox + CDC (`transactional-outbox`, `cdc-integration`)
- [ ] Outbox table + relay (poll) and Debezium variant (outbox event router SMT)
- [ ] Handling of initial snapshot, schema change, replication slot lag (WAL growth risk!), failover of slot
- [ ] **AC:** documented slot-bloat incident simulation + alert

### WP-7 CQRS + projections (`cqrs`, `materialized-views`, `eventual-consistency-monitoring`)
- [ ] Command side validates and emits events; query side builds views; separate models
- [ ] Projection rebuild from earliest offset into a new versioned table, then atomic cutover (blue/green)
- [ ] Read-your-writes: return `aggregate_version` from command, query waits until projection >= version (bounded wait) or sticky routing
- [ ] Lag metric = (latest event time) - (projection watermark)
- [ ] **AC:** rebuild 10M events, measure time, prove zero downtime cutover

### WP-8 Event sourcing mechanics (inside `domain-event-design` + `replay-mechanism`)
- [ ] Event store table (aggregate_id, seq, type, payload, metadata) with unique (aggregate_id, seq) for optimistic concurrency
- [ ] Snapshots every N events; load = snapshot + tail
- [ ] Replay: by aggregate/time/offset; flag to suppress external side effects; isolate to separate consumer group + sandbox output
- [ ] Event correction: never edit history; compensating events; upcasting
- [ ] **AC:** aggregate load time vs event count graph, with and without snapshots

### WP-9 Retry/DLQ (`retry-topics`, `dead-letter-handling`)
- [ ] Tiered retry topics (e.g. 5s, 1m, 10m, 1h) with delay via pause or timestamp-gated consumer; DLQ with error reason, stack, original topic/partition/offset, attempts
- [ ] Redrive tool with filter + rate limit; classification retryable vs permanent
- [ ] **AC:** poison message never blocks main partition; DLQ redrive restores correct effect once

### WP-10 Backpressure (`backpressure`)
- [ ] Bounded in-flight, pause/resume, lag-based autoscaling (KEDA), producer-side quotas, load shedding for low-priority topics
- [ ] **AC:** 10x traffic burst test: system degrades gracefully; lag recovers; memory bounded

### WP-11 Security + tenancy (`encryption`, `event-authorization`, `multi-tenant-event-isolation`)
- [ ] TLS + SASL/OAuth, ACL per service principal (least privilege), quotas per tenant
- [ ] Field-level encryption for PII with per-subject keys -> crypto-shredding to satisfy erasure in an immutable log
- [ ] Tenant isolation: shared topics + tenant header + per-tenant quotas vs dedicated topics for large tenants
- [ ] **AC:** deleting a subject's key makes their events unreadable across replays

### WP-12 Observability + audit (`event-operations-dashboard`, `audit-trail`, `distributed-tracing`)
- [ ] Metrics: produce rate, consume rate, lag per group/partition, rebalance count, DLQ depth, schema versions in use, end-to-end latency
- [ ] Trace context in headers; span links for fan-out/batch consumption
- [ ] **AC:** a single event traceable from API call to projection in one trace

### WP-13 Infrastructure
- [ ] Fill `messaging/schemas`, `messaging/topics/topics.yaml` (generate + lint, `topics.lock` drift check), `database/migrations/sql`
- [ ] Verify Helm/ArgoCD/Terraform render; KRaft cluster config; rack awareness
- [ ] Decide on `rust-modules` (e.g. high-throughput envelope validator) / `cpp-modules` or delete

## 3. Deep-dive concepts to master
- [ ] Kafka log: segments, index/timeindex, page cache, zero-copy `sendfile`, why sequential IO is fast
- [ ] Replication: leader/follower, ISR, high watermark, leader epoch, unclean leader election, `acks` x `min.insync.replicas` durability matrix
- [ ] KRaft controller quorum (no ZooKeeper); metadata log
- [ ] Consumer group protocol: coordinator, generations, eager vs cooperative rebalance, static membership, `session.timeout.ms` vs `max.poll.interval.ms`
- [ ] Log compaction semantics, tombstones, `delete.retention.ms`
- [ ] Kafka transactions: producer epochs, transactional.id, read_committed, what EOS does NOT cover (external side effects)
- [ ] Tiered storage; quotas; KIP-848 new rebalance protocol
- [ ] Stream processing: windowing, watermarks, late data, exactly-once state with changelog topics (Kafka Streams/Flink)
- [ ] Event sourcing pitfalls: schema evolution forever, snapshot strategy, GDPR, query complexity, eventual consistency UX
- [ ] CQRS pitfalls: dual models, lag, rebuild cost; simpler alternatives (read replicas, materialized views)
- [ ] Debezium internals: WAL/logical decoding/binlog, replication slots, snapshot modes
- [ ] Google Pub/Sub semantics: at-least-once, ordering keys, ack deadlines, exactly-once option, push vs pull, dead letter; compare to Kafka in a table
- [ ] Dataflow/Beam model: event time vs processing time, windows, triggers, watermarks (the "Dataflow Model" paper)

## 4. Failure drills (`scripts/test/chaos/`)
- [ ] D1 consumer crash mid-batch
- [ ] D2 broker loss, ISR shrink, `acks=all` behavior with `min.insync.replicas=2`
- [ ] D3 rebalance storm from slow handler
- [ ] D4 poison pill on a partition
- [ ] D5 producer retries reorder (idempotence off vs on)
- [ ] D6 incompatible schema deployed
- [ ] D7 projection bug found after 3 days -> rebuild runbook timed
- [ ] D8 consumer lag spike triage
- [ ] D9 dual-write bug reproduction then outbox fix
- [ ] D10 replication slot growth fills disk
- [ ] D11 duplicate event with different payload (same id) -> detection
- [ ] D12 clock skew in `occurred_at` ordering assumptions
- **AC:** expected behavior documented, automated reproduction, runbook

## 5. Experiments (record in `docs/08-benchmarks.md`)
- [ ] Throughput vs partitions vs batch/linger vs compression matrix
- [ ] End-to-end latency percentiles at 10/50/80% of capacity
- [ ] Rebalance time: eager vs cooperative vs static membership
- [ ] Projection rebuild speed vs batch size and parallelism
- [ ] Event-store aggregate load with/without snapshots
- [ ] Cost per million events

## 6. Company-specific mapping

### Google
- [ ] Design "Pub/Sub from scratch" and "Dataflow-style streaming aggregation" on a whiteboard; know ack deadline and ordering-key design
- [ ] Understand Dremel/Colossus/Borg at overview level; talk about multi-tenant fairness and quotas
- [ ] Practice estimation: events/s -> partitions -> brokers -> cost, in 5 minutes

### Stripe
- [ ] Webhook delivery system design: at-least-once, signed (HMAC + timestamp to prevent replay), exponential backoff over days, per-endpoint circuit breaking, event ordering not guaranteed, event API for catch-up, idempotent handlers
- [ ] Events API versioning: events pinned to API version of account; how to evolve payloads safely
- [ ] Explain how events + reconciliation keep money state correct

### Neo4j
- [ ] Kafka Connect Neo4j sink / source: map events -> Cypher `MERGE` idempotently; ordering and tombstone handling
- [ ] Use the graph as a CQRS read model: relationship-heavy queries (recommendations, lineage) served from Neo4j fed by events
- [ ] Explain event-lineage as a graph (event -> caused -> event) and query it

### Anthropic
- [ ] Streaming (SSE) inference gateway design: backpressure from slow clients, cancellation propagation, partial-response billing
- [ ] Usage-event pipeline for token metering (links to `distributed-transaction-engine` metering saga)
- [ ] Eval/telemetry pipelines: sampling, PII scrubbing, privacy-preserving storage, retention
- [ ] Reliability: capacity-aware queueing for GPU workloads, priority classes, load shedding

## 7. Mock interview bank
1. Design a notification system (email/push/SMS) at 1B/day. *Strong: topics per channel, priority, dedupe, rate limit per user, retry tiers, templates, preference service, DLQ.*
2. Design Kafka (the system itself). *Strong: log, partitions, replication, ISR, consumer groups, offsets, retention, ordering scope.*
3. Design a real-time analytics pipeline (clicks -> dashboards). *Strong: ingestion, stream processing with windows/late data, OLAP store, exactly-once-ish via idempotent sinks.*
4. How do you guarantee ordering in Kafka at scale? *Strong: per-key only; key choice; one in-flight or idempotent producer; consumer concurrency per key; repartitioning caveat.*
5. A consumer group lags 2 hours — walk me through. *Strong: metrics, slow handler vs under-provisioned vs hot partition vs downstream, scale/shed/skip, backfill plan.*
6. How do you evolve a schema used by 40 consumers? *Strong: compatibility mode, expand/contract, consumer-driven contract tests, deprecation timeline, dual publish.*
7. Event sourcing: how do you handle GDPR erasure? *Strong: crypto-shredding, PII outside events, tombstone/compaction caveats.*
8. Design a webhook platform (Stripe). See Stripe section.
9. Design an audit log that can't be tampered with. *Strong: append-only, hash chain/Merkle, external anchoring, access control.*
10. Exactly-once in Kafka — what does it really give you? *Strong: within Kafka read-process-write; external effects need idempotency.*
11. Active-active multi-region event platform. *Strong: avoid multi-master per key, region affinity, offset translation, conflict resolution, failover drills.*
12. When would you refuse to use event sourcing? *Strong: simple CRUD, no audit need, small team, reporting-heavy ad hoc queries.*

## 8. Reading list
1. DDIA ch. 11 (stream processing) and ch. 12; Kafka: The Definitive Guide
2. Jay Kreps "The Log" essay
3. Confluent blogs: exactly-once, transactions, cooperative rebalancing, KIP-848
4. Fowler: Event Sourcing, CQRS, "What do you mean by Event-Driven?"
5. Debezium docs: outbox pattern, PostgreSQL connector
6. Google: Pub/Sub docs; "The Dataflow Model" paper; MillWheel
7. Stripe blog: webhooks, API versioning
8. Greg Young: event versioning book/talks
9. Pat Helland: "Immutability Changes Everything"

## 9. Weekly cadence
- Implement 1 WP + AC; write 1 ADR; run 1 drill or benchmark; 1 timed mock; 1 paper

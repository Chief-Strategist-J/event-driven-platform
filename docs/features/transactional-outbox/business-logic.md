```
TRANSACTIONAL OUTBOX — PRODUCTION DECISION TREE (v3)
Legend : [C] Certain   [L] Likely   [G] Guessing
Severity: LOSS (event gone) | CORRUPT (wrong downstream state) | OUTAGE (writes stop) | DEGRADE (latency/cost)
Every failure node reads:  Scenario → Impact → Handling → Detection
│
├── 0. INVARIANTS  (system-wide; each needs an owner + automated test + live monitor)
│   ├── INV-01 Atomicity [C]
│   │   └── Business mutation and outbox insert share ONE connection, ONE transaction, ONE database
│   ├── INV-02 Delivery semantics [C]
│   │   └── At-least-once end to end; "exactly-once" exists only as an idempotent EFFECT at the consumer
│   ├── INV-03 Per-aggregate ordering [C]  — holds only if ALL of these hold:
│   │   ├── Message key = aggregate_id
│   │   ├── All event types of one aggregate go to ONE topic
│   │   ├── Partition count of that topic never changes
│   │   ├── Every producer serializes the key to identical bytes
│   │   └── Relay preserves commit order per key and never sends k+1 before k is acked
│   ├── INV-04 Version monotonicity [C]
│   │   └── Each event carries aggregate_version = previous + 1, assigned under the aggregate row lock
│   ├── INV-05 Event identity [C]
│   │   └── event_id generated once, immutable, identical across retries, replays and re-publishes
│   ├── INV-06 Single writer [C]
│   │   └── Exactly one owning service and one primary database per aggregate type
│   ├── INV-07 No ghost events [C]
│   │   └── Never publish an event whose transaction can still be lost (async commit, async failover)
│   ├── INV-08 No silent loss [C]
│   │   └── Every committed event is either consumed, or detected as a gap within a bounded time (SLO)
│   ├── INV-09 Business idempotency [C]
│   │   └── Same command + same idempotency key → no second aggregate, no second event
│   ├── INV-10 No cross-aggregate atomicity downstream [C]
│   │   └── One tx touching N aggregates = N independent events on N partitions; consumers see partial state
│   ├── INV-11 Retention chain [C]
│   │   ├── Source-of-truth retention (outbox / audit)  ≥  Kafka topic retention
│   │   ├── Kafka topic retention  ≥  longest plausible consumer outage
│   │   ├── Consumer dedupe retention  ≥  largest replay window you will ever run
│   │   └── External provider idempotency window  ≥  consumer redelivery window
│   ├── INV-12 Payload fidelity [C]
│   │   └── Money as integer minor units or decimal string + currency; IDs as strings; no floats in contract
│   ├── INV-13 Contract stability [C]
│   │   └── Schema changes backward compatible only; breaking change = new event type or new schema version
│   └── INV-14 Observability completeness [C]
│       └── Absence of data is an alert condition, never a healthy state
│
├── 1. WRITE PATH  (inside the business transaction)
│   ├── 1.0 Normal flow
│   │   ├── Step 1: Idempotency-key claim for the incoming command (first statement in tx)
│   │   ├── Step 2: Business mutation; aggregate row updated with version + 1 under row lock
│   │   ├── Step 3: Payload built from POST-mutation state (DB defaults, triggers, generated values included)
│   │   ├── Step 4: Outbox insert with event_id, aggregate_type, aggregate_id, aggregate_version,
│   │   │           event_type, schema_version, correlation/trace id, source_service, payload
│   │   ├── Step 5 (CDC path only): delete the same outbox row in the same tx (WAL keeps the insert)
│   │   └── Step 6: COMMIT
│   │
│   ├── F1.1 Dual-write attempt (DB commit + direct Kafka send) — LOSS/CORRUPT [C]
│   │   ├── Impact: DB commits, Kafka fails (lost event) or Kafka sends, DB rolls back (ghost event)
│   │   ├── Handling: forbidden by architecture rule; no Kafka producer client in business services
│   │   └── Detection: dependency scan in CI; reconciliation (§9) finds ghosts/gaps
│   │
│   ├── F1.2 Ambiguous commit (connection drops during COMMIT) — CORRUPT [C]
│   │   ├── Impact: client cannot know if tx committed; naive retry → duplicate aggregate + duplicate event
│   │   ├── Handling: caller retries with SAME idempotency key; Step 1 makes the retry a no-op
│   │   └── Detection: metric on idempotency-key conflicts; spike = network instability
│   │
│   ├── F1.3 Framework silently splits the transaction — LOSS/CORRUPT [C]
│   │   ├── Scenario: "requires new" propagation, after-commit event listeners, separate connection pool,
│   │   │             async handler, outbox written from a different service layer
│   │   ├── Impact: INV-01 broken; passes unit tests, fails under partial failure
│   │   ├── Handling: single outbox-writer component that REQUIRES an existing tx and refuses otherwise
│   │   └── Detection: fault-injection test that aborts after outbox insert and asserts zero rows anywhere;
│   │                  reconciliation finds outbox rows without business rows or vice versa
│   │
│   ├── F1.4 Partial rollback via savepoint / exception block — CORRUPT [C]
│   │   ├── Impact: business write survives but outbox insert rolled back (or reverse)
│   │   ├── Handling: outbox insert must happen at the same savepoint level as the mutation it describes
│   │   └── Detection: reconciliation (§9)
│   │
│   ├── F1.5 Concurrent mutations on same aggregate — CORRUPT [C]
│   │   ├── Impact: two events with same version, or WAL commit order ≠ business order
│   │   ├── Handling: optimistic version check on aggregate row (0 rows updated → abort + retry)
│   │   │             row lock serializes commits per aggregate → commit order = version order
│   │   └── Detection: uniqueness on (aggregate_type, aggregate_id, aggregate_version) in outbox/audit
│   │
│   ├── F1.6 Payload built from request DTO, not persisted state — CORRUPT [C]
│   │   ├── Impact: event missing DB-generated IDs, defaults, trigger-modified values
│   │   ├── Handling: build payload from values returned by the mutation itself
│   │   └── Detection: contract test comparing event payload to row state after commit
│   │
│   ├── F1.7 Oversized payload — OUTAGE for that aggregate, LOSS if tolerated [C]
│   │   ├── Impact: exceeds Kafka size limits → permanent poison event, blocks the key forever
│   │   ├── Handling: size cap enforced at write time (business tx fails, not the relay);
│   │   │             large data via claim-check (object store, event carries reference + checksum)
│   │   └── Detection: payload size histogram; alert at 50% of the smallest limit in the chain
│   │
│   ├── F1.8 Bulk mutation emits massive event count in one tx — DEGRADE→OUTAGE [C]
│   │   ├── Impact: logical decoding spills to disk, relay memory pressure, consumer burst,
│   │   │           every other tenant's events delayed behind it
│   │   ├── Handling: chunk bulk jobs into bounded transactions; rate-limit; separate bulk lane (§3 F3.14)
│   │   └── Detection: events-per-transaction histogram; decode spill metrics
│   │
│   ├── F1.9 Numeric precision loss (money, 64-bit IDs) — CORRUPT, financial [C]
│   │   ├── Impact: JSON numbers parsed as float64 in many runtimes; IDs > 2^53 rounded; cents drift
│   │   ├── Handling: INV-12 enforced by schema; reject float types at schema validation
│   │   └── Detection: reconciliation of monetary totals between producer and consumer ledgers
│   │
│   ├── F1.10 Thin event + read-back to producer — CORRUPT [C]
│   │   ├── Impact: consumer reads NEWER state, deleted row (404), or OLDER state from lagging replica
│   │   ├── Handling: state-carrying events; if read-back unavoidable, request "version ≥ N" and retry
│   │   └── Detection: consumer metric on version-mismatch on read-back
│   │
│   ├── F1.11 Multiple writers for one aggregate type — CORRUPT [C]
│   │   ├── Scenario: active-active regions, two services both mutating "order"
│   │   ├── Impact: two independent streams; ordering and versioning meaningless
│   │   ├── Handling: INV-06; other services request changes via commands, never write directly
│   │   └── Detection: source_service header; alert if one aggregate_type has >1 source
│   │
│   ├── F1.12 Outbox table DDL blocks all writes — OUTAGE [C]
│   │   ├── Scenario: index build without concurrent mode, column with volatile default, any strong lock
│   │   ├── Impact: lock waits in queue; every business tx queues behind it → full write outage
│   │   ├── Handling: short lock timeout on all migrations touching outbox; concurrent index builds;
│   │   │             additive nullable columns only; migration review gate for outbox table
│   │   └── Detection: lock wait duration alert; blocked-session count
│   │
│   ├── F1.13 Insert hot spot at high TPS — DEGRADE [L]
│   │   ├── Impact: right-edge index page contention on time-ordered keys; WAL volume per insert
│   │   ├── Handling: CDC path keeps only primary key on outbox; no secondary indexes
│   │   └── Detection: insert latency p99 on outbox vs business tables
│   │
│   ├── F1.14 Invalid outbox row (null key, null type, null payload) — LOSS if relay skips it [C]
│   │   ├── Handling: database-level constraints so the BUSINESS tx fails, never the relay
│   │   └── Detection: constraint violation counter on write path
│   │
│   ├── F1.15 Asynchronous commit on business tx — LOSS/ghost [C]
│   │   ├── Impact: tx acknowledged to client but lost on crash; event may already be published
│   │   ├── Handling: synchronous commit enforced for any role that writes outbox
│   │   └── Detection: config audit; role-level setting check at startup
│   │
│   ├── F1.16 Time semantics misuse — CORRUPT [C]
│   │   ├── Impact: created_at is tx START time, not commit time; IDs from multiple app nodes skew
│   │   ├── Handling: never order by timestamp or ID; order only by aggregate_version per key;
│   │   │             timezone-aware timestamps only
│   │   └── Detection: consumer gap/reorder detector (§6)
│   │
│   └── F1.17 Personal data in payload — compliance [C]
│       ├── Impact: erasure request deletes DB row but data lives on in Kafka retention and consumers
│       ├── Handling: reference-only PII, or per-subject encryption key destroyed on erasure
│       └── Detection: schema classification of PII fields; erasure audit trail
│
├── 2. POSTGRESQL / WAL / REPLICATION SLOT TIER
│   ├── 2.0 Prerequisites
│   │   ├── Logical WAL level; enough replication slots and WAL senders with headroom for failover
│   │   ├── Native output plugin (pgoutput); publication containing ONLY the outbox table
│   │   ├── If outbox is partitioned: publication publishes via partition root (else child names leak)
│   │   └── Dedicated replication role with least privilege; not superuser
│   │
│   ├── F2.1 Relay or Kafka down → slot stops advancing → WAL grows — OUTAGE [C]
│   │   ├── Impact: disk fills; primary stops accepting writes; whole service down
│   │   ├── Handling: tiered alerts on slot lag (warn / page / critical) sized to disk headroom hours
│   │   │             WAL keep-size cap as last resort ONLY with §11 slot-loss runbook ready
│   │   └── Detection: slot lag bytes, slot active flag, WAL disk free %, time-to-full projection
│   │
│   ├── F2.2 Slot invalidated by WAL keep-size cap — LOSS [C]
│   │   ├── Impact: relay cannot resume; every event in the gap never published
│   │   ├── Handling: §11 R1 runbook; requires a retained source of truth (audit table or versions)
│   │   └── Detection: slot status "unreserved" = page now; "lost" = incident declared
│   │
│   ├── F2.3 Long-running or idle-in-transaction session — OUTAGE→LOSS [C]
│   │   ├── Impact: slot restart point cannot advance past oldest running tx; WAL accumulates
│   │   │           even with healthy relay; also blocks vacuum database-wide
│   │   ├── Handling: idle-in-transaction timeout and statement timeout for app AND human roles;
│   │   │             analytics never run on the primary
│   │   └── Detection: alert on oldest transaction age > 10 min
│   │
│   ├── F2.4 Orphaned prepared (2PC) transaction — OUTAGE [C]
│   │   ├── Impact: holds locks, vacuum horizon, WAL; its outbox row invisible indefinitely
│   │   ├── Handling: disable prepared transactions unless genuinely required
│   │   └── Detection: any prepared tx older than 1 minute → page
│   │
│   ├── F2.5 Idle outbox on a busy cluster — OUTAGE [C]
│   │   ├── Impact: no outbox events → slot never confirms → WAL from other DBs/tables retained
│   │   ├── Handling: connector heartbeat that performs a write so the slot advances
│   │   └── Detection: slot lag growing while outbox insert rate ≈ 0
│   │
│   ├── F2.6 Unrelated heavy WAL (bulk loads, reindex, big updates) — DEGRADE [C]
│   │   ├── Impact: decoder processes ALL WAL; outbox events wait behind gigabytes of irrelevant changes
│   │   ├── Handling: isolate outbox-critical DB from bulk/analytics workloads; schedule bulk windows
│   │   └── Detection: canary latency (§9) spikes correlated with WAL generation rate
│   │
│   ├── F2.7 Very large transactions — DEGRADE [C]
│   │   ├── Impact: decoding memory exceeded → spill to disk; delayed emission of the whole tx
│   │   ├── Handling: chunk bulk writes (F1.8); size decoding memory deliberately
│   │   └── Detection: decode spill counters in replication slot stats
│   │
│   ├── F2.8 Stale slot holds catalog horizon — DEGRADE→OUTAGE [C]
│   │   ├── Impact: system catalog bloat; DDL and planning slow down cluster-wide
│   │   ├── Handling: never leave unused slots; drop slots of decommissioned connectors immediately
│   │   └── Detection: inventory of slots vs known connectors; any inactive slot → alert
│   │
│   ├── F2.9 Major version upgrade — LOSS or mass duplicates [C]
│   │   ├── Impact: before v17, upgrade does not carry logical slots; managed blue/green recreates them
│   │   ├── Handling: §10 L4 upgrade runbook (drain → record position → upgrade → recreate → reconcile)
│   │   └── Detection: post-upgrade reconciliation mandatory
│   │
│   ├── F2.10 Updates to outbox rows under CDC — CORRUPT [L]
│   │   ├── Impact: relay emits unexpected update records; router warns or misroutes
│   │   ├── Handling: CDC path is insert(+delete) only; status columns exist only on poller path
│   │   └── Detection: count of non-insert change events from outbox (should be deletes only)
│   │
│   └── F2.11 Table bloat from churn — DEGRADE [C]
│       ├── Impact: insert+delete (CDC) or status updates (poller) create dead tuples continuously
│       ├── Handling: aggressive per-table autovacuum on outbox; monitor vacuum lag
│       └── Detection: dead tuple ratio, last autovacuum age on outbox
│
├── 3. RELAY PATH A — LOG-BASED CDC (Debezium)  [recommended]
│   ├── 3.0 Mandatory configuration decisions (pin all; version-controlled; reviewed)
│   │   ├── Route by aggregate_type ONLY; event_type travels as header (INV-03)
│   │   ├── Key = aggregate_id, serialized as plain string (INV-03)
│   │   ├── event_id exposed as header for consumer dedupe (INV-05)
│   │   ├── Error tolerance = none (fail loudly, never skip)
│   │   ├── Slot is NOT dropped on stop
│   │   ├── Snapshot mode = no historical data on (re)start
│   │   ├── Publication created manually, outbox only (no auto-create for all tables)
│   │   ├── Heartbeat enabled with write action (F2.5)
│   │   ├── Producer: idempotence on, acks from all in-sync replicas
│   │   └── Payload wire format decided once (expanded JSON object vs string) and contract-tested
│   │
│   ├── F3.1 Connector crash between Kafka write and offset flush — duplicates [C]
│   │   ├── Impact: restart replays from last flushed position → events re-sent
│   │   ├── Handling: expected under INV-02; consumers idempotent (§6)
│   │   └── Detection: consumer duplicate-rate metric; spike correlates with restarts
│   │
│   ├── F3.2 Task FAILED while connector reports RUNNING — OUTAGE→LOSS [C]
│   │   ├── Impact: no events flow; slot inactive; WAL grows; no automatic restart
│   │   ├── Handling: automated restart with backoff; human page after N failures
│   │   └── Detection: alert on TASK state (not connector state); canary staleness
│   │
│   ├── F3.3 Errors tolerated and skipped — LOSS [C]
│   │   ├── Impact: transform/converter failures silently dropped; source side has no dead-letter topic
│   │   ├── Handling: zero tolerance; invalid rows prevented at write time (F1.14)
│   │   └── Detection: config audit; reconciliation
│   │
│   ├── F3.4 Slot dropped on connector stop — LOSS [C]
│   │   ├── Impact: every restart/redeploy creates a gap
│   │   ├── Handling: explicit config; deploy pipeline asserts slot exists after restart with same position
│   │   └── Detection: slot creation-time change alert
│   │
│   ├── F3.5 Unintended snapshot on restart — mass duplicates / replay storm [C]
│   │   ├── Scenario: offsets lost, connector renamed, snapshot mode left at default
│   │   ├── Handling: snapshot disabled for outbox; renames treated as migrations (§10 L3)
│   │   └── Detection: sudden publish spike with old event timestamps
│   │
│   ├── F3.6 Connect internal topics misconfigured or lost — duplicates or gap [L]
│   │   ├── Impact: offsets/config/status lost → connector forgets position
│   │   ├── Handling: compacted, replication factor 3, min in-sync 2, pre-created, never auto-created
│   │   └── Detection: config audit at deploy
│   │
│   ├── F3.7 Two Connect clusters share group identity — CORRUPT state [C]
│   │   ├── Handling: unique group and internal-topic names per environment and cluster
│   │   └── Detection: unexpected task assignments / rebalance storms
│   │
│   ├── F3.8 Key serialized differently than other producers — CORRUPT ordering [C/L]
│   │   ├── Scenario: structured/JSON-wrapped key vs plain string key; migration poller → CDC
│   │   ├── Impact: same aggregate hashed to different partitions → ordering broken
│   │   ├── Handling: plain string key converter; one serialization standard org-wide
│   │   └── Detection: test producing same key from every producer and asserting same partition
│   │
│   ├── F3.9 Payload double-encoded — CORRUPT contract [L]
│   │   ├── Impact: consumers parse JSON-inside-a-string; a later "fix" breaks all consumers at once
│   │   ├── Handling: format chosen at day one; consumer-driven contract tests in CI
│   │   └── Detection: schema validation at consumer ingress
│   │
│   ├── F3.10 Two connectors on same table/slot — duplicates or conflict [C]
│   │   ├── Scenario: blue/green deploy, forgotten old connector, migration overlap
│   │   ├── Handling: one slot per connector; single active owner enforced by deploy tooling
│   │   └── Detection: duplicate rate jump; "slot already active" errors
│   │
│   ├── F3.11 Outbox table schema change mid-stream — OUTAGE [C]
│   │   ├── Handling: additive-only changes; connector restart validated in staging first
│   │   └── Detection: task failure (F3.2)
│   │
│   ├── F3.12 Connector attached to wrong node — LOSS/OUTAGE [L]
│   │   ├── Scenario: stale DNS after failover, pooler in path, connection to a standby
│   │   ├── Handling: direct connection via writer endpoint; startup check "am I on the primary"; no pooler
│   │   └── Detection: connector host vs current primary identity check
│   │
│   ├── F3.13 Single-stream throughput ceiling — DEGRADE→OUTAGE [C]
│   │   ├── Impact: one connector = one task = one decode stream; extra slots re-decode all WAL
│   │   ├── Handling: load test at 2–3× peak; scale by sharding databases, not by adding tasks
│   │   └── Detection: slot lag trend at peak; canary latency p99
│   │
│   ├── F3.14 Noisy tenant / bulk lane starvation — DEGRADE [L]
│   │   ├── Impact: one tenant's bulk import delays every tenant's events (single ordered stream)
│   │   ├── Handling: rate-limit bulk at write side; separate outbox + connector for bulk with weaker SLO
│   │   └── Detection: per-tenant event rate; canary latency during imports
│   │
│   ├── F3.15 Connect worker rebalance — DEGRADE + duplicates [C]
│   │   ├── Handling: stable worker membership; cooperative rebalancing; expected dupes absorbed
│   │   └── Detection: rebalance count; publish pauses
│   │
│   └── F3.16 Debezium / Connect version upgrade changes defaults — CORRUPT contract [L]
│       ├── Handling: pin versions; diff effective config before/after; canary in staging with contract tests
│       └── Detection: contract test failures; header/key format drift
│
├── 4. RELAY PATH B — APPLICATION POLLER  [fallback]
│   ├── 4.0 Normal flow (lease-based, never holding DB locks across the network)
│   │   ├── Claim: short tx marks a bounded batch IN_FLIGHT with worker id + lease expiry; commit
│   │   ├── Send: publish with no DB transaction open
│   │   └── Confirm: short tx marks PUBLISHED only rows with per-record broker ack AND still owned by this worker
│   │
│   ├── F4.1 Row locks held during Kafka send — OUTAGE [C]
│   │   ├── Impact: long transactions; vacuum horizon pinned; bloat on every table
│   │   ├── Handling: lease model (4.0)
│   │   └── Detection: oldest transaction age owned by relay role
│   │
│   ├── F4.2 Multiple workers reorder events of one aggregate — CORRUPT [C]
│   │   ├── Handling: single worker, OR workers sharded by hash of aggregate_id;
│   │   │             never send version k+1 while k is unacked or parked
│   │   └── Detection: consumer reorder/gap detector (§6)
│   │
│   ├── F4.3 Commit-order visibility gap — LOSS (cursor) / CORRUPT (order) [C]
│   │   ├── Impact: tx with earlier ID/timestamp commits AFTER poller read past it;
│   │   │           watermark-based cursor skips it forever
│   │   ├── Handling: select by status, never by "greater than last seen ID/time"
│   │   └── Detection: reconciliation; PENDING rows older than relay cycle
│   │
│   ├── F4.4 Partial batch failure — LOSS or duplicates [C]
│   │   ├── Handling: per-record ack tracking; only acked records marked
│   │   └── Detection: rows stuck IN_FLIGHT past lease
│   │
│   ├── F4.5 Crash after broker ack, before marking — duplicates [C]
│   │   └── Handling: expected (INV-02)
│   │
│   ├── F4.6 Lease expires while original worker still sending — duplicates + reorder [C]
│   │   ├── Impact: second worker reclaims and sends; first worker's late sends interleave
│   │   ├── Handling: lease ≫ worst-case send time; confirm step fenced on ownership;
│   │   │             per-key serialization survives reclaim
│   │   └── Detection: reclaim counter; duplicates with different worker ids
│   │
│   ├── F4.7 Poison event — OUTAGE for that aggregate [C]
│   │   ├── Handling: attempt counter + last error; after N attempts → PARKED;
│   │   │             park the WHOLE aggregate (block later versions), never skip just one
│   │   └── Detection: parked-aggregate count must be 0; page on any
│   │
│   ├── F4.8 Backlog growth degrades claim query — DEGRADE→OUTAGE [C]
│   │   ├── Handling: partial index covering only unpublished rows; bounded batch size
│   │   └── Detection: claim query latency; oldest PENDING age
│   │
│   ├── F4.9 Status-update bloat — DEGRADE [C]
│   │   └── Handling: aggressive autovacuum; space reserved in pages for in-place updates
│   │
│   ├── F4.10 Producer retries reorder within a session — CORRUPT [C]
│   │   └── Handling: idempotent producer with bounded in-flight requests
│   │
│   └── F4.11 Cleanup deletes unpublished rows — LOSS [C]
│       ├── Handling: delete only PUBLISHED and older than retention (INV-11);
│       │             never drop a partition containing PENDING / IN_FLIGHT / PARKED rows
│       └── Detection: pre-drop assertion; cleanup job dry-run count
│
├── 5. KAFKA TIER
│   ├── 5.0 Mandatory topic configuration (pre-created, enforced by IaC, auto-create disabled)
│   │   ├── Replication factor 3, replicas spread across availability zones
│   │   ├── Minimum in-sync replicas 2
│   │   ├── Unclean leader election disabled
│   │   ├── Delete cleanup policy (never compaction for event topics)
│   │   ├── Retention ≥ longest consumer outage (INV-11)
│   │   └── Size limits aligned across broker, topic, producer request, consumer fetch
│   │
│   ├── F5.1 Min in-sync replicas = 1 — LOSS [C]   ← highest-severity misconfig
│   │   ├── Impact: "ack from all" means one broker; that broker dies → acked event gone;
│   │   │           outbox row already deleted/marked → unrecoverable without reconciliation
│   │   ├── Handling: 5.0
│   │   └── Detection: config drift alert; under-replicated partitions alert
│   │
│   ├── F5.2 Unclean leader election — LOSS [C]
│   │   └── Impact: out-of-sync replica becomes leader; acked writes truncated
│   │
│   ├── F5.3 All replicas in one zone — LOSS on zone failure [C]
│   │   └── Handling: rack-aware replica placement
│   │
│   ├── F5.4 Topic auto-created with defaults — LOSS/CORRUPT [C]
│   │   └── Impact: wrong replication, wrong partitions, wrong retention, silently
│   │
│   ├── F5.5 Partition count increased — CORRUPT ordering [C]
│   │   ├── Impact: key→partition mapping changes; in-flight aggregates split across partitions
│   │   └── Handling: never resize ordered topics; migrate to a new topic with coordinated cutover
│   │
│   ├── F5.6 Compaction on event topic — LOSS of history [C]
│   │   └── Impact: only last event per key retained; new/rebuilt consumers miss intermediate events
│   │
│   ├── F5.7 Retention shorter than consumer outage — LOSS at consumer [C]
│   │   └── Detection: consumer lag in TIME vs retention; alert at 50% of retention
│   │
│   ├── F5.8 Size limit mismatch across chain — OUTAGE (poison) [C]
│   │   └── Handling: one org-wide max size, enforced first at write path (F1.7)
│   │
│   ├── F5.9 Hot partition from one dominant aggregate — DEGRADE [C]
│   │   └── Handling: aggregate design review; split aggregate if ordering scope allows
│   │
│   ├── F5.10 Broker disk full / produce errors — OUTAGE propagates to DB WAL (F2.1) [C]
│   │   └── Detection: broker disk alerts tied into the same incident as slot lag
│   │
│   ├── F5.11 Producer delivery timeout — relay task failure (F3.2) [C]
│   │
│   ├── F5.12 Kafka transactions / exactly-once source mode — option, not default [G]
│   │   └── Verify support in your exact Connect + Debezium versions before relying on it;
│   │       consumer idempotency remains mandatory regardless
│   │
│   └── F5.13 Cluster migration / mirroring — duplicates, reorder, offset loss [L]
│       └── Handling: treat as a full migration with reconciliation before and after
│
├── 6. CONSUMER TIER  (the other half of the pattern)
│   ├── 6.0 Normal flow
│   │   └── In ONE consumer DB tx: version check → apply effect → record event_id → commit → then commit offset
│   │
│   ├── 6.1 Version rule (replaces the "≠ last + 1 → park" rule from v2, which was wrong)
│   │   ├── Incoming version ≤ last applied     → duplicate → acknowledge and skip
│   │   ├── Incoming version = last applied + 1 → apply
│   │   └── Incoming version > last applied + 1 → GAP = upstream loss → hold, do not apply, page
│   │
│   ├── F6.1 Offset committed before processing — LOSS [C]
│   │   ├── Scenario: auto-commit with async processing; crash after commit, before effect
│   │   └── Handling: commit offsets only after the consumer DB tx commits
│   │
│   ├── F6.2 Auto offset reset to "latest" — LOSS [C]
│   │   ├── Impact: expired/missing offsets → consumer silently jumps to now
│   │   └── Handling: fail loudly ("none") for critical consumers; explicit recovery procedure
│   │
│   ├── F6.3 Rebalance mid-batch — duplicates [C]
│   │   └── Handling: idempotent apply (6.0); cooperative rebalancing
│   │
│   ├── F6.4 Dedupe retention shorter than replay window — CORRUPT [C]
│   │   └── Handling: INV-11, or rely on permanent version state (6.1) instead of TTL'd dedupe table
│   │
│   ├── F6.5 External side effects outside the consumer tx — CORRUPT, financial [C]
│   │   ├── Scenario: charge card / send payout / send email, crash before inbox commit → redelivery
│   │   ├── Handling: idempotency key derived from event_id passed to provider;
│   │   │             or consumer writes intent to its own outbox and a dedicated worker performs the call
│   │   └── Detection: provider-side duplicate detection reports; financial reconciliation
│   │
│   ├── F6.6 Provider idempotency window shorter than redelivery window — CORRUPT [L]
│   │   └── Handling: INV-11; know each provider's window; block replays older than it
│   │
│   ├── F6.7 Poison event at consumer — OUTAGE for partition [C]
│   │   ├── Decision: block partition (safe, stalls others on that partition)
│   │   │         vs park the key + continue (needs per-key parking store)
│   │   ├── Never: send to dead-letter and continue with later versions of the same key
│   │   └── Detection: parked keys count; partition stall duration
│   │
│   ├── F6.8 Unknown event type or schema version — OUTAGE or CORRUPT [C]
│   │   ├── Decision per consumer: ignore-and-log (tolerant reader) vs halt (strict)
│   │   └── Must be decided explicitly and documented; never left to default exception handling
│   │
│   ├── F6.9 Cross-aggregate ordering assumption — CORRUPT [C]
│   │   └── Handling: INV-10; consumers wait for or tolerate missing related aggregates
│   │
│   └── F6.10 Replay storm after upstream recovery — OUTAGE downstream [L]
│       └── Handling: bounded batch size; downstream rate limits; backpressure; rehearsed drain
│
├── 7. DURABILITY & FAILOVER TIER
│   ├── F7.1 Ghost events after asynchronous failover — CORRUPT (INV-07) [C]
│   │   ├── Impact: relay published events whose tx never reached the new primary
│   │   ├── Handling: v17+: logical decoding waits for physical standbys (synchronized standby slots);
│   │   │             earlier: synchronous replication for outbox-writing DBs
│   │   └── Detection: post-failover reconciliation: events published with no matching business row
│   │
│   ├── F7.2 Logical slot missing on new primary — LOSS [C]
│   │   ├── Impact: v16 and below: logical slots not replicated → gap after failover
│   │   ├── Handling: v17+: failover-enabled slots synced to standby;
│   │   │             earlier: slot-sync extension or HA manager permanent slots + reconciliation
│   │   └── Detection: slot existence + position check as part of failover runbook
│   │
│   ├── F7.3 Split brain (old primary still accepting writes) — CORRUPT [C]
│   │   ├── Impact: two primaries emit events; versions collide
│   │   ├── Handling: fencing in HA manager; outbox version uniqueness surfaces conflicts
│   │   └── Detection: duplicate (aggregate, version) with different event_ids
│   │
│   ├── F7.4 Point-in-time restore / backup restore — duplicates AND gaps [C]
│   │   ├── Impact: restored state behind Kafka; versions reissued with different content
│   │   ├── Handling: restore = incident; freeze consumers; reconcile; version conflicts resolved manually
│   │   └── Detection: consumer sees same version with different event_id
│   │
│   ├── F7.5 Region disaster with independent DB and Kafka failover — CORRUPT [L]
│   │   ├── Impact: DB and Kafka recover to different points in time
│   │   └── Handling: documented RPO per component; reconciliation as part of DR exercise
│   │
│   └── F7.6 Managed-service specific slot/failover behavior — unknown [G]
│       └── Verify against current vendor docs for your exact platform; do not assume upstream behavior
│
├── 8. OBSERVABILITY TIER
│   ├── 8.1 Database signals
│   │   ├── Slot lag (bytes and projected time-to-disk-full)
│   │   ├── Slot status (reserved / extended / unreserved / lost) and active flag
│   │   ├── Oldest transaction age; prepared transaction count/age
│   │   ├── Inventory: slots vs known connectors (orphans)
│   │   └── Outbox dead tuple ratio; lock wait time on outbox
│   ├── 8.2 Relay signals
│   │   ├── Task state (not connector state); restart count
│   │   ├── Poller: oldest PENDING age, IN_FLIGHT past lease, parked aggregates
│   │   └── Publish rate vs outbox insert rate
│   ├── 8.3 Kafka signals
│   │   ├── Under-replicated / under-min-ISR partitions
│   │   ├── Topic config drift vs IaC
│   │   └── Broker disk
│   ├── 8.4 Consumer signals
│   │   ├── Lag in TIME (not messages), compared against retention
│   │   ├── Duplicate rate, gap detections (must be 0), parked keys (must be 0)
│   │   └── Unknown-type / schema rejection count
│   └── 8.5 Absence rules (INV-14)
│       └── Every critical metric also alerts when it stops reporting
│
├── 9. CORRECTNESS PROOF TIER  (the two mechanisms that actually prevent silent loss)
│   ├── 9.1 End-to-end canary
│   │   ├── Synthetic heartbeat event written every ~30s through the REAL write path
│   │   │   (same tx pattern, same outbox, same relay, same topic)
│   │   ├── Canary consumer measures end-to-end latency
│   │   ├── Alerts: staleness > 2 min; metric absent; latency SLO breach
│   │   └── Catches: every failure that stops flow, regardless of which component broke
│   │
│   └── 9.2 Reconciliation
│       ├── Hourly: business source of truth (aggregate versions changed in window)
│       │           vs consumer-recorded event_ids/versions for same window
│       ├── Source of truth under CDC delete pattern: small audit record per event, or aggregate versions
│       ├── Gap found → re-publish with the SAME event_id (idempotent consumers absorb overlap)
│       ├── Ghost found (event without business row) → incident (F7.1 class)
│       └── Financial domains: also reconcile monetary totals, not just counts
│
├── 10. LIFECYCLE & CHANGE MANAGEMENT
│   ├── L1 Schema evolution: backward-compatible only; schema version header; registry if typed formats
│   ├── L2 Outbox table migrations: lock timeout, concurrent builds, additive only (F1.12)
│   ├── L3 Relay migration (poller → CDC, connector rename, cluster move)
│   │   ├── Never run both relays live on the same rows
│   │   ├── Drain old relay to zero → record cutover point → start new relay from that point
│   │   └── Verify key serialization identical (F3.8); reconcile after
│   ├── L4 PostgreSQL major upgrade
│   │   └── Drain to zero lag → record position + last event per aggregate → upgrade →
│   │       recreate slot/publication → start relay without snapshot → reconcile
│   ├── L5 Kafka / Connect / Debezium upgrades: pinned versions, config diff, staging canary first
│   ├── L6 Retention and cleanup: governed by INV-11; cleanup jobs assert before deleting
│   ├── L7 Data erasure: per F1.17; erasure must reach Kafka and every consumer store
│   └── L8 Decommission: remove connector → drop its slot → drop publication → remove topics (in that order)
│
├── 11. RECOVERY RUNBOOKS
│   ├── R1 Slot lost / invalidated
│   │   └── Declare incident → recreate slot → bound the gap (last confirmed vs first new position)
│   │       → re-publish gap from source of truth with original event_ids → reconcile → close
│   ├── R2 Gap detected by consumer (6.1)
│   │   └── Hold the key → identify missing versions → re-publish → consumer resumes automatically
│   ├── R3 Poison / parked aggregate
│   │   └── Inspect → fix payload or consumer → unpark → drain in version order
│   ├── R4 Ghost event after failover
│   │   └── Identify events without business rows → compensating events or manual correction downstream
│   ├── R5 Replay of a time range or aggregate set
│   │   └── Bounded by INV-11 windows; rate-limited; original event_ids; announced to consumer owners
│   └── R6 Topic loss or misconfiguration
│       └── Within retention: re-publish from source of truth; beyond it: full reconciliation rebuild
│
└── 12. GO-LIVE GATE  (each test must end with reconciliation = zero gaps, zero ghosts)
    ├── Kill relay mid-batch
    ├── Kill Kafka partition leader under load
    ├── Fail over PostgreSQL primary under load
    ├── Force slot invalidation in staging and execute R1 end to end
    ├── Hold an idle transaction for 1 hour; verify alerts fire before damage
    ├── Drop network during COMMIT; verify idempotent retry
    ├── Drain a 24-hour backlog; verify downstream survives
    ├── Rehearse major version upgrade (L4)
    ├── Stop metrics pipeline; verify absence alerts fire
    └── Run a bulk import during peak; verify canary latency stays within SLO
```

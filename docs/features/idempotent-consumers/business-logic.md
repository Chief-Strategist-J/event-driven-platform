# Idempotent Consumers — Business Logic Decision Tree

```
IDEMPOTENT CONSUMERS — PRODUCTION DECISION TREE (v2)
Legend : [C] Certain   [L] Likely   [G] Guessing
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE
Every failure node reads:  Scenario → Impact → Handling → Detection
│
├── 0. INVARIANTS  (each needs owner + automated test + live monitor)
│   ├── IC-01 Atomic effect [C]
│   │   └── Dedupe record + business effect + (ideally) offset position commit in ONE transaction, ONE store
│   ├── IC-02 Effect-once, not delivery-once [C]
│   │   └── Transport is at-least-once forever; correctness lives entirely in the consumer
│   ├── IC-03 Identity [C]
│   │   └── Dedupe key = producer-assigned event_id, identical across every redelivery and re-publish
│   ├── IC-04 Two layers of protection [C]
│   │   ├── Layer 1 — event identity (event_id): catches transport and relay duplicates
│   │   └── Layer 2 — aggregate version: catches stale, reordered, and replayed events permanently
│   ├── IC-05 Version rule [C]
│   │   ├── version ≤ last applied        → duplicate/stale → acknowledge, skip
│   │   ├── version = last applied + 1    → apply
│   │   └── version > last applied + 1    → GAP → hold, never apply, page
│   ├── IC-06 Delta vs state semantics [C]
│   │   ├── State events (full snapshot of aggregate) → "strictly newer wins" is sufficient
│   │   └── Delta events (increment, append, transfer) → strict next-version + event_id dedupe both required
│   ├── IC-07 Offset after effect [C]
│   │   └── Offset is advanced only after the effect is durably committed; never before
│   ├── IC-08 Dedupe scope [C]
│   │   └── Dedupe key is (handler identity, event_id), never event_id alone
│   ├── IC-09 Dedupe window [C]
│   │   └── Window ≥ maximum time between FIRST and LAST possible delivery of the same event_id
│   │       (topic retention, outbox re-publish, reconciliation re-publish, DLQ re-drive, manual replay)
│   ├── IC-10 No ordering bypass [C]
│   │   └── No retry/DLQ path may apply version k after k+1 has been applied for the same aggregate
│   ├── IC-11 External effect idempotency [C]
│   │   └── Every non-transactional side effect carries an idempotency key derived from event_id
│   ├── IC-12 Business duplicates are not transport duplicates [C]
│   │   └── Two different event_ids describing the same business fact are deduped by business key, not event_id
│   └── IC-13 Silence is a failure [C]
│       └── A consumer that stops reporting is treated as failing
│
├── 1. DUPLICATE & REDELIVERY SOURCES  (what creates a second delivery, and whether event_id catches it)
│   ├── S1 Relay replay after crash (CDC offset not flushed / poller crash after ack) [C]
│   │   └── Same event_id, NEW Kafka offset → caught by event_id; NOT by offset-based dedupe
│   ├── S2 Producer retry within a producer session [C]
│   │   └── Prevented by idempotent producer (default on in modern clients); still possible if disabled
│   ├── S3 Consumer crash after effect, before offset commit [C]
│   │   └── Same offset re-read → caught by event_id or by transactional offset storage
│   ├── S4 Rebalance mid-batch / revoked partitions [C]
│   │   └── New owner re-reads from last committed offset
│   ├── S5 Poll interval exceeded → consumer kicked from group → offset commit rejected [C]
│   │   └── Slow processing silently turns into reprocessing loops; also creates zombies (S6)
│   ├── S6 Zombie consumer still processing after losing ownership [C]
│   │   └── Old and new owner process the SAME events CONCURRENTLY
│   ├── S7 Retry topic / DLQ re-drive [C]
│   │   └── Same event_id, possibly days later, possibly after newer versions applied
│   ├── S8 Manual offset reset / consumer group recreated / group renamed [C]
│   │   └── Bulk redelivery of entire retention window
│   ├── S9 Reconciliation re-publish from producer outbox [C]
│   │   └── Same event_id, potentially weeks later → tests IC-09
│   ├── S10 Cluster mirroring / migration [L]
│   │   └── Same event_id, new offsets, possibly different partition order
│   ├── S11 Aborted producer transactions read by uncommitted-isolation consumers [C]
│   │   └── Not a duplicate but a GHOST; consumer must use read-committed isolation if producers use transactions
│   └── S12 Business-level duplicate (user double-submit, upstream retry minting new event_id) [C]
│       └── DIFFERENT event_id → invisible to event_id dedupe → needs business key (IC-12)
│
├── 2. PREREQUISITES ON THE INBOUND EVENT  (reject the event if these are missing)
│   ├── P1 event_id present, globally unique, assigned at outbox write, never by relay or consumer [C]
│   │   └── Missing → derived key from topic+partition+offset dedupes ONLY S3/S4, not S1/S7/S9 (LOSS of protection)
│   ├── P2 aggregate_id + aggregate_version present for any aggregate-scoped projection [C]
│   ├── P3 Event creation time stamped by producer (immutable; used for dedupe partitioning, not ordering) [C]
│   ├── P4 Event type + schema version present [C]
│   ├── P5 Semantic class declared per event type: STATE or DELTA (IC-06) [C]
│   └── P6 Business idempotency key present where business duplicates are possible (IC-12) [C]
│
├── 3. MECHANISM SELECTION  (decided per handler, by sink type)
│   │
│   ├── Decision D1: Is the sink a transactional database the consumer owns?
│   │   ├── YES → 3.1 Transactional inbox (+ 3.2 version guard)          [primary pattern]
│   │   └── NO  → D2
│   ├── Decision D2: Is the write naturally idempotent (overwrite by key, full state)?
│   │   ├── YES → 3.3 Natural-key upsert with atomic external versioning
│   │   └── NO  → D3
│   ├── Decision D3: Is it an external side effect (payment, email, third-party API)?
│   │   ├── YES → 3.5 Side-effect idempotency patterns
│   │   └── NO  → D4
│   ├── Decision D4: Kafka-in → Kafka-out only, no external writes?
│   │   ├── YES → 3.6 Stream-processor exactly-once mode
│   │   └── NO  → re-architect: introduce a transactional sink or the consumer-side outbox (3.5)
│   └── Redis filter (3.4) is NEVER the sole mechanism for non-idempotent effects
│
│   ├── 3.1 TRANSACTIONAL INBOX  (same DB as the projection)
│   │   ├── Normal flow (one transaction):
│   │   │   ├── Step 1: Insert inbox record (handler, event_id, aggregate_id, version, event time)
│   │   │   │           using a conflict-ignoring insert and checking affected-row count
│   │   │   │           (a raised unique-violation ABORTS the whole PostgreSQL transaction)
│   │   │   ├── Step 2: 0 rows inserted → duplicate → rollback → advance offset
│   │   │   ├── Step 3: Version guard on the aggregate projection (3.2)
│   │   │   ├── Step 4: Apply business effect
│   │   │   ├── Step 5: Store consumed offset per partition in the SAME transaction (recommended)
│   │   │   └── Step 6: Commit → then commit Kafka offset (or rely on Step 5 on reassignment)
│   │   │
│   │   ├── F3.1.1 Unique-violation handled as an exception — OUTAGE/DEGRADE [C]
│   │   │   ├── Impact: transaction aborted; subsequent statements fail; noisy error logs; retries
│   │   │   ├── Handling: conflict-ignore insert + affected-row check; duplicate is a normal outcome
│   │   │   └── Detection: error-rate metric should not include duplicates
│   │   │
│   │   ├── F3.1.2 Inbox and projection in different databases — CORRUPT [C]
│   │   │   ├── Impact: inbox commits, projection fails (event "seen" but never applied) or reverse
│   │   │   ├── Handling: co-locate; if impossible, projection itself must be version-guarded (3.2)
│   │   │   └── Detection: reconciliation inbox vs projection
│   │   │
│   │   ├── F3.1.3 Permanent business error rolls back inbox row → infinite redelivery — OUTAGE [C]
│   │   │   ├── Handling: classify error (§6); permanent → park the aggregate, record failure durably
│   │   │   └── Detection: same event_id retried > N times
│   │   │
│   │   ├── F3.1.4 Partitioned inbox loses uniqueness — CORRUPT [C]
│   │   │   ├── Impact: uniqueness enforced only within a partition; cross-partition duplicates accepted
│   │   │   ├── Handling A: partition by producer-stamped event creation time (identical on every redelivery)
│   │   │   ├── Handling B: unpartitioned table + batched deletes
│   │   │   ├── Handling C: rely on version guard (permanent) as primary; inbox as secondary
│   │   │   └── NEVER partition by processing time
│   │   │
│   │   ├── F3.1.5 Concurrent processing of the same event (S6 zombie) — CORRUPT [C]
│   │   │   ├── Impact: both transactions race; the unique insert serializes them, second waits then no-ops
│   │   │   ├── Risk remains: zombie applies older version AFTER new owner applied newer (no version guard)
│   │   │   ├── Handling: version guard (3.2) makes stale zombie writes no-ops; optional partition-epoch fencing
│   │   │   └── Detection: concurrent-duplicate counter (insert waited on lock)
│   │   │
│   │   ├── F3.1.6 Batch inserts deadlock — OUTAGE [C]
│   │   │   ├── Impact: two consumers inserting overlapping event_ids in different orders deadlock
│   │   │   ├── Handling: sort batch by key before writing; keep batches per partition
│   │   │   └── Detection: deadlock counter
│   │   │
│   │   ├── F3.1.7 Inbox becomes the hot spot — DEGRADE [L]
│   │   │   ├── Impact: every event writes one more indexed row; bloat; vacuum pressure
│   │   │   ├── Handling: minimal columns; per-table autovacuum tuning; retention per IC-09
│   │   │   └── Detection: inbox insert latency p99; dead tuple ratio
│   │   │
│   │   └── F3.1.8 Isolation level assumptions — CORRUPT [C]
│   │       ├── Impact: read-then-write projection logic under read-committed loses concurrent updates
│   │       ├── Handling: single-statement conditional updates or row lock on the aggregate projection row
│   │       └── Detection: reconciliation of aggregates vs producer state
│   │
│   ├── 3.2 VERSION GUARD  (state-based idempotency; permanent; mandatory for aggregate projections)
│   │   ├── Normal flow: projection row stores last applied version; update applies only per IC-05
│   │   │
│   │   ├── F3.2.1 "Greater than or equal" guard — CORRUPT [C]
│   │   │   ├── Impact: same version re-applied; delta events double-applied; divergent same-version content accepted
│   │   │   └── Handling: strictly newer for STATE; exactly next for DELTA
│   │   │
│   │   ├── F3.2.2 "Strictly newer" used for DELTA events — CORRUPT [C]
│   │   │   ├── Impact: skipping from v5 to v7 applies v7's delta without v6's → wrong balance
│   │   │   └── Handling: DELTA requires exactly next version; gap → hold (IC-05)
│   │   │
│   │   ├── F3.2.3 Gap held forever — OUTAGE for that aggregate [C]
│   │   │   ├── Handling: gap timeout → page → producer-side re-publish (outbox runbook)
│   │   │   └── Detection: held-aggregate count and age
│   │   │
│   │   ├── F3.2.4 Same version, different content (producer restore / split brain upstream) — CORRUPT [C]
│   │   │   ├── Handling: store content hash with version; mismatch → park + incident, never overwrite silently
│   │   │   └── Detection: hash-mismatch counter (must be 0)
│   │   │
│   │   └── F3.2.5 Aggregate created out of order (update arrives before create) — CORRUPT/OUTAGE [C]
│   │       └── Handling: version 1 is the only valid first event; anything else = gap rule
│   │
│   ├── 3.3 NATURAL-KEY UPSERT  (search index, key-value store, document store)
│   │   ├── Normal flow: write whole document keyed by aggregate_id with version enforced BY THE STORE
│   │   │
│   │   ├── F3.3.1 Check-then-write race — CORRUPT [C]
│   │   │   ├── Impact: read version, then write → concurrent writer overwrites newer with older
│   │   │   ├── Handling: store-native conditional write (external versioning: accept only strictly greater)
│   │   │   └── Detection: version regressions sampled from store
│   │   │
│   │   ├── F3.3.2 Partial document updates treated as idempotent — CORRUPT [C]
│   │   │   ├── Impact: scripted increments / appends re-applied on redelivery
│   │   │   └── Handling: only full-state overwrites qualify; partial updates need 3.1
│   │   │
│   │   ├── F3.3.3 Deletes and resurrection — CORRUPT [C]
│   │   │   ├── Impact: delete applied, then stale update re-creates the document
│   │   │   ├── Handling: tombstone with version retained (not hard delete) for at least IC-09 window
│   │   │   └── Detection: documents existing for aggregates deleted upstream
│   │   │
│   │   ├── F3.3.4 Store's write acknowledged but not durable/visible — LOSS [L]
│   │   │   ├── Impact: async replication / refresh intervals; failover loses acknowledged writes
│   │   │   ├── Handling: durability setting appropriate to criticality; reconciliation
│   │   │   └── Detection: periodic count/version reconciliation
│   │   │
│   │   └── F3.3.5 Multi-document effect from one event — CORRUPT [C]
│   │       └── Handling: each document version-guarded independently; partial failure → retry whole event
│   │
│   ├── 3.4 REDIS DEDUPE FILTER  (optimization only; never the source of truth)
│   │   ├── Valid use: shedding obvious duplicates before an already-idempotent sink (load reduction)
│   │   │
│   │   ├── F3.4.1 Mark-before-process — LOSS [C]
│   │   │   ├── Impact: crash after marking → every redelivery dropped
│   │   │   ├── Handling: two states — PROCESSING (short lease) → COMPLETED (long TTL);
│   │   │   │             redelivery seeing PROCESSING → retry later, never drop
│   │   │   └── Detection: keys stuck in PROCESSING beyond lease
│   │   │
│   │   ├── F3.4.2 Mark-after-process — duplicates [C]
│   │   │   └── Impact: crash between sink write and marking → re-applied (sink must tolerate)
│   │   │
│   │   ├── F3.4.3 Not atomic with the sink — CORRUPT [C]
│   │   │   └── Impact: no single commit spans Redis and sink; one of F3.4.1 / F3.4.2 is unavoidable
│   │   │
│   │   ├── F3.4.4 Eviction under memory pressure — duplicates [C]
│   │   │   ├── Impact: eviction policy removes dedupe keys before TTL
│   │   │   └── Handling: no-eviction policy on dedupe instance; capacity alert
│   │   │
│   │   ├── F3.4.5 Failover loses acknowledged keys — duplicates [C]
│   │   │   └── Impact: asynchronous replication; acknowledged writes lost on primary failure
│   │   │
│   │   ├── F3.4.6 TTL shorter than IC-09 window — duplicates [C]
│   │   │   └── Impact: 24h TTL vs 7-day retention / weeks-later re-publish
│   │   │
│   │   └── F3.4.7 Cluster key distribution / multi-key ops — DEGRADE [L]
│   │       └── Handling: single-key operations only; no cross-slot transactions
│   │
│   ├── 3.5 EXTERNAL SIDE EFFECTS  (payments, payouts, emails, SMS, third-party APIs)
│   │   ├── Pattern A — Provider idempotency key
│   │   │   ├── Key derived deterministically from (handler, event_id) → same on every retry
│   │   │   ├── F3.5.1 Provider window shorter than redelivery window — CORRUPT, financial [C]
│   │   │   │   └── Handling: know each provider's window; refuse re-drive of events older than it
│   │   │   ├── F3.5.2 Key reused with different parameters — provider error/OUTAGE [L]
│   │   │   │   └── Handling: parameters derived only from immutable event content
│   │   │   └── F3.5.3 Timeout with unknown outcome — CORRUPT [C]
│   │   │       └── Handling: retry with SAME key (provider returns original result); never mint a new key
│   │   │
│   │   ├── Pattern B — Consumer-side outbox (intent table)
│   │   │   ├── Consumer tx records intent (inbox + intent) atomically
│   │   │   ├── Dedicated worker executes intent with Pattern A key, records outcome
│   │   │   └── Gives retry, audit, and reconciliation for every external effect
│   │   │
│   │   ├── Pattern C — Provider without idempotency support
│   │   │   ├── Query-before-act using provider reference derived from event_id (race window remains) [L]
│   │   │   └── Human-reviewed exception queue for unknown outcomes on financial effects
│   │   │
│   │   └── F3.5.4 Non-reversible effects (email sent, SMS delivered) — accepted duplicates [C]
│   │       └── Handling: explicitly document "at-least-once" for these channels; user-facing dedupe hints
│   │
│   └── 3.6 STREAM-PROCESSOR EXACTLY-ONCE (Kafka-in → Kafka-out)
│       ├── Covers: consume + state store + produce atomically within Kafka
│       ├── F3.6.1 Any external write inside the processor — NOT covered → CORRUPT [C]
│       ├── F3.6.2 Downstream consumers on uncommitted isolation — ghosts (S11) [C]
│       └── F3.6.3 Producer fencing / transaction timeouts under long processing — OUTAGE [L]
│
├── 4. ORDERING, CONCURRENCY & PARALLELISM
│   ├── F4.1 Parallel processing within one partition (thread pool / async) — CORRUPT [C]
│   │   ├── Impact: v6 applied before v5; offsets committed past unfinished work → LOSS on crash
│   │   ├── Handling: parallelize ACROSS keys only (key-ordered workers); commit only contiguous completed offsets
│   │   └── Detection: gap/stale counters (IC-05)
│   │
│   ├── F4.2 Non-blocking retry topics — CORRUPT [C]
│   │   ├── Impact: failed v5 goes to retry topic; v6 applied; v5 later applied over v6
│   │   ├── Handling: when a key has an event in retry, all later events for that key also divert (key parking);
│   │   │             or blocking retry in-partition
│   │   └── Detection: stale-apply attempts blocked by version guard (should be visible, not silent)
│   │
│   ├── F4.3 DLQ re-drive after newer versions applied — CORRUPT [C]
│   │   └── Handling: re-drive passes through version guard; stale → skipped and reported, not forced
│   │
│   ├── F4.4 Zombie consumer (S6) — CORRUPT [C]
│   │   ├── Handling: version guard; processing time bounded well under poll interval;
│   │   │             optional fencing token (group generation) stored with offsets
│   │   └── Detection: writes from instances that no longer own the partition
│   │
│   ├── F4.5 Slow handler exceeds poll interval (S5) — OUTAGE (reprocess loop) [C]
│   │   ├── Handling: bound batch size; move heavy work to Pattern B worker; pause/resume partitions
│   │   └── Detection: rebalance rate; same offsets processed repeatedly
│   │
│   ├── F4.6 Cross-aggregate dependencies — CORRUPT [C]
│   │   ├── Impact: event for aggregate B references A not yet seen (different partitions)
│   │   ├── Handling: tolerate missing references (placeholder, retry later) — never assume arrival order
│   │   └── Detection: unresolved-reference count and age
│   │
│   └── F4.7 Hot key — DEGRADE [C]
│       └── Handling: key-ordered workers keep it serialized; capacity planned for the hottest key
│
├── 5. OFFSET MANAGEMENT
│   ├── F5.1 Offset committed before effect (auto-commit + async processing) — LOSS [C]
│   │   └── Handling: auto-commit off for critical handlers; commit after DB commit
│   │
│   ├── F5.2 Offset commit fails after DB commit — duplicates [C]
│   │   └── Handling: expected; absorbed by inbox / version guard
│   │
│   ├── F5.3 Offsets stored only in Kafka, sink restored from backup — LOSS [C]
│   │   ├── Impact: sink rewinds to backup point; Kafka offsets remain ahead; events in between never re-read
│   │   ├── Handling: store offsets in the sink transaction (IC-01) and seek to them on assignment
│   │   └── Detection: post-restore reconciliation mandatory
│   │
│   ├── F5.4 Offsets expire for idle/stopped group — LOSS or mass redelivery [C]
│   │   ├── Impact: group offsets retention elapses; reset policy decides: latest (LOSS) / earliest (replay)
│   │   └── Handling: "fail on missing offset" for critical groups; sink-stored offsets (F5.3)
│   │
│   ├── F5.5 Reset policy "latest" — LOSS [C]
│   │   └── Handling: never for critical consumers; explicit recovery procedure instead
│   │
│   └── F5.6 Consumer group renamed / redeployed with new id — mass redelivery or LOSS [C]
│       └── Handling: group id is a versioned, reviewed config; migration runbook (§12 L3)
│
├── 6. FAILURE CLASSIFICATION, RETRY & POISON HANDLING
│   ├── 6.0 Classify every error before retrying
│   │   ├── Transient (timeout, lock wait, connection reset, 5xx) → bounded retry with backoff, same partition
│   │   ├── Permanent data (schema invalid, missing required field) → park aggregate, page
│   │   ├── Permanent logic (handler bug) → park aggregate, page, fix, re-drive
│   │   ├── Dependency outage (sink down) → pause partitions, do not burn retries, do not DLQ
│   │   └── Unknown → treat as transient up to a limit, then permanent
│   │
│   ├── F6.1 Everything retried forever — OUTAGE (partition stall) [C]
│   ├── F6.2 Everything sent to DLQ — CORRUPT (ordering, IC-10) and LOSS (DLQ never re-driven) [C]
│   │   └── Detection: DLQ depth and oldest age; DLQ must be zero or owned
│   ├── F6.3 Dependency outage triggers mass DLQ — LOSS in practice [C]
│   │   └── Handling: circuit breaker → pause consumption instead of failing each event
│   ├── F6.4 Poison event blocks unrelated keys on same partition — DEGRADE [C]
│   │   └── Decision: block partition (simple, safe) vs per-key parking store (complex, isolates)
│   ├── F6.5 Unknown event type / schema version — OUTAGE or silent skip [C]
│   │   └── Decision per handler, documented: tolerant (skip + record) vs strict (park)
│   └── F6.6 Retry storms against recovering dependency — OUTAGE downstream [L]
│       └── Handling: jittered backoff; concurrency caps; rate limits on resume
│
├── 7. DEDUPE STORE LIFECYCLE
│   ├── F7.1 Retention set to topic retention + buffer — duplicates [C]
│   │   ├── Impact: ignores outbox re-publish, reconciliation, DLQ re-drive (IC-09)
│   │   └── Handling: window = max of all redelivery sources; or make version guard primary
│   │
│   ├── F7.2 Pruned by processing time — duplicates [C]
│   │   └── Handling: prune by producer event time with window per IC-09
│   │
│   ├── F7.3 Partitioning breaks uniqueness (F3.1.4) — duplicates [C]
│   │
│   ├── F7.4 Shared dedupe table across handlers — LOSS [C]
│   │   ├── Impact: handler A records event; handler B sees "duplicate" and skips its own effect
│   │   └── Handling: IC-08 (handler identity in key)
│   │
│   ├── F7.5 Purge job deletes while consumers are behind — duplicates [C]
│   │   └── Handling: purge horizon = min(window, oldest unprocessed event time across partitions)
│   │
│   └── F7.6 Growth and bloat at high volume — DEGRADE [L]
│       └── Handling: time-partitioned by event time with whole-partition drops; capacity alerts
│
├── 8. REPLAY & PROJECTION REBUILD
│   ├── 8.0 Normal rebuild flow
│   │   ├── Shadow projection + shadow inbox, empty; separate consumer group
│   │   ├── Source: snapshot of producer state at a known position + replay from that position
│   │   ├── Shadow catches up to live head; parity checks vs live projection
│   │   └── Atomic cutover (switch reads), then retire old projection after observation window
│   │
│   ├── F8.1 Replay from topic start with finite retention — CORRUPT (incomplete projection) [C]
│   │   └── Handling: snapshot + replay; or durable full-history source (event store / archive)
│   │
│   ├── F8.2 Shadow shares inbox with live — LOSS for shadow [C]
│   │   └── Handling: separate inbox per projection generation (IC-08 applied to generations)
│   │
│   ├── F8.3 Replay triggers external side effects — CORRUPT, financial [C]
│   │   ├── Impact: rebuild re-sends emails / re-charges cards
│   │   └── Handling: replay mode disables all Pattern A/B/C side effects; effects belong to live only
│   │
│   ├── F8.4 Handler logic changed between original run and replay — CORRUPT [C]
│   │   └── Handling: decide explicitly: replay with current logic (new projection) vs versioned handlers
│   │
│   ├── F8.5 Cutover gap — LOSS or duplicates [C]
│   │   └── Handling: shadow keeps consuming until after cutover; parity check at cutover moment
│   │
│   └── F8.6 Replay load harms live traffic — DEGRADE [C]
│       └── Handling: throttled replay; separate resources; off-peak
│
├── 9. CONSUMER-SIDE DURABILITY & FAILOVER
│   ├── F9.1 Sink DB failover with async replication — LOSS [C]
│   │   ├── Impact: committed effect + inbox lost; Kafka offset already advanced → never re-read
│   │   ├── Handling: sink-stored offsets (F5.3) so offsets rewind with the data; or synchronous replication
│   │   └── Detection: post-failover reconciliation
│   │
│   ├── F9.2 Sink restore from backup — LOSS (if offsets in Kafka) [C]
│   │   └── Handling: same as F9.1; restore runbook seeks to stored offsets
│   │
│   └── F9.3 Split brain on sink — CORRUPT [C]
│       └── Handling: fencing at sink HA layer; version/hash mismatch detection (F3.2.4)
│
├── 10. OBSERVABILITY
│   ├── Per handler: processed rate, duplicate rate, stale-skip rate, gap detections (must be 0)
│   ├── Held/parked aggregates count and oldest age (must be 0 or owned)
│   ├── Hash-mismatch count (must be 0)
│   ├── Consumer lag in TIME vs topic retention and vs dedupe window
│   ├── Rebalance rate; repeated-offset processing (S5 loops)
│   ├── DLQ / retry topic depth and oldest age
│   ├── External effect outcomes: success / unknown / failed; unknown must be worked to zero
│   ├── Inbox table size, insert latency, dead tuples
│   └── Absence alerts on every metric above (IC-13)
│
├── 11. CORRECTNESS PROOF
│   ├── 11.1 End-to-end canary
│   │   └── Producer heartbeat events consumed by every critical handler; staleness + latency alerts
│   └── 11.2 Reconciliation
│       ├── Producer aggregate versions vs consumer projection versions per window
│       ├── Gaps → producer re-publish (same event_id); excess → investigate ghost / business duplicate
│       ├── Financial handlers: reconcile amounts and external provider records, not just counts
│       └── Unknown-outcome external effects reconciled against provider statements daily
│
├── 12. LIFECYCLE & CHANGE MANAGEMENT
│   ├── L1 New handler on existing topic: decide start position explicitly (history vs now); own inbox
│   ├── L2 Handler logic change: backward compatible with in-flight and parked events
│   ├── L3 Consumer group rename/migration: stop old → record positions → start new at those positions
│   ├── L4 Schema evolution: tolerant reader; unknown fields ignored; new types handled per F6.5
│   ├── L5 Partition count change upstream: ordering broken during transition → version guard absorbs
│   └── L6 Decommission: stop group → archive inbox → delete offsets → remove alerts last
│
├── 13. RECOVERY RUNBOOKS
│   ├── R1 Gap detected → hold key → request producer re-publish of missing versions → auto-resume
│   ├── R2 Parked aggregate → diagnose → fix data or code → unpark → drain in version order
│   ├── R3 DLQ re-drive → through version guard → stale events reported, not forced
│   ├── R4 Sink restored / failed over → seek to sink-stored offsets → reconcile
│   ├── R5 Unknown external outcome → query provider with derived reference → resolve or escalate
│   └── R6 Projection corrupt → shadow rebuild (8.0) → parity → cutover
│
└── 14. GO-LIVE GATE  (each ends with reconciliation: zero gaps, zero double-effects)
    ├── Kill consumer after effect, before offset commit → no double effect
    ├── Force rebalance mid-batch → no double effect, no loss
    ├── Pause a consumer past poll interval (zombie) → stale writes rejected by version guard
    ├── Re-publish a week-old event with original event_id → absorbed
    ├── Deliver v6 before v5 → v6 held, v5 applied, v6 applied
    ├── Deliver same version with different content → parked + alert
    ├── Fail sink DB over / restore from backup → no lost effects
    ├── Kill Redis (if used) / evict all keys → no loss, sink still idempotent
    ├── External provider timeout with unknown outcome → no double charge
    ├── Run a full shadow rebuild → zero side effects fired, parity achieved
    └── Stop metrics → absence alerts fire
```

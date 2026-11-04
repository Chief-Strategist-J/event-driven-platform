# Event Ordering Protection — Business Logic Decision Tree

```
EVENT ORDERING PROTECTION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-AGG  Producer aggregate store — owns aggregate row + version (sequence) column
│   │   ├── C-OBX  Producer outbox — event bytes + aggregate version, written in the business tx
│   │   ├── C-RLY  Relay (CDC or poller) — preserves commit order per key
│   │   ├── C-KAF  Kafka — per-partition total order; key = aggregate_id
│   │   ├── C-RT   Consumer runtime — partition owner, sequence validator, held-key manager
│   │   ├── C-QS   Consumer quarantine/held store (from DLQ tree) — durable held events, offsets, epochs
│   │   ├── C-REM  Producer re-emit API — re-publishes specific versions with original event_ids
│   │   ├── C-SNP  Producer snapshot API — authoritative aggregate state at a version (for resync)
│   │   └── C-MON  Monitoring + reconciliation — gap rate, held keys, producer↔consumer version parity
│   │
│   ├── 0.2 Ordering guarantee chain (break any link → ordering is not guaranteed)
│   │   ├── L1 Producer assigns version N+1 only after N committed for that aggregate
│   │   ├── L2 Commit order per aggregate = version order (row lock)
│   │   ├── L3 Relay emits per key in commit order
│   │   ├── L4 Kafka stores per partition in send order (idempotent producer, same key → same partition)
│   │   ├── L5 Consumer processes per key in partition order (no reordering parallelism)
│   │   └── L6 Consumer applies only exact-next version (sequence validator)
│   │
│   └── 0.3 Ownership
│       ├── Producer team: L1–L3, re-emit API, snapshot API, sequence integrity
│       ├── Platform: L4, relay config, held store library, monitoring
│       └── Consumer team: L5–L6, bootstrap policy, cross-aggregate handling
│
├── 1. INVARIANTS  (with enforcement)
│   ├── OR-01 Gapless per-aggregate sequence at source [C]
│   │   └── Enforced by: conditional update on aggregate row (expected version) in business tx;
│   │                   no DB sequence objects; uniqueness on (aggregate, version) in outbox/audit
│   ├── OR-02 Commit order = version order per aggregate [C]
│   │   └── Enforced by: row lock on aggregate during mutation; one tx per version
│   ├── OR-03 Single ordered stream per aggregate [C]
│   │   └── Enforced by: one topic per aggregate stream; key = aggregate_id; fixed partition count
│   ├── OR-04 Version type cannot overflow [C]
│   │   └── Enforced by: 64-bit integer end to end (DB, schema, consumer store)
│   ├── OR-05 Exact-next application [C]
│   │   └── Enforced by: consumer applies only last_applied + 1; everything else classified
│   ├── OR-06 Same version, same content [C]
│   │   └── Enforced by: content hash stored with last applied version; mismatch = integrity incident
│   ├── OR-07 Held events are durable [C]
│   │   └── Enforced by: held store write + offset advance in one consumer DB transaction; no memory-only buffers
│   ├── OR-08 Hold the key, not the partition [C]
│   │   └── Enforced by: held-key mechanism (shared with DLQ parking); partition halt only on systemic trigger
│   ├── OR-09 Gap resolution never fabricates [C]
│   │   └── Enforced by: only original events (re-emit) or versioned snapshots (resync) can close a gap
│   ├── OR-10 Single writer per key at consumer [C]
│   │   └── Enforced by: partition ownership + epoch fencing on every write
│   ├── OR-11 Bootstrap is explicit [C]
│   │   └── Enforced by: first-seen version rule (§7.4); snapshot required unless version = 1
│   ├── OR-12 Ordering is per aggregate only [C]
│   │   └── Enforced by: consumers designed for cross-aggregate reordering (§11); no global order assumed
│   └── OR-13 Gap rate is an SLO with target zero [C]
│       └── Enforced by: page on any confirmed gap for T0/T1; reconciliation proves parity
│
├── 2. ORDERING BREAK MAP  (every hop where a gap or reorder can be created)
│   ├── H1 Producer sequence assignment → duplicate versions, skipped versions (§3)
│   ├── H2 Producer commit → commit order ≠ version order without row lock (§3)
│   ├── H3 Outbox → events filtered/never written for some types (§3.4)
│   ├── H4 Relay → multi-worker reorder, crash replay, slot loss, filtered SMT, wrong routing (§4)
│   ├── H5 Kafka → key serialization mismatch, partition change, multiple topics, unclean election (§5)
│   ├── H6 Consumer ingestion → parallel processing, retry topics, rebalance, zombies, offset bugs (§6)
│   ├── H7 Consumer validation → wrong rule (≥ vs =), silent ignore, missing bootstrap (§7)
│   └── H8 Topology change → stream migration, mirroring, DR failover, restores (§12, §14)
│
├── 3. PRODUCER-SIDE SEQUENCE ASSIGNMENT  (H1–H3)
│   ├── 3.1 Normal flow
│   │   ├── Conditional update of aggregate row: set version = expected + 1 where version = expected
│   │   ├── 0 rows → concurrent modification → abort and retry command (never reuse the version)
│   │   ├── Event built with returned version, written to outbox in same tx
│   │   └── Commit (row lock held until commit → next version cannot commit first)
│   │
│   ├── F3.1 Read-then-assign race — CORRUPT [C]
│   │   ├── Impact: two events with same version, different content
│   │   ├── Handling: conditional update (3.1); uniqueness on (aggregate, version) in outbox/audit
│   │   └── Detection: unique violation counter; consumer OR-06 hash mismatch
│   │
│   ├── F3.2 Database sequence object used for versions — false gaps [C]
│   │   ├── Impact: sequences are non-transactional; rollbacks and cache loss burn numbers → permanent "gaps"
│   │   ├── Handling: version stored on aggregate row only
│   │   └── Detection: gaps that re-emit API cannot fill (never existed)
│   │
│   ├── F3.3 Version incremented by code paths that don't emit events — false gaps [C]
│   │   ├── Scenario: admin scripts, migrations, internal updates bump version without outbox row
│   │   ├── Handling: version bump and outbox insert in one shared function; DB check that every
│   │   │             version has an outbox/audit record (reconciliation)
│   │   └── Detection: producer self-reconciliation: versions without events
│   │
│   ├── F3.4 Events emitted to different streams with one shared version counter — gaps by design [C]
│   │   ├── Scenario: internal-only events not published; some types to topic A, others to topic B;
│   │   │             consumer subscribed to subset sees gaps
│   │   ├── Handling (choose one, document per stream):
│   │   │   ├── Every version published on the aggregate stream (preferred; internal events marked)
│   │   │   ├── Separate per-stream sequence for each published stream
│   │   │   └── Event carries previous_version pointer for that stream (consumer validates chain)
│   │   └── Detection: persistent gaps at consumers of filtered streams
│   │
│   ├── F3.5 Multiple writers for one aggregate (active-active, two services, split brain) — CORRUPT [C]
│   │   ├── Handling: single owner + single primary; fencing at DB HA layer
│   │   └── Detection: same version, different event_id / hash at consumers
│   │
│   ├── F3.6 Producer restore from backup — versions reissued with different content — CORRUPT [C]
│   │   ├── Impact: consumer already applied v100–v120; producer now re-creates v101+ differently
│   │   ├── Handling: restore = incident; producer must emit correction/resync signal; consumers resync per aggregate
│   │   └── Detection: OR-06 hash mismatch spike after restore
│   │
│   ├── F3.7 Bulk/batch commands touching many aggregates — lock contention, deadlocks [C]
│   │   └── Handling: deterministic lock ordering by aggregate id; bounded batch size
│   │
│   └── F3.8 Version overflow (32-bit) on long-lived hot aggregates — CORRUPT [C]
│       └── Handling: OR-04
│
├── 4. RELAY ORDERING  (H4)
│   ├── 4.1 CDC relay: emits in WAL commit order (single stream) → per-key order preserved [C]
│   ├── 4.2 Poller relay: per-key order only with single worker or hash(aggregate_id) sharding [C]
│   │
│   ├── F4.1 Multi-worker poller — reorder [C]
│   │   ├── Impact: worker B sends v6 while worker A still sending v5
│   │   └── Handling: shard by key; never send v(k+1) while v(k) unacked/parked (outbox tree §4)
│   │
│   ├── F4.2 Relay crash and replay — duplicates (not gaps) [C]
│   │   └── Handling: consumer OR-05 absorbs (stale/duplicate path)
│   │
│   ├── F4.3 Slot invalidation / relay skip — true gap [C]
│   │   ├── Handling: outbox tree R1 runbook; re-emit missing versions via C-REM
│   │   └── Detection: consumer gaps cluster by time window across many aggregates
│   │
│   ├── F4.4 Relay transform drops events (filter, errors tolerated) — true gap [C]
│   │   └── Handling: zero error tolerance; no filtering transforms on aggregate streams
│   │
│   ├── F4.5 Routing by event type splits aggregate across topics — reorder [C]
│   │   └── Handling: route by aggregate type only (outbox tree INV-03)
│   │
│   └── F4.6 Poller visibility gap (later-committed lower ID skipped by cursor) — true gap [C]
│       └── Handling: status-based selection, never "greater than last seen"
│
├── 5. KAFKA TRANSPORT ORDERING  (H5)
│   ├── 5.1 Guarantees relied on
│   │   ├── Per partition, records appended in order received from a producer session [C]
│   │   ├── Idempotent producer prevents duplicates and reordering from retries within a session [C]
│   │   └── With idempotence, up to 5 in-flight requests per connection keep order [C]
│   │
│   ├── 5.2 Producer error handling
│   │   ├── Retriable errors: client retries internally, order preserved within session [C]
│   │   ├── Sequence/epoch errors (out-of-order sequence, fenced/unknown producer) [L]
│   │   │   ├── Treat producer instance as poisoned → close and recreate
│   │   │   ├── Do NOT mark any in-flight records as published
│   │   │   └── Relay resumes from earliest unacknowledged record per key, in order
│   │   └── Delivery timeout: same as above; duplicates possible on resend (consumer absorbs)
│   │
│   ├── F5.1 Producer recreated mid-stream — duplicates across sessions [C]
│   │   └── Handling: expected; consumer duplicate path
│   │
│   ├── F5.2 Records after a failed one already written (pre-idempotence clients / idempotence disabled) — reorder [C]
│   │   └── Handling: idempotence mandatory; config audit
│   │
│   ├── F5.3 Key serialization differs between producers — same aggregate on two partitions — reorder [C]
│   │   └── Handling: one key serializer org-wide; partition-affinity test in CI
│   │
│   ├── F5.4 Partition count increased — keys remap mid-stream — reorder [C]
│   │   └── Handling: never resize ordered topics; new stream version (§12)
│   │
│   ├── F5.5 Custom partitioner differs between producers/relays — reorder [C]
│   │   └── Handling: default partitioner everywhere or one shared implementation
│   │
│   ├── F5.6 Unclean leader election / min ISR 1 — acked records lost — true gap [C]
│   │   └── Handling: outbox tree §5.0 durability config
│   │
│   ├── F5.7 Transactional producers + consumers reading uncommitted — ghost events [C]
│   │   └── Handling: read-committed isolation on consumers of transactional producers
│   │
│   └── F5.8 Compacted topic used as event stream — gaps by design [C]
│       └── Handling: delete policy for event streams; compacted topics only for state snapshots
│
├── 6. CONSUMER INGESTION ORDERING  (H6)
│   ├── 6.1 Processing model
│   │   ├── Partition owner reads in offset order
│   │   ├── Parallelism only ACROSS keys (key-hashed worker lanes); same key → same lane, FIFO
│   │   └── Stored offset advances only to the lowest offset not yet durably handled across all lanes
│   │
│   ├── F6.1 Unordered parallel processing within partition — reorder [C]
│   │   └── Handling: 6.1 key lanes; per-lane FIFO
│   │
│   ├── F6.2 Offset committed past in-memory-only work — LOSS [C]
│   │   ├── Impact: crash loses buffered/in-flight records whose offsets were already committed
│   │   └── Handling: OR-07; offsets stored in consumer DB with effects / held writes
│   │
│   ├── F6.3 Non-blocking retry topics — reorder [C]
│   │   └── Handling: failing key held (DLQ parking); later versions of that key never bypass
│   │
│   ├── F6.4 Rebalance — new owner without held-key state — reorder [C]
│   │   └── Handling: load held keys from durable store before processing (DLQ tree §5.1)
│   │
│   ├── F6.5 Zombie owner writes after revocation — reorder/CORRUPT [C]
│   │   └── Handling: epoch fencing in every consumer transaction
│   │
│   ├── F6.6 Consumer subscribes to multiple topics carrying one aggregate — reorder [C]
│   │   ├── Scenario: legacy per-event-type topics, migration dual streams
│   │   ├── Handling: consolidate streams; during migration use version-based cutover (versioning tree §11)
│   │   └── Detection: legit transient gaps — the ONLY case where short waiting is meaningful (§8.2)
│   │
│   ├── F6.7 Manual offset reset / seek skips records — true gap at consumer [C]
│   │   └── Handling: seeks only via runbook; post-seek reconciliation; prefer resync over seek
│   │
│   └── F6.8 Offsets expire, reset to latest — true gap at consumer [C]
│       └── Handling: "fail on missing offset"; offsets in consumer DB
│
├── 7. SEQUENCE VALIDATION  (H7 — per record, in the consumer transaction)
│   ├── 7.1 Inputs: aggregate_id, incoming_version, event_id, content hash, last_applied_version, last_hash
│   │
│   ├── 7.2 Decision
│   │   ├── Key currently HELD → append to held store (durable) → advance offset → done
│   │   ├── No consumer state for aggregate → §7.4 bootstrap rule
│   │   ├── incoming = last + 1 → APPLY (effect + last_applied + hash + offset in one tx)
│   │   │   └── After commit: if held events exist for key with next version → drain (§9.3)
│   │   ├── incoming ≤ last
│   │   │   ├── incoming = last AND hash equal → DUPLICATE → skip, count
│   │   │   ├── incoming < last → STALE → skip, count (hash compare optional per tier; T0: stored history hash)
│   │   │   └── incoming = last AND hash different → INTEGRITY → hold key + quarantine + incident
│   │   └── incoming > last + 1 → GAP → hold key (durable) → classify (§8)
│   │
│   ├── 7.3 Hash policy [L]
│   │   ├── Hash over canonical (upcasted) payload, excluding transport metadata
│   │   ├── T0: retain hashes for last K versions per aggregate to validate stale duplicates too
│   │   └── T2: last-version hash only
│   │
│   ├── 7.4 Bootstrap rule (first event seen for an aggregate)
│   │   ├── incoming = 1 → APPLY
│   │   ├── incoming > 1 and consumer started from beginning of full history → GAP (true loss)
│   │   ├── incoming > 1 and history truncated (retention, new consumer, rebuild) →
│   │   │   ├── Resync from C-SNP at version ≥ incoming − 1, then apply
│   │   │   └── Or bulk bootstrap: load snapshot of all aggregates at known position before consuming
│   │   └── Policy declared per consumer: FULL_HISTORY or SNAPSHOT_BOOTSTRAP
│   │
│   ├── F7.1 "Greater than or equal" or "strictly greater" accepted for DELTA streams — CORRUPT [C]
│   │   └── Handling: exact-next only (idempotent tree IC-05/IC-06)
│   │
│   ├── F7.2 Silent ignore of same-version different-content — CORRUPT hidden [C]
│   │   └── Handling: 7.2 INTEGRITY branch
│   │
│   ├── F7.3 New consumer on retention-limited topic treats every aggregate as gap — OUTAGE storm [C]
│   │   └── Handling: 7.4 SNAPSHOT_BOOTSTRAP
│   │
│   ├── F7.4 Aggregate deleted then id reused — CORRUPT [C]
│   │   ├── Impact: new aggregate's version 1 looks stale vs old aggregate's last version
│   │   ├── Handling: ids never reused (UUIDs); or deletion emits terminal event and consumer records generation
│   │   └── Detection: version-1 events for aggregates with existing state
│   │
│   └── F7.5 Terminal/tombstone event followed by later versions — CORRUPT [C]
│       └── Handling: terminal state recorded; later versions → INTEGRITY (producer bug)
│
├── 8. GAP CLASSIFICATION & RESOLUTION LADDER
│   ├── 8.1 Classification (decides waiting vs acting)
│   │   ├── G-A TRANSIENT-POSSIBLE: missing version may arrive via another path
│   │   │   ├── Consumer reads multiple streams for this aggregate (F6.6)
│   │   │   ├── Relay replay / re-emit already in progress for this window
│   │   │   └── Missing version is currently parked in this consumer's own DLQ (resolves via drain)
│   │   ├── G-B DEFECT: single stream, correct key, no other path → missing version will NOT arrive by itself
│   │   │   └── This is the DEFAULT for a correctly built system
│   │   └── G-C SYSTEMIC: gaps across many aggregates in a short window
│   │       └── Trigger [L]: > 20 aggregates with new gaps in 5 min, or gaps clustered in one time window
│   │
│   ├── 8.2 G-A handling: bounded wait (durable, not memory)
│   │   ├── Hold key durably; wait deadline [L: 30–60 s for dual-stream; until drain for self-parked]
│   │   ├── Missing arrives → apply → drain held
│   │   └── Deadline passes → reclassify as G-B
│   │
│   ├── 8.3 G-B handling: request re-emit
│   │   ├── Call C-REM for versions (last+1 .. incoming−1) — idempotent request keyed by (consumer, aggregate, range)
│   │   ├── Producer re-publishes original events (original event_ids, original bytes) to the stream
│   │   ├── Re-emitted events arrive AFTER held events in partition → consumer applies them, then drains held
│   │   ├── Re-emit returns "version does not exist" → producer-side defect (F3.2/F3.3) → §8.4
│   │   └── No response / producer down → retry with backoff; escalate at deadline [L: 15 min T0]
│   │
│   ├── 8.4 Producer cannot supply missing version → RESYNC
│   │   ├── Fetch snapshot from C-SNP at version V ≥ incoming − 1
│   │   ├── In one tx: replace projection, set last_applied = V, new hash, mark held ≤ V superseded
│   │   ├── Drain held > V
│   │   ├── Side effects for skipped versions → compensation review (DLQ tree §10.3)
│   │   └── Incident record: which versions were never applied and why
│   │
│   ├── 8.5 G-C handling: systemic
│   │   ├── Halt consumer group (DLQ tree F4.4) — prevents thousands of held keys and re-emit storms
│   │   ├── Identify upstream cause (relay slot loss, routing change, partition change, restore)
│   │   └── Fix upstream → bulk re-emit for window or bulk resync → resume
│   │
│   ├── F8.1 In-memory wait-and-see buffer — LOSS on crash [C]
│   │   └── Handling: OR-07
│   │
│   ├── F8.2 Waiting on G-B gaps "for N+1 to arrive on partition" — wasted latency + false hope [C]
│   │   └── Handling: classification 8.1 — default straight to re-emit
│   │
│   ├── F8.3 Consumer queries producer DB/outbox directly — false "not found" + coupling [C]
│   │   ├── Impact: CDC insert+delete outbox has no rows; replica lag; schema coupling; credentials sprawl
│   │   └── Handling: only C-REM / C-SNP APIs
│   │
│   ├── F8.4 Partition rewind as gap fix — duplicates, no recovery [C]
│   │   ├── Impact: missing event was never on the partition; rewind replays everything else
│   │   └── Handling: forbidden as gap remedy; re-emit or resync instead
│   │
│   ├── F8.5 Re-emit storm (many consumers × many aggregates) — OUTAGE producer [C]
│   │   ├── Handling: re-emit requests deduplicated by (aggregate, range) across consumers;
│   │   │             rate-limited; batched by time window for systemic cases
│   │   └── Detection: re-emit request rate; producer re-emit queue depth
│   │
│   ├── F8.6 Re-emitted events reach ALL consumer groups — duplicates elsewhere [C]
│   │   └── Handling: acceptable (idempotent consumers absorb); or targeted re-emit topic per consumer for T2 bulk
│   │
│   ├── F8.7 Snapshot not consistent with stream version — CORRUPT [C]
│   │   └── Handling: snapshots must carry exact version; resync refuses unversioned snapshots
│   │
│   └── F8.8 Missing event arrives late after resync — stale [C]
│       └── Handling: ≤ last → stale path; hash check against snapshot lineage where available
│
├── 9. HELD-KEY MECHANISM  (unified with DLQ parking — one mechanism, one store)
│   ├── 9.1 Hold reasons: GAP | PARKED_FAILURE | INTEGRITY | RESYNCING
│   ├── 9.2 Held store per (group, handler, aggregate): reason, blocking version, since, held count, epoch
│   ├── 9.3 Drain protocol (executed by partition owner)
│   │   ├── Lock held key row; verify epoch
│   │   ├── Repeatedly take lowest held version; if = last + 1 → apply in tx; else stop (still gap)
│   │   ├── Unpark only in the tx that verifies no held events remain
│   │   └── New arrivals during drain append to held store (never bypass)
│   │
│   ├── F9.1 Hot aggregate held — held store growth [C]
│   │   ├── Handling: per-key cap [G: 10,000 events / 100 MB] → T0 escalation; hard cap → pause partition
│   │   └── Detection: held count per key; top-N held keys
│   │
│   ├── F9.2 Many keys held (systemic not detected) — DEGRADE [C]
│   │   └── Handling: 8.5 trigger; per-tenant held-key budgets
│   │
│   ├── F9.3 Held key lost on rebalance — reorder [C]
│   │   └── Handling: load held keys on assignment before processing
│   │
│   ├── F9.4 Two reasons overlap (GAP while PARKED_FAILURE) — wrong resolution order [C]
│   │   └── Handling: reasons stacked; drain only when all reasons cleared; resolution order documented
│   │
│   └── F9.5 Held age exceeds dedupe / provider idempotency windows — CORRUPT later [C]
│       └── Handling: escalation at 50% of smallest window (DLQ tree DL-09)
│
├── 10. PRODUCER RE-EMIT & SNAPSHOT APIs  (required for any T0/T1 stream)
│   ├── 10.1 Re-emit API contract
│   │   ├── Input: aggregate_id, version range, requesting consumer identity
│   │   ├── Behavior: re-publishes ORIGINAL events (same event_id, bytes, schema id) via outbox
│   │   ├── Source of truth: producer event store/audit (outbox rows are not retained under CDC delete pattern)
│   │   ├── Idempotent: repeated requests for same range coalesce
│   │   ├── Authorization: only registered consumers of that stream
│   │   └── Response: accepted | version does not exist | aggregate unknown | rate limited
│   │
│   ├── 10.2 Snapshot API contract
│   │   ├── Output: full aggregate state + exact version + schema id + content hash
│   │   ├── Consistency: read in one tx with version
│   │   └── Authorization and PII masking per consumer entitlement
│   │
│   ├── F10.1 Producer keeps no retained event history — re-emit impossible [C]
│   │   └── Handling: retain audit/event store for at least the readability horizon (versioning tree §14)
│   │
│   ├── F10.2 Re-emitted event re-serialized with current schema — CORRUPT identity [C]
│   │   └── Handling: original bytes + schema id re-published (upcasting is consumer's job)
│   │
│   └── F10.3 Snapshot API exposes more data than consumer is entitled to — COMPLIANCE [C]
│       └── Handling: per-consumer projections of snapshot; audit access
│
├── 11. CROSS-AGGREGATE & CAUSAL ORDERING  (no global order exists)
│   ├── 11.1 Facts: different aggregates → different partitions → no relative order [C]
│   ├── 11.2 Patterns
│   │   ├── Tolerate: placeholder/pending state for references to unseen aggregates; resolve on arrival
│   │   ├── Causal metadata: event carries reference (aggregate, min version) it depends on;
│   │   │   consumer holds until dependency version applied (bounded, durable)
│   │   └── Co-locate: if strict order needed between entities, they belong to one aggregate (re-model)
│   │
│   ├── F11.1 Payment applied before order exists — CORRUPT / failed lookups [C]
│   │   └── Handling: causal dependency hold or placeholder; never reject as poison
│   │
│   ├── F11.2 Causal hold waits forever (dependency never arrives) — OUTAGE for key [C]
│   │   └── Handling: deadline → reclassify dependency as gap on the OTHER stream → §8
│   │
│   ├── F11.3 Timestamps used to order across aggregates — CORRUPT [C]
│   │   └── Handling: clocks are not ordering; only versions and explicit causal references
│   │
│   └── F11.4 Saga steps assumed ordered across services — CORRUPT [C]
│       └── Handling: saga state machine tolerates out-of-order step events; correlation id + step version
│
├── 12. TOPOLOGY CHANGES  (H8)
│   ├── F12.1 Stream migration v1 → v2 — reorder/gap during dual consumption [C]
│   │   └── Handling: version continuity + per-aggregate cutover (versioning tree §11.3)
│   ├── F12.2 Partition increase requested for throughput — reorder [C]
│   │   └── Handling: new stream version with planned cutover; never in place
│   ├── F12.3 Cluster migration / mirroring — reorder across mirrored partitions [L]
│   │   ├── Handling: mirror preserves partition mapping and key; validate with per-aggregate parity
│   │   └── Detection: gap/stale spike on mirrored consumers
│   └── F12.4 Producer service split (aggregate ownership moves) — dual writers during transition [C]
│       └── Handling: ownership handover at a fenced version; old owner stops before new starts
│
├── 13. HOT KEYS & MULTI-TENANCY
│   ├── F13.1 Hot aggregate saturates one partition lane — DEGRADE [C]
│   │   └── Handling: capacity sized for hottest key; aggregate re-model if sustained
│   ├── F13.2 One tenant's gap storm consumes re-emit capacity — DEGRADE others [C]
│   │   └── Handling: per-tenant re-emit quotas; tenant-level halt
│   └── F13.3 Held keys concentrated on one partition — partition lag grows [L]
│       └── Handling: held keys don't block offsets (durable hold); monitor per-partition held counts
│
├── 14. FAILOVER, RESTORE & DR
│   ├── F14.1 Producer DB async failover — published versions lost at source → reissued differently — CORRUPT [C]
│   │   └── Handling: outbox tree F7.1 (no ghost events); consumers detect via OR-06; resync per aggregate
│   ├── F14.2 Consumer DB failover/restore — last_applied rewinds with offsets (co-located) [C]
│   │   └── Handling: re-reads replay → exact-next rule re-applies correctly; external effects need idempotency keys
│   ├── F14.3 Consumer DB restored WITHOUT co-located offsets — gaps at consumer [C]
│   │   └── Handling: offsets must be in consumer DB (OR-07)
│   └── F14.4 Region failover with Kafka and DBs at different points in time — gaps + stale [L]
│       └── Handling: post-failover reconciliation (§16) mandatory before declaring recovery complete
│
├── 15. OBSERVABILITY
│   ├── Per handler: applied, duplicate, stale, integrity (must be 0), gap (target 0) rates
│   ├── Held keys by reason; oldest held age; held events per key; top-N held keys
│   ├── Gap classification counts (G-A / G-B / G-C); time-to-resolve distribution
│   ├── Re-emit requests: sent, coalesced, fulfilled, "does not exist" (must be 0), latency
│   ├── Resync count and reasons
│   ├── Bootstrap events (snapshot bootstraps) per consumer
│   ├── Epoch-fenced aborts; rebalance count
│   ├── Producer: conditional-update conflicts, unique violations on (aggregate, version) (must be 0)
│   └── Absence alerts on all of the above
│
├── 16. RECONCILIATION  (proof, not hope)
│   ├── RC-01 Hourly: producer current version per aggregate vs consumer last_applied
│   │   └── Every lag must be explained by a held key or active processing window
│   ├── RC-02 Hourly: held keys whose missing version exists at producer → auto re-emit
│   ├── RC-03 Daily: hash parity sample (T0: full) between producer state and consumer projection at same version
│   ├── RC-04 Producer self-check: every version 1..current has an event record (catches F3.2/F3.3)
│   └── RC-05 Post-incident: full parity before closing any gap/integrity incident
│
├── 17. RUNBOOKS
│   ├── RB-01 Single G-B gap → re-emit → verify drain → close
│   ├── RB-02 Re-emit says "does not exist" → producer defect investigation → resync consumer → fix producer path
│   ├── RB-03 Systemic gaps (G-C) → halt group → locate hop in break map (§2) → fix → bulk re-emit/resync → resume
│   ├── RB-04 Integrity mismatch → hold key → compare producer history → producer correction or resync → incident review
│   ├── RB-05 New consumer onboarding on truncated topic → snapshot bootstrap → start at recorded position
│   ├── RB-06 Producer restore → broadcast resync signal for affected aggregates → consumers resync → reconcile
│   └── RB-07 Hot key held at cap → partition pause decision → producer engagement
│
└── 18. GO-LIVE TEST MATRIX
    ├── Concurrent commands on one aggregate → no duplicate versions; one retries
    ├── Rolled-back transaction → no version consumed (no false gap)
    ├── Relay multi-worker / crash replay → consumer sees duplicates only, never reorder
    ├── Drop one event in relay (fault injection) → consumer holds key, re-emits, drains, converges
    ├── Re-emit "does not exist" → resync path completes, incident recorded
    ├── Same version different content injected → integrity hold + page
    ├── Crash while key held → held events survive; drain completes after restart
    ├── Rebalance while keys held → new owner respects holds
    ├── Zombie writer after revocation → epoch abort
    ├── New consumer on 7-day retention topic → snapshot bootstrap, zero false gaps
    ├── 50 aggregates gap within 5 min → systemic halt, no re-emit storm
    ├── Cross-aggregate: payment before order → placeholder/causal hold, converges
    ├── Partition count change attempt → blocked by policy
    ├── Producer DB failover with async replica → hash mismatches detected, resync converges
    ├── Consumer DB restore → offsets rewind together, no gaps, no double external effects
    └── Metrics pipeline down → absence alerts fire
```

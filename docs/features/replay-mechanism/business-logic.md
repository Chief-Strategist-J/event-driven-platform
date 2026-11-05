# Replay Mechanism — Business Logic Decision Tree

```
REPLAY MECHANISM — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-ORC  Replay orchestrator — run manifests, approvals, lifecycle state machine, scheduling
│   │   ├── C-SRC  Replay sources (unified reader with per-aggregate ordering guarantee)
│   │   │   ├── S1 Kafka live retention
│   │   │   ├── S2 Kafka tiered storage (if enabled)
│   │   │   ├── S3 Archive (raw envelopes: bytes + schema id + original offsets, partition-ordered)
│   │   │   ├── S4 Producer event store / re-emit API (per aggregate)
│   │   │   └── S5 Producer snapshot API (state at exact version)
│   │   ├── C-RUN  Replay runner — the SAME consumer runtime in a declared run mode (no fork of handler code)
│   │   ├── C-EFG  Effect gateway — the only path for outbound calls, outbox writes, notifications, cache/search writes
│   │   ├── C-TGT  Target stores — shadow tables/indexes; namespaced caches; isolated outbox
│   │   ├── C-PAR  Parity service — compares target vs source of truth at matching aggregate versions
│   │   ├── C-CUT  Cutover controller — read pointer switch, write ownership handoff, rollback window
│   │   └── C-MON  Monitoring — progress, ETA, load impact, parity, effect gateway decisions
│   │
│   ├── 0.2 Run modes (declared in manifest; enforced by C-EFG and C-RUN)
│   │   ├── REBUILD     — new projection from history; all external effects suppressed
│   │   ├── HYDRATE     — new read model introduction; same as REBUILD
│   │   ├── FORENSIC    — point-in-time reconstruction; read-only target; pinned code/config/reference data
│   │   ├── RECOVER     — disaster recovery; effects in RECONCILE mode (§7.3)
│   │   ├── REPAIR      — targeted aggregate set re-projection into live store (rare; strict guards)
│   │   └── LIVE        — normal operation (the only mode where effects execute without restriction)
│   │
│   └── 0.3 Ownership
│       ├── Platform: orchestrator, sources, effect gateway, cutover controller, archive
│       ├── Consumer team: handler determinism, effect declarations, parity rules, cutover readiness
│       ├── Producer team: event store retention, re-emit/snapshot APIs, archive completeness
│       └── Business / compliance: approvals for T0, FORENSIC and RECOVER runs; erasure obligations
│
├── 1. INVARIANTS  (with enforcement)
│   ├── RP-01 Replay never touches the production group or production store directly [C]
│   │   └── Enforced by: orchestrator refuses manifests targeting live group ids or live tables (except REPAIR)
│   ├── RP-02 Effects fail closed [C]
│   │   └── Enforced by: C-EFG denies every external effect unless run mode is LIVE or effect explicitly allowed
│   ├── RP-03 One effect path [C]
│   │   └── Enforced by: handlers have no direct clients for email/SMS/webhooks/payments/outbox/cache/search;
│   │                   static analysis + dependency rules in CI
│   ├── RP-04 Replay coverage is proven, not assumed [C]
│   │   └── Enforced by: source coverage check (every aggregate's version chain complete from start point)
│   ├── RP-05 Per-aggregate order and exact-next application hold during replay [C]
│   │   └── Enforced by: same version guard, held-key mechanism, gap handling as live
│   ├── RP-06 Deterministic handlers [C]
│   │   └── Enforced by: injected clock (event time), deterministic ids from event identity, no live enrichment
│   ├── RP-07 Write ownership hands off; it is never dropped [C]
│   │   └── Enforced by: cutover promotes replay run to live writer before old writer stops
│   ├── RP-08 Rollback stays cheap until decommission [C]
│   │   └── Enforced by: old projection kept LIVE (dual-run) through observation window
│   ├── RP-09 Parity is against source of truth, with expected differences declared [C]
│   │   └── Enforced by: C-PAR at matching aggregate versions; unexplained diff blocks cutover
│   ├── RP-10 Run manifest is immutable and complete [C]
│   │   └── Enforced by: manifest hash recorded; code build, upcaster revisions, config, reference data versions pinned
│   ├── RP-11 Replay honors erasure [C]
│   │   └── Enforced by: crypto-shredded payloads stay unreadable; handlers treat shredded fields as erased
│   └── RP-12 Replay cannot degrade live traffic beyond budget [C]
│       └── Enforced by: rate limits on Kafka reads and target writes; automatic throttle on live SLO breach
│
├── 2. INTENT CATALOGUE & DECISION
│   ├── Q1 Why replay?
│   │   ├── Projection bug fix (A)                 → REBUILD → shadow → parity w/ expected-diff → cutover
│   │   ├── New read model (B)                     → HYDRATE → shadow → parity vs source → cutover (first serve)
│   │   ├── Forensic / audit reconstruction (C)    → FORENSIC → isolated read-only target → report; no cutover
│   │   ├── Disaster recovery (D)                  → RECOVER → rebuild target → effects RECONCILE → resume LIVE
│   │   ├── Targeted correction (few aggregates)   → prefer resync per aggregate (ordering tree §8.4);
│   │   │                                            REPAIR only if resync impossible
│   │   └── "Something looks off"                  → not a replay; investigate first (reconciliation trees)
│   │
│   ├── Q2 Is full history available from sources? (§3 coverage) → if NO: snapshot bootstrap required
│   ├── Q3 Do handlers have per-version side effects? → yes: effect declarations reviewed for the mode
│   ├── Q4 Is the target the live store? → yes: REPAIR mode only, with approvals (RP-01 exception)
│   └── Q5 Tier T0? → second approver; business owner sign-off for RECOVER/REPAIR
│
├── 3. REPLAY SOURCES & COVERAGE
│   ├── 3.1 Source selection
│   │   ├── Within Kafka retention → S1 (cheapest)
│   │   ├── Beyond retention → S3 archive (or S2 tiered) for the old segment, then hand off to S1
│   │   ├── Per-aggregate needs → S4 event store / re-emit
│   │   └── Start mid-history → S5 snapshot at version V + stream events > V
│   │
│   ├── 3.2 Handoff archive → Kafka (critical)
│   │   ├── Handoff point = per-partition offset recorded in archive manifest
│   │   ├── Handoff offset MUST still be retained in Kafka when archive replay finishes
│   │   ├── Retention margin check: ETA(archive phase) < retention − age(handoff offset) − safety [L: 50%]
│   │   └── If margin insufficient → extend topic retention for duration of run, or stream archive faster
│   │
│   ├── 3.3 Archive requirements (for replays to be possible at all)
│   │   ├── Raw envelopes: key bytes, value bytes, headers, schema id, original topic/partition/offset, timestamp
│   │   ├── Partition-ordered layout (per-key order preserved within a partition file sequence)
│   │   ├── Continuous coverage: no offset ranges missing (holes only where transactions/compaction explain them)
│   │   └── Registry schemas exported with archive (versioning tree EV-07)
│   │
│   ├── 3.4 Coverage verification (RP-04) before run starts
│   │   ├── Per partition: archive offsets + Kafka offsets form a contiguous range from start point
│   │   ├── Per aggregate (sampled; T0 full): version chain 1..current has no gaps across sources
│   │   └── Result recorded in run; coverage failure blocks REBUILD/HYDRATE (unless snapshot bootstrap)
│   │
│   ├── F3.1 "Replay from offset 0" on retention-bounded topic — incomplete projection — CORRUPT [C]
│   │   └── Handling: coverage check; archive or snapshot bootstrap
│   │
│   ├── F3.2 Archive written by an at-least-once job — duplicates and missing ranges [C]
│   │   ├── Handling: archive keyed by (topic, partition, offset); completeness audits; idempotent writer
│   │   └── Detection: coverage check holes
│   │
│   ├── F3.3 Archive converted to columnar format (lost bytes/schema ids) — unreplayable [C]
│   │   └── Handling: raw envelopes are the replay source; columnar is analytics-only
│   │
│   ├── F3.4 Replay outruns retention at handoff — gap between archive end and Kafka start — LOSS [C]
│   │   └── Handling: 3.2 margin check; continuous re-check during run; pause and extend retention
│   │
│   ├── F3.5 Tiered storage fetch latency/cost spikes — slow replay, broker load [L]
│   │   └── Handling: rate limits; prefer archive for very old ranges
│   │
│   └── F3.6 Topic recreated in the past (new incarnation) — offsets don't connect [C]
│       └── Handling: source manifest tracks topic ids; replay stitches incarnations explicitly
│
├── 4. SCOPE SEMANTICS
│   ├── 4.1 Full history → correct when coverage proven (or snapshot bootstrap)
│   ├── 4.2 Time window
│   │   ├── Into NEW target: requires snapshot state at window start for every affected aggregate
│   │   ├── Into EXISTING target: only valid in REPAIR with explicit version reset for affected aggregates
│   │   └── Window boundaries by offsets (recorded), not timestamps (non-monotonic CreateTime) [L]
│   ├── 4.3 Aggregate set
│   │   ├── Source: S4 per aggregate (Kafka cannot filter by key without full partition scans)
│   │   ├── Preferred mechanism: resync from snapshot (ordering tree §8.4)
│   │   └── Kafka-based alternative: scan partitions owning those keys; filter in runner; cost ∝ partition size
│   ├── 4.4 Partition subset → only for parallelizing a full replay; never a semantic scope
│   │
│   ├── F4.1 Window replay without starting state — CORRUPT (partial aggregates) [C]
│   ├── F4.2 Window replay into live store blocked by version guard — silent no-op [C]
│   │   └── Detection: replay applied-count ≈ 0 with stale-skip ≈ total
│   └── F4.3 Timestamp-bounded window misses or includes edge events [L]
│       └── Handling: offsets per partition from timestamp lookup plus safety margin; idempotency absorbs overlap
│
├── 5. RUN MANIFEST  (immutable; RP-10)
│   ├── Identity: run id, mode, intent, requester, approvers, ticket
│   ├── Sources: per partition start/handoff/end positions, snapshot set (if any), coverage result
│   ├── Code: handler build id, upcaster revision set, schema registry snapshot id
│   ├── Config: feature flags, handler configuration, reference data versions (as-of time for FORENSIC)
│   ├── Target: shadow store identifiers, isolated outbox, cache/search namespaces
│   ├── Effects: per effect type → SUPPRESS | ALLOW | RECONCILE (mode defaults; overrides need approval)
│   ├── Rates: max read rate per partition, max write rate to target, live-SLO guard thresholds
│   ├── Stop condition: end offsets per partition or "follow live" (for promotion)
│   └── Parity rules: comparison fields, expected-diff rules, tolerances, sampling level
│
├── 6. ISOLATION
│   ├── 6.1 Consumer group: "<env>.replay.<target>.<run id>" — new group per run; ACL-scoped
│   ├── 6.2 Positions: stored in replay target's DB (same model as live; offset tree)
│   ├── 6.3 Target store: shadow tables / new index / separate schema; never shared with live writes
│   ├── 6.4 Quarantine/held store: per run (replay poison never pollutes live DLQ)
│   ├── 6.5 Consumer outbox: isolated or disabled per mode (§7)
│   ├── 6.6 Caches & search: namespaced per run; live caches never invalidated by replay
│   ├── 6.7 Metrics/analytics: replay-labeled; business counters (revenue, signups) never incremented by replay
│   ├── 6.8 Credentials: replay runner has no credentials for payment/notification providers (defense in depth)
│   │
│   ├── F6.1 Replay shares live consumer group id — production rebalance + position corruption [C]
│   ├── F6.2 Replay invalidates live cache keys — cache stampede on live traffic [C]
│   ├── F6.3 Replay increments business metrics — dashboards and alerts lie [C]
│   ├── F6.4 Replay poison enters live DLQ — triage confusion, wrong redrives [C]
│   └── F6.5 Replay writes to shared tables beyond the projection (audit log, user activity) — CORRUPT [C]
│       └── Handling: effect gateway covers every write outside the target namespace
│
├── 7. SIDE-EFFECT CONTROL  (C-EFG)
│   ├── 7.1 Effect inventory (declared per handler, reviewed)
│   │   ├── External: payments, payouts, emails, SMS, push, webhooks, partner APIs, KYC
│   │   ├── Internal async: consumer outbox events to other services
│   │   ├── Internal sync: calls to other services (enrichment, validation, commands)
│   │   ├── Shared state: caches, search indexes, shared tables, feature stores
│   │   └── Observability with business meaning: billing meters, usage counters
│   │
│   ├── 7.2 Modes per effect
│   │   ├── SUPPRESS — recorded as "would have executed" with full parameters (for verification)
│   │   ├── ALLOW — executes (only internal, namespaced, or explicitly approved)
│   │   └── RECONCILE — §7.3
│   │
│   ├── 7.3 RECONCILE (RECOVER mode)
│   │   ├── For each effect intent: idempotency key derived exactly as live
│   │   ├── Within provider idempotency window → call provider with same key (provider dedupes executed ones)
│   │   ├── Beyond window → query provider records / statements; execute only if absent; else record as done
│   │   ├── Ambiguous → hold for human decision (T0 financial)
│   │   └── Effect ledger output: executed / already-done / held per intent
│   │
│   ├── F7.1 Header-based replay flag — never set on historical records — effects fire [C]
│   │   └── Handling: run mode from manifest; RP-02
│   │
│   ├── F7.2 Handler calls a provider client directly — bypasses gateway — duplicate charges [C]
│   │   └── Handling: RP-03 enforced in CI; replay runner has no provider credentials (6.8)
│   │
│   ├── F7.3 Consumer outbox emits during replay — downstream floods with historical events [C]
│   │   ├── Impact: every downstream service processes weeks of re-emitted events; their own effects fire
│   │   └── Handling: outbox writes are effects (SUPPRESS by default); HYDRATE of a downstream-facing model
│   │                 requires explicit downstream coordination
│   │
│   ├── F7.4 Live enrichment calls during replay — load storm on internal services + non-determinism [C]
│   │   └── Handling: SUPPRESS with recorded intent; enrichment data must come from events or as-of reference data
│   │
│   ├── F7.5 RECOVER with blanket SUPPRESS — effects that never happened are skipped forever — LOSS [C]
│   │   └── Handling: 7.3 RECONCILE
│   │
│   └── F7.6 Suppressed-effect log contains PII — COMPLIANCE [C]
│       └── Handling: same classification and retention as quarantine data; masked views
│
├── 8. DETERMINISM  (RP-06)
│   ├── 8.1 Time: handlers use event time (injected clock), never wall clock, for business decisions
│   ├── 8.2 Identifiers: generated from (event_id, purpose) deterministically — rebuilt rows keep same ids
│   ├── 8.3 Reference data: versioned; replay uses versions as-of event time (or manifest-declared)
│   ├── 8.4 Feature flags: evaluated as-of event time or pinned in manifest
│   ├── 8.5 Randomness: forbidden in projection logic
│   ├── 8.6 Cross-aggregate reads: only from target store state built so far (replay order dependent) →
│   │       handlers must tolerate any interleaving (ordering tree §11)
│   │
│   ├── F8.1 Projection generates random UUIDs — external references to rows break after cutover — CORRUPT [C]
│   │   ├── Impact: clients, bookmarks, other services holding view ids find nothing
│   │   └── Handling: 8.2; parity check includes id stability
│   │
│   ├── F8.2 Wall-clock logic (age calculations, "expired" flags) — rebuilt state differs from what users saw [C]
│   │   └── Handling: 8.1; derived time fields computed at read time, not stored, when possible
│   │
│   ├── F8.3 Replay interleaves partitions differently than original — different cross-aggregate results [C]
│   │   └── Handling: 8.6 tolerance; parity flags interleaving-sensitive fields
│   │
│   └── F8.4 FORENSIC with current code — reinterpretation presented as historical truth — COMPLIANCE [C]
│       └── Handling: FORENSIC pins code/upcasters/config/reference data; report states exactly what was pinned
│
├── 9. EXECUTION
│   ├── 9.1 Pipeline per record: source read → upcast (manifest revision set) → version guard → handler →
│   │                            target write + position (one tx) → effect gateway decisions recorded
│   │
│   ├── 9.2 Ordering & gaps
│   │   ├── Same exact-next rule, held keys, gap classification as live
│   │   ├── Gaps in history → run-local hold → re-emit from S4 or snapshot resync for that aggregate
│   │   └── Unresolvable gaps → run report lists aggregates as INCOMPLETE (blocks cutover for T0)
│   │
│   ├── 9.3 Poison in replay
│   │   ├── Quarantined in run-local store; keys held
│   │   ├── Known live quarantines for same events: linked for triage
│   │   └── Threshold of quarantines → run PAUSED (systemic: bad upcaster, bad handler build)
│   │
│   ├── 9.4 Throughput & load protection (RP-12)
│   │   ├── Kafka: per-partition read rate cap; fetch from followers if available; avoid evicting hot page cache
│   │   ├── Target DB: bulk-friendly writes; batch sizes bounded; WAL and replica lag monitored
│   │   ├── Live SLO guard: live time lag or producer latency breach → automatic replay throttle/pause
│   │   └── Parallelism: across partitions and key lanes, bounded by target capacity
│   │
│   ├── 9.5 Bulk-load tuning with mandatory restore steps
│   │   ├── PostgreSQL: create secondary indexes after bulk phase (concurrently); keep primary key; analyze after
│   │   ├── OpenSearch: refresh disabled and replicas 0 during bulk → restored before parity and cutover
│   │   └── Restore steps are tracked checklist items in run state (cutover blocked until verified)
│   │
│   ├── 9.6 Checkpointing & resume
│   │   ├── Positions stored in target DB per partition (offset tree)
│   │   ├── Runner crash → resume from stored positions; archive phase resumes by archive file offset
│   │   └── Code/config change mid-run → forbidden (new manifest = new run)
│   │
│   ├── F9.1 Replay evicts broker page cache — live producer/consumer latency rises — DEGRADE [L]
│   │   └── Handling: 9.4 rate caps; off-peak scheduling for full replays
│   │
│   ├── F9.2 Target replicas lag (WAL volume) — read replicas serving stale/broken data elsewhere [C]
│   │   └── Handling: separate target cluster/schema for large rebuilds; replica lag guard
│   │
│   ├── F9.3 OpenSearch replicas left at 0 after cutover — data loss on node failure — LOSS [C]
│   │   └── Handling: 9.5 checklist gate
│   │
│   ├── F9.4 Upcaster bug discovered mid-run — partial target with mixed logic — CORRUPT [C]
│   │   └── Handling: abort run; new manifest; restart from clean target
│   │
│   └── F9.5 Run takes longer than estimated — retention handoff margin consumed (F3.4) [C]
│       └── Handling: continuous ETA vs margin monitoring; pause + retention extension
│
├── 10. CATCH-UP & PROMOTION  (replay run becomes the live writer for the new target)
│   ├── 10.1 Phases
│   │   ├── P1 Historical: sources → target until handoff to live Kafka positions
│   │   ├── P2 Follow-live: run continues consuming live topic; time lag converges to live SLO
│   │   ├── P3 Dual-run: old live group keeps writing old projection; new run keeps writing new projection
│   │   └── P4 Promotion: new run's group becomes the registered LIVE group for the new target
│   │                    (mode switches from REBUILD/HYDRATE to LIVE for its target-only effects)
│   │
│   ├── 10.2 Effects at promotion
│   │   ├── External effects remain owned by exactly ONE live group across the pair
│   │   ├── If the new projection's handler also owns external effects, ownership transfer happens at a
│   │   │   recorded per-partition position: old group stops effects at P, new group starts effects after P
│   │   └── Never both, never neither (effect ownership ledger)
│   │
│   ├── F10.1 Replay group terminated at cutover — new target frozen — CORRUPT (stale reads) [C]
│   │   └── Handling: RP-07 promotion
│   │
│   ├── F10.2 Effect ownership overlap — duplicate notifications/charges [C]
│   │   └── Handling: 10.2 position-based handoff, verified by effect ledger
│   │
│   ├── F10.3 Effect ownership gap — effects skipped between handoff points — LOSS [C]
│   │   └── Handling: same; reconciliation of effects over handoff window
│   │
│   └── F10.4 Follow-live never converges (target slower than live rate) — cutover impossible [C]
│       └── Handling: capacity fix before cutover; never cut over a lagging target
│
├── 11. PARITY & VERIFICATION
│   ├── 11.1 Comparison basis: source of truth (producer write model / snapshot API) at the SAME aggregate version
│   ├── 11.2 Expected-diff rules (Scenario A): each rule names the bug, affected fields, predicate of affected aggregates
│   ├── 11.3 Levels [L]
│   │   ├── T0: 100% aggregates, field-level comparison
│   │   ├── T1: 100% aggregate versions + sampled field-level (≥ 5%)
│   │   └── T2: sampled (≥ 1%) + counts and checksums
│   ├── 11.4 Structural checks: row counts per partition of keyspace, id stability, index/document counts,
│   │                           referential integrity, null-rate drift per column
│   ├── 11.5 Old vs new comparison (secondary): every difference classified as expected-fix or unexplained
│   ├── 11.6 Gate: unexplained differences = 0 (T0), ≤ tolerance (T1/T2, approved)
│   │
│   ├── F11.1 Parity against moving live state — flapping results [C]
│   │   └── Handling: compare at matching versions; ignore aggregates mid-update (retry later)
│   │
│   ├── F11.2 "100% parity" demanded for a bug-fix rebuild — gate never passes or is overridden [C]
│   │   └── Handling: 11.2 expected-diff rules
│   │
│   ├── F11.3 Counts match, content wrong — false confidence [C]
│   │   └── Handling: field-level checks per tier
│   │
│   └── F11.4 Parity queries overload source of truth — producer DEGRADE [C]
│       └── Handling: rate-limited snapshot API; replica reads with version awareness
│
├── 12. CUTOVER
│   ├── 12.1 Read switch (no long locks)
│   │   ├── Relational: readers query a stable view (or synonym); cutover redefines the view pointer
│   │   │   under a short lock timeout with retry; or application read-target config switch
│   │   ├── OpenSearch: atomic alias swap (remove old + add new in one request)
│   │   └── Cache: namespaced by projection version; switch with the pointer (no global invalidation storm)
│   │
│   ├── 12.2 Write ownership: already promoted (§10); old group continues writing old projection (dual-run)
│   │
│   ├── 12.3 Held/quarantine state
│   │   ├── New target has its own held/quarantine items (from run) — triaged before cutover (T0: zero open)
│   │   └── Old live quarantines for affected handler: dispositioned or migrated with explicit mapping
│   │
│   ├── 12.4 Observation window [L: 7 days T0, 72 h T1, 24 h T2]
│   │   ├── Rollback = switch read pointer back (old projection still current via dual-run)
│   │   ├── Monitoring: error rates, query latency, business KPIs, parity sampling continues
│   │   └── Exit criteria recorded; approval to decommission
│   │
│   ├── 12.5 Decommission
│   │   ├── Stop old group → remove old positions → archive or drop old projection → remove old cache namespace
│   │   └── Dropping old projection only after decommission approval (never on a fixed 48 h timer)
│   │
│   ├── F12.1 Table rename under live load — lock queue outage [C]
│   │   └── Handling: 12.1 pointer switch with lock timeout
│   │
│   ├── F12.2 Reader queries incompatible with new schema — errors at cutover [C]
│   │   └── Handling: readers deployed compatible with both projections before switch
│   │
│   ├── F12.3 Rollback to a stale backup (old projection not maintained) — CORRUPT [C]
│   │   └── Handling: RP-08 dual-run
│   │
│   ├── F12.4 Cutover with non-converged target (lag above SLO) — stale reads [C]
│   │   └── Handling: gate requires follow-live time lag ≤ live SLO for a sustained window
│   │
│   └── F12.5 Alias swap done as two requests — window with zero or two indexes — DEGRADE/CORRUPT [C]
│       └── Handling: single atomic alias update
│
├── 13. SCENARIO-SPECIFIC REQUIREMENTS
│   ├── 13.1 A — Projection bug fix
│   │   ├── Expected-diff rules mandatory
│   │   ├── Consider resync of affected aggregates only (cheaper, safer) when predicate is precise
│   │   └── Downstream consumers of the projection informed of corrected values (if they cached them)
│   │
│   ├── 13.2 B — New read model
│   │   ├── Coverage or snapshot bootstrap decision
│   │   ├── Cutover = first exposure; readers feature-flagged
│   │   └── No old projection → rollback = disable feature
│   │
│   ├── 13.3 C — Forensic reconstruction
│   │   ├── Pinned build, upcasters, schemas, reference data, flags as-of target time
│   │   ├── Stop condition: per-partition offsets mapped from target time (recorded, margin documented)
│   │   ├── Target read-only after completion; access-controlled; legal hold if required
│   │   ├── All effects SUPPRESS; no outbox; no enrichment
│   │   └── Report: manifest hash, coverage result, gaps, interpretation caveats
│   │
│   └── 13.4 D — Disaster recovery
│       ├── Start from latest consistent backup + its stored positions (state and positions co-located)
│       ├── Replay from stored positions to live head (archive if positions are older than retention)
│       ├── Effects: RECONCILE (7.3) for the window between backup point and recovery
│       ├── Gaps from lost producer data → producer recovery first (outbox tree §7, §11)
│       └── Declare recovery complete only after reconciliation (ordering tree RC-01, effects ledger)
│
├── 14. COMPLIANCE & SECURITY
│   ├── 14.1 Erasure: replay never resurrects erased data (RP-11); projections from shredded payloads drop fields
│   ├── 14.2 Retention: replays may not process data past its lawful retention unless legally required (FORENSIC)
│   ├── 14.3 Access: run creation, approval, and target access role-separated; FORENSIC targets restricted
│   ├── 14.4 Audit: manifest, approvals, effect gateway decisions, cutover actions immutable
│   │
│   ├── F14.1 Erased customer reappears in rebuilt view — COMPLIANCE [C]
│   │   └── Detection: post-rebuild check against erasure registry
│   │
│   └── F14.2 Replay target with production PII placed in lower environment — COMPLIANCE [C]
│       └── Handling: replays of production data only into production-classified infrastructure
│
├── 15. CAPACITY & TIME ESTIMATION  [G — needs your numbers]
│   ├── Inputs: events in scope, bytes in scope, sustained handler throughput per lane, target write capacity,
│   │           allowed fraction of broker/target capacity, Kafka retention, live produce rate
│   ├── ETA(historical) ≈ events in scope ÷ sustainable replay rate
│   ├── Convergence condition: replay rate > live produce rate by a margin [L: ≥ 2×]
│   ├── Retention handoff margin (§3.2) computed from ETA, re-evaluated continuously
│   └── Target storage: final size + bulk overhead + old projection retained through observation window
│
├── 16. OBSERVABILITY
│   ├── Run state, phase, per-partition progress, ETA vs retention margin
│   ├── Applied / duplicate / stale / gap / quarantined counts per run
│   ├── Effect gateway decisions by effect type and mode (SUPPRESS counts must match expectations)
│   ├── Live impact: live time lag, producer latency, broker fetch/disk, target replica lag
│   ├── Parity progress and difference classes
│   ├── Bulk-tuning restore checklist status
│   ├── Promotion and effect ownership handoff positions
│   └── Absence alerts for runs in progress
│
├── 17. RUNBOOKS
│   ├── RB-01 Rebuild after projection bug → manifest → coverage → run → parity w/ expected-diff → promote → cutover → observe → decommission
│   ├── RB-02 New read model → HYDRATE → parity vs source → feature-flagged cutover
│   ├── RB-03 Forensic request → pinned manifest → isolated run → report → lock target
│   ├── RB-04 Disaster recovery → backup restore → replay to head → effects RECONCILE → reconciliation → resume LIVE
│   ├── RB-05 Run degrading live → automatic throttle → if persists, pause run → reschedule
│   ├── RB-06 Mid-run defect (upcaster/handler) → abort → fix → new manifest → clean target
│   ├── RB-07 Retention margin at risk → pause historical phase → extend retention → resume
│   └── RB-08 Rollback during observation → switch read pointer → investigate → keep dual-run
│
└── 18. GO-LIVE TEST MATRIX
    ├── Full rebuild in staging with production-sized data → ETA within estimate; live SLO unaffected
    ├── Handler with direct provider client → CI blocks; replay runner has no provider credentials
    ├── Replay of projection whose handler writes consumer outbox → zero events emitted downstream
    ├── Archive + Kafka handoff → contiguous coverage, no duplicates beyond idempotent absorption
    ├── Handoff margin artificially shortened → run pauses before retention loss
    ├── Gap in history → aggregate held, re-emitted/resynced, or reported INCOMPLETE
    ├── Rebuilt rows keep identical ids (deterministic ids)
    ├── Bug-fix rebuild → all diffs matched by expected-diff rules; injected unexplained diff blocks cutover
    ├── Promotion → new target stays current after cutover; old projection still current (dual-run)
    ├── Rollback during observation → pointer switch, no data loss
    ├── Cutover under heavy read load → no lock queue, no errors
    ├── OpenSearch bulk settings restored before cutover gate passes
    ├── RECOVER drill → effects reconciled: no duplicates, no skipped effects
    ├── FORENSIC run → reproducible result from same manifest hash
    ├── Erased customer data → absent from rebuilt projection
    └── Metrics pipeline down → absence alerts fire
```

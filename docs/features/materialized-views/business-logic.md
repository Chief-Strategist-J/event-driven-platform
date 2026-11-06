# Materialized Views — Business Logic Decision Tree

```
MATERIALIZED VIEWS — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-VRG  View registry — each view: class, source streams, store, owner, freshness SLO, consistency mode
│   │   ├── C-PRJ  Projection runtime — the standard consumer runtime (inbox, version guard, held keys, lanes)
│   │   ├── C-PVS  Primary view store (PostgreSQL, writable) — authoritative projections
│   │   ├── C-DVS  Derived view stores — OpenSearch (search), Redis (low-latency cache) fed from C-PVS
│   │   ├── C-FAN  Fan-out pipeline — C-PVS changes → derived stores (view outbox or CDC on projection tables)
│   │   ├── C-CTK  Consistency tokens — command responses carry aggregate version for read-your-writes
│   │   ├── C-RBD  Rebuild orchestrator — replay tree (manifests, promotion, cutover)
│   │   ├── C-REC  Reconciliation — views vs source at matching versions; derived vs primary
│   │   └── C-MON  Monitoring — freshness decomposition, version lag, held keys, store health
│   │
│   ├── 0.2 View classes (declared per view)
│   │   ├── V-STATE   Latest state per aggregate (order summary, profile)
│   │   ├── V-AGG     Aggregations across aggregates (totals, counters, balances by category)
│   │   ├── V-JOIN    Denormalized across streams/aggregates (order + customer + product)
│   │   ├── V-SEARCH  Full-text/facets over V-STATE/V-JOIN documents
│   │   ├── V-KV      Low-latency key lookups (cache of V-STATE)
│   │   └── V-REPORT  Financial/regulatory reporting (period-based, reconciled to books of record)
│   │
│   └── 0.3 Ownership
│       ├── View owner team: projection logic, view schema, SLOs, reconciliation rules
│       ├── Platform: runtime, fan-out, store operations, rebuild tooling
│       └── Finance (V-REPORT): period close rules, sign-off, reconciliation to ledger
│
├── 1. INVARIANTS  (with enforcement)
│   ├── MV-01 Every write is version-guarded by the store that holds the data [C]
│   │   └── Enforced by: PostgreSQL conditional writes in tx; OpenSearch external versioning; Redis atomic CAS script
│   ├── MV-02 Zero-row results are failures, never success [C]
│   │   └── Enforced by: runtime checks affected rows / store responses; mismatch → classify (§5)
│   ├── MV-03 Dedupe, guard, mutation, and position are atomic per store [C]
│   │   └── Enforced by: one PostgreSQL tx; for derived stores, version-in-document replaces dedupe tables
│   ├── MV-04 One authoritative store per view [C]
│   │   └── Enforced by: registry; derived stores rebuilt from it, never written independently by other paths
│   ├── MV-05 Derived stores are disposable [C]
│   │   └── Enforced by: tested rebuild path from C-PVS; no data exists only in OpenSearch/Redis
│   ├── MV-06 No TTL on view data [C]
│   │   └── Enforced by: TTL allowed only on cache entries with read-through fallback to C-PVS
│   ├── MV-07 Projections are deterministic [C]
│   │   └── Enforced by: no live calls to other services; event time not wall clock; deterministic ids (replay tree RP-06)
│   ├── MV-08 Views never write back to write models [C]
│   │   └── Enforced by: projection credentials read-only on write models (they don't read them either)
│   ├── MV-09 Freshness is explicit and measured [C]
│   │   └── Enforced by: per-view freshness SLO; decomposition metrics; staleness exposed to readers where needed
│   ├── MV-10 Read-your-writes is a declared mode per view [C]
│   │   └── Enforced by: registry consistency mode; token-aware read APIs (§9)
│   ├── MV-11 Views are tenant-scoped and erasure-aware [C]
│   │   └── Enforced by: multi-tenant tree MT-06; erasure inventory includes every view
│   └── MV-12 Each view consumes independently [C]
│       └── Enforced by: one consumer group per view (failure of one view never blocks another)
│
├── 2. STORE SELECTION
│   ├── 2.1 Decision
│   │   ├── Needs transactions, exact deltas, joins, reporting → PostgreSQL (C-PVS) — authoritative
│   │   ├── Needs full-text, facets, relevance → OpenSearch as derived from C-PVS (V-SEARCH)
│   │   ├── Needs sub-ms lookups at high QPS → Redis as derived cache from C-PVS (V-KV)
│   │   └── Simple V-STATE with full-state events and no deltas → OpenSearch may be primary IF full-document
│   │                                                             external versioning is used and rebuild path exists
│   │
│   ├── 2.2 Why layered (PostgreSQL first, then fan-out)
│   │   ├── Deltas and joins need transactions (not available in search/cache stores)
│   │   ├── Derived stores get full documents with versions → idempotent, order-safe
│   │   └── Rebuilding derived stores becomes cheap (from C-PVS, not from Kafka history)
│   │
│   ├── 2.3 PostgreSQL placement
│   │   ├── Writable database/schema dedicated to views (never the write-model database's hot path)
│   │   └── Reporting reads from a physical replica of the VIEW database (not the write model)
│   │
│   ├── F2.1 Projection writes to a physical read replica — impossible (read-only) — OUTAGE [C]
│   ├── F2.2 Redis as the only store of a view — failover/eviction loses data — LOSS [C]
│   ├── F2.3 OpenSearch partial-update scripts as primary projection — non-idempotent deltas — CORRUPT [C]
│   └── F2.4 Dashboards query write model "temporarily" — write latency and lock contention — DEGRADE [C]
│       └── Handling: registry rule + DB role separation; dashboards have no write-model credentials
│
├── 3. PROJECTION TOPOLOGY
│   ├── 3.1 One consumer group per view (MV-12); views sharing a stream don't share failure
│   ├── 3.2 Fan-out from C-PVS to derived stores
│   │   ├── Option A: view outbox row in the same projection tx → relay → derived updaters
│   │   ├── Option B: CDC on projection tables (CDC tree M-RAWTABLE, same team)
│   │   └── Payload: full document + view version (= aggregate version or view-local sequence)
│   ├── 3.3 Multi-handler-in-one-group (only when views must be updated atomically together in one DB tx)
│   │   └── Partial failure rules: consumer tree F3.6 (offset advances only when all handlers done)
│   │
│   ├── F3.1 One group feeding many views — one broken view stalls all — OUTAGE [C]
│   └── F3.2 Derived store updated directly from Kafka AND from C-PVS fan-out — two writers — CORRUPT [C]
│       └── Handling: MV-04 single path
│
├── 4. UPDATE PIPELINE PER STORE
│   ├── 4.1 PostgreSQL (authoritative)
│   │   ├── One transaction: inbox insert (conflict-ignore + row check) → version decision (§5) →
│   │   │   mutation (insert for first version, conditional update for next) → affected-row check →
│   │   │   view outbox (if fan-out) → stored offset → commit
│   │   ├── STATE semantics: replace columns from event state; guard "strictly newer" acceptable
│   │   ├── DELTA semantics: guard "exactly next" mandatory; arithmetic in the same statement
│   │   ├── Affected rows ≠ 1 → classify (§5) — never acknowledge silently (MV-02)
│   │   └── Money: exact numeric types; amounts in minor units (outbox tree INV-12)
│   │
│   ├── 4.2 OpenSearch (derived or full-state primary)
│   │   ├── Full document index with external version (accept only strictly greater) [C]
│   │   ├── No partial update scripts for versioned documents (update API lacks external versioning) [C]
│   │   ├── Version conflict response = stale → acknowledge, count (not an error)
│   │   ├── Bulk requests: inspect every item result; retry failed items individually (backpressure tree §7.2)
│   │   ├── Deletes: delete with external version; keep tombstone semantics long enough (resurrection guard)
│   │   ├── Refresh interval: near-real-time visibility (default about 1 s) — reads not immediate [C]
│   │   └── Mappings: explicit, dynamic mapping disabled or strict (field-limit explosion protection)
│   │
│   ├── 4.3 Redis (derived cache)
│   │   ├── Writes via atomic server-side script: compare stored version < incoming → write document + version [C]
│   │   ├── Pipelines used only for batching, never assumed atomic [C]
│   │   ├── No expiry on view keys; cache-only keys may expire with read-through fallback to C-PVS (MV-06)
│   │   ├── Eviction policy: no eviction for view-like data; memory capacity alerts
│   │   ├── Persistence and replication acknowledged as lossy on failover [C] → version guard + rebuild path
│   │   └── Keys tenant-prefixed (multi-tenant tree §5.5)
│   │
│   ├── F4.1 UPDATE affects 0 rows, acknowledged — silent divergence — CORRUPT [C]
│   ├── F4.2 First event uses UPDATE — new aggregates never appear — LOSS [C]
│   ├── F4.3 Delta applied with "strictly newer" guard — skipped deltas — CORRUPT [C]
│   ├── F4.4 Bulk partial failures treated as success — LOSS in search [C]
│   ├── F4.5 Search delete without version — late update resurrects document — CORRUPT [C]
│   ├── F4.6 Dynamic mapping on user-supplied keys — mapping explosion / indexing failures — OUTAGE [C]
│   ├── F4.7 Redis failover loses recent writes; later delta applied on stale base — CORRUPT [C]
│   │   └── Handling: Redis holds full documents (no deltas); version CAS; reconcile with C-PVS
│   └── F4.8 Redis TTL on view keys — inactive users vanish from view — LOSS [C]
│
├── 5. VERSION DECISION TABLE  (fixes v1's inverted logic; PostgreSQL authoritative views)
│   ├── No row exists, incoming version = 1 → INSERT
│   ├── No row exists, incoming version > 1 → bootstrap policy (snapshot) or GAP hold (ordering tree §7.4)
│   ├── incoming = current + 1 → APPLY (insert/update), affected rows must be 1
│   ├── incoming ≤ current, same content hash → DUPLICATE → acknowledge, count
│   ├── incoming ≤ current, different hash (same version) → INTEGRITY → hold key + quarantine + incident
│   ├── incoming < current (older) → STALE → acknowledge, count
│   ├── incoming > current + 1 → GAP → durable hold (never in-memory) → re-emit / resync (ordering tree §8)
│   └── Conditional write lost a race (0 rows despite expected state) → concurrent writer → epoch check / incident
│
│   ├── F5.1 Stale events quarantined as poison — DLQ noise; triage on harmless duplicates — DEGRADE [C]
│   ├── F5.2 Gaps applied as "newer" in STATE views that hold partial state — CORRUPT [C]
│   │   └── Handling: STATE "strictly newer" only when every event carries complete state
│   └── F5.3 In-memory buffer for out-of-order events — LOSS on restart [C]
│
├── 6. MULTI-STREAM / JOIN VIEWS  (V-JOIN)
│   ├── 6.1 Each source stream has its own ordering; no order across streams (ordering tree §11)
│   ├── 6.2 Local reference tables inside the view DB (customer names, product titles) maintained from their streams
│   ├── 6.3 Missing reference at projection time → placeholder / pending field; filled when reference arrives
│   ├── 6.4 Per-source version tracked separately in the view row (e.g., order_version, customer_version)
│   ├── 6.5 Fan-out on reference change
│   │   ├── Customer name change touches N order rows → update amplification
│   │   ├── Options: join at read time for rarely-changing fields; batched background re-denormalization;
│   │   │            or store reference id only and resolve via local table at query time
│   │   └── Bounded by rate limits (backpressure tree)
│   │
│   ├── F6.1 Projection calls customer service live to enrich — non-deterministic, load storms on replay — CORRUPT/OUTAGE [C]
│   │   └── Handling: MV-07; local reference tables
│   ├── F6.2 Reference update arrives before entity row exists — reference lost — CORRUPT [C]
│   │   └── Handling: reference tables independent of entity rows; entity reads current reference on insert
│   ├── F6.3 Single version column for multi-source rows — one stream's updates rejected as stale — LOSS [C]
│   │   └── Handling: 6.4 per-source versions
│   └── F6.4 Fan-out of a popular reference (1M rows) — DB saturation — OUTAGE [C]
│       └── Handling: 6.5 strategy; bounded batch re-denormalization
│
├── 7. AGGREGATION / COUNTER VIEWS  (V-AGG)
│   ├── 7.1 Every contribution is DELTA → exact-next per source aggregate + inbox (no skipping, no double)
│   ├── 7.2 Track contributions per source aggregate (contribution table) so corrections/reversals are exact
│   ├── 7.3 Recomputable: aggregate = sum of contributions (periodic full recompute validates incremental)
│   ├── 7.4 Hot rows (global counters) → contention: per-lane partial sums merged periodically, or computed at read
│   │
│   ├── F7.1 Counter incremented without per-source idempotency — double counting on redelivery — CORRUPT [C]
│   ├── F7.2 Source event corrected (refund, cancel) but contribution not reversible — wrong totals — CORRUPT [C]
│   │   └── Handling: 7.2 contribution table keyed by source aggregate
│   └── F7.3 Global counter row lock — throughput collapse — DEGRADE [C]
│
├── 8. REPORTING VIEWS  (V-REPORT)
│   ├── 8.1 Views are not books of record; reports reconcile to the ledger/write model [C]
│   ├── 8.2 Time semantics: event time for period assignment; never processing time
│   ├── 8.3 Late events
│   │   ├── Open period → applied normally
│   │   ├── Closed period → recorded as adjustment in current period (never mutate closed figures silently)
│   │   └── Late-event counts reported to finance
│   ├── 8.4 Period close
│   │   ├── Freeze: snapshot of the view at close with consumer positions recorded
│   │   ├── Reconcile to ledger; sign-off; immutable stored report
│   │   └── Re-opening a period is a governed action with audit
│   ├── 8.5 Currency and rounding rules versioned (reference data as-of)
│   │
│   ├── F8.1 Report computed from live view mid-close — numbers change after sign-off — COMPLIANCE [C]
│   ├── F8.2 Late event mutates closed period — restated figures without audit — COMPLIANCE [C]
│   └── F8.3 Processing-time bucketing — revenue shifts between periods during backlogs — CORRUPT [C]
│
├── 9. READ CONSISTENCY & FRESHNESS
│   ├── 9.1 Consistency modes (per view, MV-10)
│   │   ├── EVENTUAL — readers accept staleness up to freshness SLO
│   │   ├── TOKEN — command returns aggregate version; read API waits (bounded) until view version ≥ token,
│   │   │          else returns with "pending" indicator or falls back to authoritative read for that aggregate
│   │   └── AUTHORITATIVE-FOR-OWN — user's own just-modified aggregates read from write-model API (bounded use)
│   │
│   ├── 9.2 Freshness decomposition (per event, sampled)
│   │   ├── Producer delay: commit time → Kafka append
│   │   ├── Transport/lag: Kafka append → projection consume
│   │   ├── Processing: consume → C-PVS commit
│   │   └── Fan-out: C-PVS commit → derived store visible (incl. search refresh)
│   │
│   ├── 9.3 Staleness exposure
│   │   ├── Views include "as-of" version/time where users make decisions on them
│   │   └── Held/parked keys surface as "updating" rather than showing silently stale data (T0 views)
│   │
│   ├── F9.1 User retries action because view shows old state — duplicate commands — CORRUPT [C]
│   │   └── Handling: TOKEN mode for action-confirming screens; command idempotency keys (outbox tree INV-09)
│   ├── F9.2 Freshness metric uses event occurred-at with skewed clocks — misleading lag [C]
│   │   └── Handling: decomposition using system timestamps at each hop; clock sync monitoring
│   └── F9.3 Search shows new state, cache shows old (different fan-out speeds) — inconsistent UI [C]
│       └── Handling: one read path per screen; version-aware client merging for multi-store pages
│
├── 10. VIEW SCHEMA EVOLUTION
│   ├── 10.1 Additive changes (new column/field)
│   │   ├── Deploy readers tolerant of null/missing
│   │   ├── Backfill: re-projection (rebuild) or targeted backfill from source snapshots (version-aware)
│   │   └── New field populated only by new events = inconsistent until backfill completes (declare)
│   ├── 10.2 Breaking changes → new view version via rebuild (replay tree)
│   ├── 10.3 Event schema evolution → upcasters before projection logic (versioning tree §9)
│   │
│   ├── F10.1 Column added with NOT NULL and default on large table — lock/rewrite — OUTAGE [L]
│   │   └── Handling: online migration patterns; lock timeouts (outbox tree F1.12)
│   └── F10.2 Backfill races with live projection — older backfill value overwrites newer — CORRUPT [C]
│       └── Handling: backfill writes version-guarded (only if row version ≤ snapshot version)
│
├── 11. REBUILD & RE-PROJECTION  (replay tree is authoritative; view-specific notes)
│   ├── Source: snapshots + streams or archive (not "offset 0" on retention-bounded topics)
│   ├── Shadow view promoted to live writer at cutover (never terminated)
│   ├── Derived stores rebuilt from new C-PVS view (fast), not from Kafka
│   ├── Read switch via view/alias pointer under lock timeout (no table renames under load)
│   ├── Dual-run observation; decommission by approval, not fixed timer
│   └── Effects suppressed (views rarely have effects; view outbox for fan-out targets shadow derived stores only)
│
├── 12. DELETES, ERASURE & MULTI-TENANCY
│   ├── 12.1 Aggregate deletion events → view row removal or terminal state; derived stores delete with version
│   ├── 12.2 Erasure: views listed in plaintext inventory; erasure deletes/anonymizes rows and derived docs;
│   │        rebuild honors erasure (replay tree RP-11)
│   ├── 12.3 Tenant scoping: tenant column + RLS (PostgreSQL), filtered aliases (OpenSearch), key prefix (Redis)
│   ├── F12.1 Erased subject persists in search index or cache — COMPLIANCE [C]
│   ├── F12.2 Search aggregations leak other tenants' counts — SECURITY [C]
│   └── F12.3 Deleted aggregate resurrected by stale fan-out message — CORRUPT [C]
│       └── Handling: tombstone versions retained; derived deletes versioned
│
├── 13. PERFORMANCE & CAPACITY
│   ├── 13.1 PostgreSQL
│   │   ├── Upsert churn → bloat → per-table autovacuum tuning; fillfactor for update-heavy tables
│   │   ├── Index count minimal on write-heavy views; reporting indexes on replica-friendly tables
│   │   └── Reporting queries isolated from projection writes (replica or separate pool)
│   ├── 13.2 OpenSearch
│   │   ├── Shard sizing, refresh interval per freshness SLO, bulk sizing in bytes
│   │   ├── Field count limits respected; nested documents sized
│   │   └── Hot documents (frequently updated) → merge/indexing overhead; coalesce updates (O-STATE)
│   ├── 13.3 Redis
│   │   ├── Memory sized for full keyspace (no eviction for view-like data)
│   │   └── Large values avoided; cluster slot hot spots monitored
│   ├── 13.4 Catch-up after outage → O-STATE coalescing (backpressure tree §9.2) for V-STATE/V-SEARCH
│   │
│   ├── F13.1 Long reporting query blocks projection writes (locks, I/O) — freshness breach — DEGRADE [C]
│   ├── F13.2 Index bloat on hot view tables — latency creep — DEGRADE [C]
│   └── F13.3 Redis memory full with no eviction — writes rejected — OUTAGE [C]
│       └── Handling: capacity alerts; writes rejected → ENVIRONMENTAL pause (never drop)
│
├── 14. FAILURE & RECOVERY
│   ├── 14.1 Worker crash → resume from stored offset (C-PVS) → inbox/version guard absorb duplicates
│   ├── 14.2 C-PVS failover/restore → offsets rewind with data (co-located) → consistent replay
│   ├── 14.3 OpenSearch cluster loss → rebuild from C-PVS (MV-05) → alias switch
│   ├── 14.4 Redis loss → rebuild or lazy read-through from C-PVS
│   ├── 14.5 Poison event → DLQ tree (quarantine + key hold); view shows "updating" for held aggregates (T0)
│   ├── F14.1 Offsets stored in Kafka while view in PostgreSQL restored from backup — gap — LOSS [C]
│   │   └── Handling: positions stored in C-PVS (offset tree OF-01)
│   └── F14.2 Derived store rebuild from Kafka instead of C-PVS — slow + retention-bounded — DEGRADE/LOSS [C]
│
├── 15. OBSERVABILITY
│   ├── Per view: freshness p50/p99 by hop (§9.2) vs SLO
│   ├── Version lag: source current version vs view version (sampled per aggregate; T0 full via reconciliation)
│   ├── Decision counts: applied / duplicate / stale / gap / integrity / zero-row failures (last two must be 0)
│   ├── Held keys per view; oldest held age (business staleness)
│   ├── Derived store: version conflicts (stale fan-out), bulk item failures, refresh lag, Redis CAS rejects
│   ├── Store health: bloat, autovacuum lag, OpenSearch rejections, Redis memory
│   ├── Token-mode reads: wait time, timeouts, fallbacks
│   └── Absence alerts on all of the above
│
├── 16. RECONCILIATION
│   ├── RC-01 View vs source at matching versions (sampled; T0 full) — field-level for critical fields
│   ├── RC-02 Derived vs primary (OpenSearch/Redis vs C-PVS) — version and content hash per key
│   ├── RC-03 V-AGG: incremental totals vs full recompute from contribution table
│   ├── RC-04 V-REPORT: view totals vs ledger per period (finance sign-off)
│   └── RC-05 Counts per tenant per view vs source (catches tenant scoping bugs)
│
├── 17. RUNBOOKS
│   ├── RB-01 Freshness SLO breach → decompose by hop → act on slowest hop (producer, lag, processing, fan-out)
│   ├── RB-02 Zero-row/integrity failures → hold keys → compare with source → resync aggregate
│   ├── RB-03 Derived store drift → targeted re-fan-out from C-PVS for drifted keys
│   ├── RB-04 Search cluster loss → rebuild from C-PVS → alias switch
│   ├── RB-05 V-AGG drift → recompute from contributions → correct → investigate idempotency path
│   ├── RB-06 Late events after period close → adjustment workflow → finance notification
│   ├── RB-07 View schema change → additive deploy → version-guarded backfill → or rebuild
│   └── RB-08 Projection bug → replay tree RB-01 (rebuild with expected-diff)
│
└── 18. GO-LIVE TEST MATRIX
    ├── First event of a new aggregate → row inserted (not silently skipped)
    ├── Gap (v5 then v7) → v7 held durably; re-emit fills v6; view converges; no zero-row acknowledgment
    ├── Duplicate and stale events → acknowledged and counted; not quarantined
    ├── Same version, different content → integrity hold + incident
    ├── Delta event redelivered after crash → applied exactly once
    ├── OpenSearch: stale fan-out document → version conflict, ignored; bulk partial failure retried per item
    ├── OpenSearch: delete then stale update → no resurrection
    ├── Redis failover → version CAS prevents regression; reconciliation repairs lost writes
    ├── Redis key for inactive user after 30 days → still present (no TTL on view data)
    ├── Command then immediate read (TOKEN mode) → read reflects command or reports pending
    ├── Customer rename with 100k orders → bounded re-denormalization; DB within SLO
    ├── V-AGG refund reverses contribution exactly; recompute equals incremental
    ├── Late event after period close → adjustment in current period; closed report unchanged
    ├── Search cluster deleted → rebuilt from C-PVS within RTO
    ├── Erasure → subject removed from PostgreSQL view, search, cache; rebuild keeps it absent
    └── Metrics pipeline down → absence alerts fire
```

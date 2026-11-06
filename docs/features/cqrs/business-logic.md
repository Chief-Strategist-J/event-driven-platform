# CQRS — Business Logic Decision Tree

```
CQRS — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-GW   API gateway — authentication, rate limits, routing by method class
│   │   ├── C-CMD  Command API + handlers — idempotency, authorization, aggregate load, decisions, write + outbox
│   │   ├── C-WM   Write model (PostgreSQL primary) — aggregates, constraints, outbox, idempotency records
│   │   ├── C-PIPE Projection pipeline (outbox → relay → Kafka → projectors → authoritative view → derived stores)
│   │   ├── C-QRY  Query API — reads views, applies read-time authorization, resolves consistency tokens
│   │   ├── C-VIEW View stores (authoritative PostgreSQL views; derived OpenSearch/Redis)
│   │   ├── C-TOK  Token service — issues/verifies signed consistency tokens
│   │   ├── C-WTR  Watermark service — per-partition / per-view applied positions; change notifications
│   │   ├── C-AUTH Authorization service — authoritative permissions, near-real-time revocation
│   │   ├── C-AUTHR Authoritative read API (narrow) — owner-scoped strongly consistent reads; rate-limited
│   │   └── C-MON  Monitoring — command outcomes, token resolution, fallback, freshness, conflicts
│   │
│   ├── 0.2 Consistency modes per endpoint (from materialized views tree §9.1)
│   │   ├── EVENTUAL            — no token; staleness within view freshness SLO
│   │   ├── TOKEN               — signed token from command; resolver waits (bounded) or returns PENDING
│   │   └── AUTHORITATIVE-OWN   — narrow API for the caller's own just-modified aggregate (C-AUTHR)
│   │
│   └── 0.3 Ownership
│       ├── Domain team: commands, invariants, aggregates, views, consistency mode per endpoint
│       ├── Platform: gateway, token/watermark services, resolver library, idempotency library
│       └── Security: authorization model, revocation SLOs, token signing keys
│
├── 1. INVARIANTS  (with enforcement)
│   ├── CQ-01 Commands are idempotent by key [C]
│   │   └── Enforced by: idempotency key required on every command; stored with outcome in the same tx as the write
│   ├── CQ-02 Decisions use state read under the concurrency check [C]
│   │   └── Enforced by: load aggregate + validate + conditional write in one transaction; no pre-lock validation
│   ├── CQ-03 User-edit commands carry expected version [C]
│   │   └── Enforced by: API schema requires expected_version for edit commands; mismatch → conflict response
│   ├── CQ-04 Commands never read the read model for decisions [C]
│   │   └── Enforced by: command handlers have no view-store credentials; reference data via write-side tables
│   ├── CQ-05 Cross-aggregate invariants enforced by write-side mechanisms [C]
│   │   └── Enforced by: DB constraints, reservation aggregates, or sagas — never by reading views
│   ├── CQ-06 Queries have no side effects [C]
│   │   └── Enforced by: query service has read-only credentials; no outbox/command clients in query code
│   ├── CQ-07 Consistency tokens are server-issued, signed, bounded, and expiring [C]
│   │   └── Enforced by: C-TOK verification; unsigned/expired/excessive tokens ignored (EVENTUAL) and logged
│   ├── CQ-08 Version lives inside the view record [C]
│   │   └── Enforced by: version-guarded writes; resolver reads the record's own version
│   ├── CQ-09 Lag never cascades into the write model [C]
│   │   └── Enforced by: no generic fallback to primary; C-AUTHR rate-limited and circuit-broken; degrade to PENDING
│   ├── CQ-10 Authorization is evaluated at read time [C]
│   │   └── Enforced by: C-QRY calls C-AUTH (cached with short TTL + revocation push); views carry ownership attributes only
│   ├── CQ-11 Response shape is stable regardless of resolution path [C]
│   │   └── Enforced by: any authoritative read is projected by the same projection library into the view shape
│   └── CQ-12 Ambiguous command outcomes are resolvable [C]
│       └── Enforced by: idempotent retry returns original outcome; status endpoint by idempotency key
│
├── 2. REQUEST CLASSIFICATION
│   ├── 2.1 Commands: mutation intent; POST semantics; idempotency key header required
│   ├── 2.2 Queries: no side effects; eligible for HTTP GET (ConnectRPC supports GET for side-effect-free methods [C])
│   ├── 2.3 Forbidden hybrids
│   │   ├── Queries that "touch" (last-viewed timestamps, view counters) → separate async command/event
│   │   └── Commands returning full projected views → return token + minimal command outcome only
│   │
│   ├── F2.1 GET endpoint performs mutation — crawlers, prefetchers, retries cause side effects — CORRUPT [C]
│   ├── F2.2 Query writes audit/"last seen" synchronously — read path depends on write model — OUTAGE coupling [C]
│   │   └── Handling: emit async event; read path never blocks on writes
│   └── F2.3 Command returns view read from read model — stale response presented as result — CORRUPT UX [C]
│
├── 3. COMMAND PIPELINE
│   ├── 3.1 Steps (one transaction unless noted)
│   │   ├── Step 0 (gateway) Authenticate; rate-limit per principal/tenant; require idempotency key
│   │   ├── Step 1 Idempotency lookup by (principal/tenant, key)
│   │   │   ├── Found COMPLETED → return stored outcome (same token)
│   │   │   ├── Found IN_PROGRESS → return 409/"in progress" with retry-after (never execute twice)
│   │   │   └── Not found → insert IN_PROGRESS record
│   │   ├── Step 2 Authorize command against C-AUTH (fresh decision for mutations)
│   │   ├── Step 3 Load aggregate (row lock or optimistic read) — tenant scoped
│   │   ├── Step 4 Check expected_version (user-edit commands) → mismatch → CONFLICT outcome
│   │   ├── Step 5 Validate invariants against loaded state + write-side reference data
│   │   ├── Step 6 Conditional write (version = loaded version) + outbox insert + idempotency outcome COMPLETED
│   │   ├── Step 7 Commit
│   │   └── Step 8 Response: aggregate id, new version, signed consistency token, outcome status
│   │
│   ├── 3.2 Outcome semantics
│   │   ├── COMPLETED — state changed; token issued
│   │   ├── ACCEPTED — long-running process started (saga); status resource returned
│   │   ├── REJECTED — business rule failure (deterministic; stored for idempotent replay)
│   │   └── CONFLICT — expected version mismatch / concurrent modification (client reloads)
│   │
│   ├── F3.1 Client retries after timeout without idempotency key — duplicate orders/payments — CORRUPT [C]
│   ├── F3.2 Idempotency record written outside the business tx — duplicates or lost outcomes [C]
│   │   └── Handling: CQ-01 same transaction
│   ├── F3.3 Same idempotency key reused with different payload — wrong outcome returned — CORRUPT [C]
│   │   └── Handling: store request fingerprint; mismatch → reject with explicit error
│   ├── F3.4 Validation before lock (TOCTOU) — invariant violated under concurrency — CORRUPT [C]
│   ├── F3.5 No client expected version — lost updates on concurrent edits — CORRUPT [C]
│   ├── F3.6 Hot aggregate contention — conflict storm — DEGRADE [C]
│   │   ├── Handling: server-side retry for commutative commands only (bounded, jittered);
│   │   │             user-edit commands surface conflicts; aggregate re-model if sustained
│   │   └── Detection: conflict rate per aggregate type; top contended aggregates
│   ├── F3.7 Idempotency records retained shorter than client retry horizon — late retry duplicates [C]
│   │   └── Handling: retention ≥ max client retry window (mobile offline queues included) [G]
│   └── F3.8 Long-running command returns COMPLETED before downstream steps — UX lies — CORRUPT [C]
│       └── Handling: ACCEPTED + status resource; completion events drive UI
│
├── 4. COMMAND DECISION DATA
│   ├── 4.1 Allowed sources: the aggregate itself, write-side reference tables, synchronous calls to owning
│   │       services for authoritative checks (with timeouts and failure policy)
│   ├── 4.2 Cross-aggregate invariants
│   │   ├── Uniqueness (email, username, SKU) → unique constraints or reservation table in write model
│   │   ├── Capacity (inventory, seats, limits) → reservation aggregate with its own concurrency control
│   │   └── Multi-service → saga with compensation; never "check the view then write"
│   │
│   ├── F4.1 Command validates against read model (product active? stock available?) — stale decisions — CORRUPT [C]
│   ├── F4.2 Uniqueness checked by query before insert — duplicates under concurrency — CORRUPT [C]
│   └── F4.3 Synchronous dependency outage blocks commands — OUTAGE coupling [C]
│       └── Handling: explicit failure policy per dependency (fail closed for money; queue/ACCEPTED otherwise)
│
├── 5. PROJECTION & WATERMARKS  (materialized views tree is authoritative)
│   ├── 5.1 Version inside each view record (CQ-08); version-guarded writes
│   ├── 5.2 Per-partition watermarks in C-WTR = lowest fully applied position per view per partition
│   ├── 5.3 Change notifications: projector publishes "aggregate X reached version V" (lightweight, best-effort)
│   ├── 5.4 Search visibility
│   │   ├── GET by id is realtime in OpenSearch [C]
│   │   └── Search/list results visible only after refresh [C] → list tokens need refresh-aware watermark
│   │
│   ├── F5.1 Watermark stored separately from document — disagreement after crash — wrong resolution [C]
│   ├── F5.2 Watermark advanced past held keys — list token says "caught up" while aggregate is held — stale reads [C]
│   │   └── Handling: watermark excludes held keys; resolver checks held state for tokened aggregates
│   └── F5.3 Derived store version checked when authoritative view is behind — inconsistent between pages [C]
│       └── Handling: resolver checks the store actually used by the endpoint
│
├── 6. CONSISTENCY TOKENS  (C-TOK)
│   ├── 6.1 Content
│   │   ├── Single-aggregate commands: (view scope, aggregate id, min version)
│   │   ├── Multi-aggregate commands: list of (aggregate id, min version) — bounded size [L: ≤ 10]
│   │   ├── List/search consistency: (stream, partition, min position) of the command's outbox event
│   │   ├── Issuer, tenant, principal, expiry [L: 5–15 min]
│   │   └── Signature (HMAC/asymmetric); opaque to clients
│   │
│   ├── 6.2 Verification
│   │   ├── Invalid signature / expired / other principal → ignore token (EVENTUAL) + security metric
│   │   ├── Version beyond issued version impossible (signed) → no forced-fallback attacks
│   │   └── Token scope must match endpoint (no token reuse across unrelated endpoints)
│   │
│   ├── 6.3 Client handling
│   │   ├── Clients store latest token per session; send on subsequent reads
│   │   ├── Multiple devices: token per device session; cross-device RYW not guaranteed (documented)
│   │   └── Offline/mobile: expired tokens ignored gracefully
│   │
│   ├── F6.1 Client-supplied raw version header — forced timeouts + primary reads — SECURITY/OUTAGE [C]
│   ├── F6.2 Token leaks across users (shared caches, logs) — information disclosure about activity [L]
│   │   └── Handling: tokens bound to principal; not logged in full
│   └── F6.3 Token for multi-aggregate command covers only primary aggregate — related views stale — CORRUPT UX [C]
│
├── 7. QUERY RESOLUTION
│   ├── 7.1 By-id with token
│   │   ├── Read record → version ≥ token? → return
│   │   ├── Behind → wait on change notification or watermark (no tight polling), bounded [L: 300–800 ms]
│   │   ├── Still behind → return record with consistency status PENDING + expected version
│   │   │                  (or 202-style "processing" for creation) — not a generic primary read
│   │   └── Missing record + valid token → PENDING (never 404 for just-created aggregates)
│   │
│   ├── 7.2 List/search with token
│   │   ├── Check per-partition watermark ≥ token position (and refresh-aware for search)
│   │   ├── Behind → bounded wait → else return results flagged INCOMPLETE + client-side merge of
│   │   │            known pending item (from command response) at the top
│   │   └── Never force index refresh per request (indexing cost) [L]
│   │
│   ├── 7.3 Waiting implementation
│   │   ├── Async waits (no thread per waiting request); max concurrent waiters per instance
│   │   ├── Waiter cap reached → immediate PENDING (load shedding)
│   │   └── Watermark lag already beyond wait budget → skip waiting entirely (fast PENDING)
│   │
│   ├── 7.4 Caches in front of views (HTTP/CDN/Redis)
│   │   ├── Token present → bypass shared caches or include version in cache key
│   │   └── Private responses never cached in shared caches
│   │
│   ├── F7.1 Tight polling per request — 20× read amplification during lag — OUTAGE of view store [C]
│   ├── F7.2 Thread-per-wait — thread pool exhaustion under lag — query OUTAGE [C]
│   ├── F7.3 404 for just-created aggregate — client shows "not found", user retries create — duplicates [C]
│   ├── F7.4 CDN serves stale page despite token — RYW broken — CORRUPT UX [C]
│   └── F7.5 Waiting while lag is minutes — every request waits full budget — latency collapse [C]
│       └── Handling: 7.3 skip-wait when watermark lag exceeds budget
│
├── 8. AUTHORITATIVE-OWN READS  (C-AUTHR — the only strongly consistent read path)
│   ├── 8.1 Scope: caller's own aggregate(s) named in a valid token, specific endpoints only (e.g., payment confirmation)
│   ├── 8.2 Implementation: write-side service reads primary/synchronous replica, projects via shared projection library
│   │                        (same response shape, CQ-11)
│   ├── 8.3 Protection: per-principal rate limit; global concurrency cap; circuit breaker on write-model latency;
│   │                   disabled automatically when write model is degraded
│   ├── 8.4 Never used for lists/search, reporting, or anonymous traffic
│   │
│   ├── F8.1 Generic fallback to primary for all timed-out reads — lag cascades into write OUTAGE [C]
│   ├── F8.2 Fallback response shape differs from view — client parsing errors / inconsistent fields — CORRUPT [C]
│   └── F8.3 Fallback bypasses tenant/authorization filters present on view path — SECURITY [C]
│       └── Handling: same read-time authorization (CQ-10) on every path
│
├── 9. DEGRADED-MODE BEHAVIOR
│   ├── 9.1 Propagation stalled (relay, Kafka, projector, search) →
│   │   ├── Commands continue (outbox absorbs) as long as WAL headroom allows (outbox tree)
│   │   ├── Tokened reads return PENDING fast (7.3); UI shows "processing" states
│   │   ├── C-AUTHR limited to T0 confirmation screens within its caps
│   │   └── Status page / banners for extended lag
│   ├── 9.2 Write model degraded →
│   │   ├── Commands shed by priority at gateway (backpressure tree §12)
│   │   ├── Queries unaffected (views independent)
│   │   └── C-AUTHR disabled automatically
│   ├── 9.3 View store degraded →
│   │   ├── Queries degrade (cached, partial with flags, or error) — never redirected to write model
│   │   └── Commands unaffected
│   └── F9.1 Read path failure redirected to write model "temporarily" — combined outage [C]
│
├── 10. AUTHORIZATION IN CQRS
│   ├── 10.1 Read-time decisions (CQ-10): view rows carry tenant/owner/resource attributes; C-QRY asks C-AUTH
│   ├── 10.2 Revocation SLO [G: e.g., ≤ 60 s]; permission cache TTL ≤ SLO; revocation push invalidates caches
│   ├── 10.3 Search: authorization filters applied in query (tenant + resource ACL attributes), plus post-filter verification
│   ├── 10.4 Commands: fresh authorization per command (no long-lived cached grants for mutations)
│   │
│   ├── F10.1 Permissions projected into views — revoked users see data until projection catches up — SECURITY [C]
│   ├── F10.2 Search aggregations computed over unauthorized documents — counts leak — SECURITY [C]
│   └── F10.3 Token used after principal's access revoked — read allowed via token path — SECURITY [C]
│       └── Handling: token never grants access; authorization evaluated independently
│
├── 11. API EVOLUTION & VIEW REBUILDS
│   ├── 11.1 Tokens reference aggregate versions (stable across view rebuilds) — not view-internal sequences
│   ├── 11.2 During cutover: resolver checks the view currently serving the endpoint (cutover controller state)
│   ├── 11.3 Query API versions map to view versions; breaking view changes → new API version or compatible fields
│   └── F11.1 Token minted against old view's internal sequence — meaningless after rebuild — wrong resolution [C]
│
├── 12. MULTI-REGION
│   ├── 12.1 Commands routed to the aggregate's home region (single writer)
│   ├── 12.2 Tokens carry region; reads in another region either wait for replicated watermark or return PENDING
│   ├── 12.3 C-AUTHR only in home region; cross-region authoritative reads disabled (latency, coupling)
│   └── F12.1 Read in region B immediately after write in region A — stale or cross-region fallback storm [C]
│
├── 13. OBSERVABILITY
│   ├── Commands: outcome mix (COMPLETED/REJECTED/CONFLICT/ACCEPTED), idempotent replays, key mismatches, latency
│   ├── Conflicts: rate per aggregate type; hot aggregates
│   ├── Tokens: issued, verified, invalid/expired (security signal), resolution path (immediate/waited/PENDING/C-AUTHR)
│   ├── Wait time distribution; waiter caps hit; skip-wait activations
│   ├── C-AUTHR: rate, rejections by limit/breaker, write-model latency impact
│   ├── Freshness per view (materialized views tree §9.2); PENDING ratio per endpoint
│   ├── Authorization: revocation propagation time; denied reads after revocation
│   └── Absence alerts on all of the above
│
├── 14. RUNBOOKS
│   ├── RB-01 PENDING ratio spike → check propagation hops → fix lagging hop → no fallback expansion
│   ├── RB-02 Duplicate commands reported → verify idempotency key coverage per client → fix client/gateway enforcement
│   ├── RB-03 Conflict storm → identify hot aggregates → server-side retry for commutative ops or re-model
│   ├── RB-04 Invalid token surge → security investigation (forgery/replay) → rotate signing keys if needed
│   ├── RB-05 Write-model latency from C-AUTHR → tighten caps / disable C-AUTHR → communicate degraded UX
│   ├── RB-06 Revocation SLO breach → invalidate permission caches → investigate push path
│   └── RB-07 Regional failover → commands re-homed → tokens from failed region ignored → reconciliation
│
└── 15. GO-LIVE TEST MATRIX
    ├── Command retried with same key after timeout → single effect, identical response
    ├── Same key, different payload → rejected
    ├── Concurrent edits with expected versions → one succeeds, other gets CONFLICT
    ├── Uniqueness under concurrent creates → constraint holds
    ├── Command validation never touches view stores (credential test)
    ├── Projector stalled 10 min → tokened reads return PENDING fast; write-model load unchanged
    ├── Forged/inflated token → ignored, security metric increments, no fallback triggered
    ├── Just-created aggregate read with token → PENDING, never 404
    ├── List after create with token → INCOMPLETE flag + pending item merged; no forced refresh
    ├── 10k concurrent waiting requests → waiter cap enforced; no thread exhaustion
    ├── CDN in path → tokened requests bypass shared cache
    ├── C-AUTHR under write-model degradation → breaker opens, PENDING returned
    ├── Permission revoked → reads denied within revocation SLO regardless of view lag
    ├── View rebuild cutover → tokens still resolve correctly
    ├── Cross-region read after write → PENDING or replicated watermark wait; no cross-region primary reads
    └── Metrics pipeline down → absence alerts fire
```

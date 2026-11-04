# Backpressure — Business Logic Decision Tree

```
BACKPRESSURE — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. BACKPRESSURE CHAIN MAP  (every hop has a buffer, a limit, and a failure mode when full)
│   ├── H1 Edge / API gateway        → buffer: none (request) → full: latency, timeouts → control: admission, 429
│   ├── H2 Producer service + DB     → buffer: connection pool, locks → full: API latency → control: pool limits
│   ├── H3 Outbox table / WAL        → buffer: WAL + slot → full: DISK EXHAUSTION → control: relay health, alerts
│   ├── H4 Relay (CDC / poller)      → buffer: memory, producer batches → full: WAL growth upstream
│   ├── H5 Kafka brokers             → buffer: log (retention-bounded) → full: retention LOSS / disk → control: quotas, capacity
│   ├── H6 Consumer client prefetch  → buffer: fetch queues → full: OOM → control: fetch limits, pause
│   ├── H7 Consumer lanes            → buffer: lane queues → full: memory, latency → control: bounded queues, pause
│   ├── H8 Sink (PostgreSQL / OpenSearch / Redis / external APIs) → buffer: pools, queues → full: latency, rejections
│   │                                → control: adaptive concurrency, bulkheads, rate limits
│   ├── H9 Consumer's own outbox     → feeds the next service's chain (pressure propagates downstream)
│   └── Principle: absorb in Kafka (durable, bounded by retention); reject at the edge (cheap);
│                  never absorb in memory; never let pressure land silently on WAL
│
├── 1. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-ACL  Adaptive concurrency limiter per (handler, dependency)
│   ├── C-BH   Bulkheads — dedicated pools/lanes per dependency (DB pool, search client, API client)
│   ├── C-PC   Pause controller (on poll thread) — receives pause/resume intents with reasons
│   ├── C-MEM  Memory governor — byte budgets, live-heap measurement, runtime memory limit
│   ├── C-CB   Circuit breakers (retry tree §6) — hard failures; backpressure handles slowness
│   ├── C-AS   Autoscaler — capacity-aware, backpressure-aware
│   ├── C-ADM  Edge admission control — priority shedding, retry-after
│   └── C-MON  Monitoring — queueing delay, limits, pauses by reason, headroom
│
├── 2. INVARIANTS  (with enforcement)
│   ├── BP-01 Memory is bounded by bytes, not counts [C]
│   │   └── Enforced by: byte budgets on fetch, lane queues, batches; runtime memory limit set
│   ├── BP-02 Backpressure is scoped to the saturated resource [C]
│   │   └── Enforced by: handler-dependency map; pauses carry dependency reason; bulkheads
│   ├── BP-03 Every pause has a reason, an owner, and a maximum duration [C]
│   │   └── Enforced by: pause controller rejects reasonless pauses; escalation timer per pause
│   ├── BP-04 Resume is gradual [C]
│   │   └── Enforced by: concurrency ramp (slow start) after resume; never full burst
│   ├── BP-05 Scaling never adds load to a saturated dependency [C]
│   │   └── Enforced by: autoscaler ignores backpressure/breaker-induced lag; connection budget check
│   ├── BP-06 Total connections ≤ dependency capacity at max scale [C]
│   │   └── Enforced by: replicas × pool size ≤ DB connection budget (validated at deploy); pooler where needed
│   ├── BP-07 Kafka buffering is bounded by retention headroom [C]
│   │   └── Enforced by: pause duration alerts vs retention (offset tree AT-RISK)
│   ├── BP-08 Pressure never lands silently on WAL [C]
│   │   └── Enforced by: relay lag + slot lag alerts tied to backpressure incidents (outbox tree §2)
│   ├── BP-09 Ordering classes decide what may be shed or coalesced [C]
│   │   └── Enforced by: O-STRICT never shed/coalesced; O-STATE may coalesce; O-NONE T2 may sample
│   ├── BP-10 Priority isolation is physical, not logical [C]
│   │   └── Enforced by: T0 and T2 never share partitions, consumer groups, pools, or nodes' critical resources
│   ├── BP-11 Consumer client calls happen on the poll thread only [C]
│   │   └── Enforced by: pause/resume/seek/commit routed as commands to poll thread
│   └── BP-12 Backpressure is observable as a state, not inferred from lag [C]
│       └── Enforced by: explicit "throttled/paused by reason" metrics per partition and handler
│
├── 3. SATURATION SIGNALS
│   ├── 3.1 Primary signals (use these)
│   │   ├── Queueing delay: time a record waits in lane queue before execution (rising = saturation)
│   │   ├── Dependency latency vs own baseline (e.g., p99 > 2× trailing 1h baseline) [L]
│   │   ├── Explicit rejections: 429, bulk rejections, pool acquisition timeouts, lock timeouts
│   │   ├── In-flight requests per dependency vs current limit (C-ACL)
│   │   └── Memory: live heap / in-use bytes vs runtime limit (not RSS)
│   │
│   ├── 3.2 Secondary signals (diagnostic only)
│   │   ├── Worker utilization (high utilization is healthy; don't trigger on it)
│   │   ├── CPU usage and container CPU throttling
│   │   └── Record lag (consequence, not cause)
│   │
│   ├── F3.1 Utilization threshold triggers — flapping pause/resume [C]
│   │   └── Handling: queueing delay + latency gradient; hysteresis
│   │
│   ├── F3.2 RSS as recovery signal — never resumes [L]
│   │   ├── Impact: runtime keeps freed memory; RSS stays high; consumer stalls forever
│   │   └── Handling: live-heap metrics; runtime memory limit (Go memory limit / JVM heap sizing)
│   │
│   ├── F3.3 Absolute latency thresholds (250 ms) across different dependencies — wrong for most [C]
│   │   └── Handling: per-dependency baselines and SLOs
│   │
│   └── F3.4 Signal from a hot key misread as global saturation — whole consumer throttled [C]
│       └── Handling: per-lane / per-key queueing delay; hot-key detection; throttle the lane, not the instance
│
├── 4. CONTROL MODEL
│   ├── 4.1 Layer A — adaptive concurrency per (handler, dependency) [L]
│   │   ├── Increase limit additively while latency stays near baseline and no rejections
│   │   ├── Decrease multiplicatively on rejections or latency gradient breach
│   │   ├── Floor ≥ 1 (progress always possible); ceiling ≤ bulkhead pool size
│   │   └── Effect: throughput self-tunes to dependency capacity; most slowdowns never need pausing
│   │
│   ├── 4.2 Layer B — partition pause (when Layer A floor reached and queues still growing)
│   │   ├── Pause only partitions whose handlers depend on the saturated resource
│   │   ├── Pause triggered by lane queue byte budget high watermark [L: 80%]
│   │   ├── Resume at low watermark [L: 40%] AND dependency healthy for ≥ 30 s
│   │   └── Minimum pause duration [L: 5 s] to avoid flapping
│   │
│   ├── 4.3 Layer C — circuit breaker (hard failure; retry tree §6)
│   │   └── Slowness → A/B; errors/timeouts at threshold → C
│   │
│   ├── 4.4 Resume ramp (slow start)
│   │   ├── Resume with concurrency limit at 25% of pre-pause value [L]
│   │   ├── Ramp up per Layer A rules
│   │   └── Catch-up backlog drained at controlled rate (dependency protected)
│   │
│   ├── 4.5 Maximum pause duration & escalation [L]
│   │   ├── 2 min → warning (T0/T1)
│   │   ├── 10 min → page
│   │   ├── 25% of retention → incident (LOSS horizon)
│   │   └── Pause never silently permanent (BP-03)
│   │
│   ├── F4.1 Resume requires "all subsystems recovered" — deadlock on one sticky metric [C]
│   │   └── Handling: per-reason pauses; each reason resolves independently; max duration escalation
│   │
│   ├── F4.2 Full-speed resume — immediate re-saturation, oscillation [C]
│   │   └── Handling: 4.4 ramp
│   │
│   ├── F4.3 Fixed concurrency tuned for normal day — collapses under contention [C]
│   │   └── Handling: Layer A adaptive limits
│   │
│   └── F4.4 More concurrency makes contended DB slower (retrograde scaling) — throughput collapse [C]
│       └── Handling: Layer A decreases on latency gradient, discovering the optimal concurrency
│
├── 5. CONSUMER CLIENT MECHANICS
│   ├── 5.1 Pause/resume issued only by poll thread (BP-11); workers send intents with reason
│   ├── 5.2 Poll continues while paused (membership + heartbeats + poll interval) [C]
│   ├── 5.3 Prefetch limits set in bytes, considering partitions owned
│   │   ├── Total fetch bytes cap per poll
│   │   ├── Per-partition fetch cap
│   │   └── Client-side queued prefetch caps (library-specific; verify units and scope per partition vs total) [L]
│   │
│   ├── 5.4 Behavior of already-fetched records on pause is library-specific [L]
│   │   └── Verify: retained and returned after resume, discarded and re-fetched, or delivered anyway
│   │
│   ├── F5.1 Pause called from worker thread — client corruption / exceptions [C]
│   │   └── Handling: 5.1
│   │
│   ├── F5.2 Prefetch queues scale with partition count — OOM before pause triggers [L]
│   │   ├── Impact: assignment of many partitions (rebalance, scale-in) multiplies prefetch memory
│   │   └── Handling: byte caps sized for max partitions per instance; test with max assignment
│   │
│   ├── F5.3 Pause state lost on rebalance — new owner floods saturated dependency [C]
│   │   └── Handling: new owner starts at ramp floor and consults limiter/breaker state (consumer tree §8 Step 4)
│   │
│   ├── F5.4 Paused partitions forgotten (resume never issued) — silent stall [C]
│   │   └── Handling: BP-03 reasons + max duration; watchdog checks pause registry
│   │
│   └── F5.5 Pausing doesn't reduce memory held by in-flight records — still OOM [C]
│       └── Handling: lane queue byte budgets (BP-01) — pause early enough to leave headroom
│
├── 6. MEMORY PROTECTION
│   ├── 6.1 Budgets in bytes (BP-01)
│   │   ├── Fetch buffers + lane queues + in-flight records + batch builders + runtime overhead ≤ memory limit
│   │   └── Large messages counted by size; claim-check above threshold (DLQ tree F4.3)
│   │
│   ├── 6.2 Runtime memory limit
│   │   ├── Go: soft memory limit below container limit (GC works harder before OOM) [L]
│   │   ├── JVM: heap max below container limit with headroom for off-heap/native buffers [L]
│   │   └── Native client buffers (librdkafka) are outside the managed heap — budget separately [L]
│   │
│   ├── F6.1 Count-based channel capacity with variable message sizes — unbounded memory [C]
│   │   └── Handling: byte-based queues
│   │
│   ├── F6.2 Unbounded goroutines/threads per record or per request — memory + scheduler collapse [C]
│   │   └── Handling: fixed lanes; bounded executor per dependency
│   │
│   ├── F6.3 GC death spiral near memory limit — CPU burned on GC → latency → more queueing → more memory [C]
│   │   ├── Handling: pause at memory budget well below limit [L: 70% of soft limit]
│   │   └── Detection: GC CPU fraction; GC frequency
│   │
│   └── F6.4 OOM crash loop — restart → rebalance → same assignment → OOM [C]
│       └── Handling: budgets validated at max assignment; crash-loop detection pages; reduce fetch caps
│
├── 7. DOWNSTREAM-SPECIFIC BEHAVIOR
│   ├── 7.1 PostgreSQL sink
│   │   ├── Signals: pool acquisition wait, lock waits, statement latency, deadlocks
│   │   ├── Connection budget: replicas × pool ≤ allotted share of max connections (BP-06)
│   │   ├── F7.1.1 Connection explosion on scale-out — DB refuses connections — OUTAGE [C]
│   │   │   └── Handling: BP-06; server-side pooler for many small clients; autoscaler connection check
│   │   ├── F7.1.2 Contention on hot rows (aggregate counters) — latency regardless of load [C]
│   │   │   └── Handling: per-key lanes already serialize; reduce cross-key contention; schema design
│   │   └── F7.1.3 Autovacuum / checkpoint spikes misread as overload — over-throttling [L]
│   │       └── Handling: latency gradient over windows; don't react to single spikes
│   │
│   ├── 7.2 OpenSearch sink
│   │   ├── Signals: bulk rejections (queue full), indexing latency, refresh/merge pressure
│   │   ├── Bulk sizing in bytes [L: 5–15 MB per bulk]; concurrency adaptive
│   │   ├── F7.2.1 Partial bulk failures treated as success — LOSS [C]
│   │   │   └── Handling: per-item result inspection; failed items retried individually (version-guarded)
│   │   └── F7.2.2 Rejections retried immediately — amplification [C]
│   │       └── Handling: Layer A decrease + jittered backoff
│   │
│   ├── 7.3 External APIs
│   │   ├── Token bucket per dependency matching contracted rate limits
│   │   ├── Honor retry-after; reduce concurrency
│   │   └── F7.3.1 Rate limit shared across services (same partner credential) — mutual starvation [C]
│   │       └── Handling: central rate allocation per credential; documented quotas per service
│   │
│   └── 7.4 Redis / caches
│       └── F7.4.1 Cache slowdown blocks primary path — DEGRADE [C]
│           └── Handling: cache calls with strict timeouts; bypass on degradation
│
├── 8. PRIORITY, FAIRNESS & ISOLATION
│   ├── 8.1 Priority is physical (BP-10)
│   │   ├── T0 streams: own topics, consumer groups, pools, node pools / priority class
│   │   ├── T2 (telemetry, analytics): separate everything; first to be shed
│   │   └── Priority cannot be implemented inside one partition (order is fixed)
│   │
│   ├── 8.2 Tenant fairness
│   │   ├── Per-tenant concurrency caps in lanes (where tenant identifiable)
│   │   ├── Per-tenant ingestion quotas at edge
│   │   └── Noisy tenant → tenant-level throttling, not global
│   │
│   ├── F8.1 T0 and T2 share a DB pool — telemetry burst starves payments [C]
│   │   └── Handling: bulkheads per tier
│   │
│   ├── F8.2 One tenant's bulk import saturates shared partitions — DEGRADE others [C]
│   │   └── Handling: bulk lanes/topics separate; edge quotas per tenant
│   │
│   └── F8.3 Kubernetes node pressure evicts T0 pods first — OUTAGE [C]
│       └── Handling: priority classes; guaranteed QoS for T0 consumers
│
├── 9. CATCH-UP, SHEDDING & COALESCING  (by ordering class — BP-09)
│   ├── 9.1 O-STRICT (delta, ledgers): never shed, never coalesce; catch up at controlled rate
│   ├── 9.2 O-STATE (full-state projections): coalescing allowed when behind
│   │   ├── Within a lane, if multiple versions of a key are queued, apply only the latest
│   │   │   (version guard accepts strictly greater; skipped versions recorded as coalesced)
│   │   ├── Only when the handler has no per-version side effects
│   │   └── Big catch-up speedup after outages
│   ├── 9.3 O-NONE T2 (telemetry): sampling or dropping allowed under declared policy, with counters
│   ├── 9.4 Deadline-aware: events past valid_until → EXPIRED disposition (retry tree §9.4), not processed late
│   │
│   ├── F9.1 Coalescing on delta stream — CORRUPT [C]
│   │   └── Handling: config validation per ordering class
│   │
│   ├── F9.2 Coalescing skips versions that trigger side effects (notifications per status) — LOSS of effects [C]
│   │   └── Handling: handlers with per-version effects declared non-coalescable
│   │
│   └── F9.3 Silent drop of T2 data — analytics wrong without anyone knowing [C]
│       └── Handling: dropped/sampled counters exported; downstream consumers informed via metadata
│
├── 10. AUTOSCALING INTERPLAY
│   ├── 10.1 Scale on lag in time ONLY when partitions are not paused/throttled for dependency reasons
│   ├── 10.2 Scale-out precondition: dependency headroom available (connection budget + latency healthy)
│   ├── 10.3 Ceiling: partitions; at ceiling → capacity incident (lanes, handler efficiency, partitioning plan)
│   ├── 10.4 Cooldowns and step limits (consumer tree §11.2)
│   │
│   ├── F10.1 Feedback loop: pause → lag → scale → more DB load → more pause — OUTAGE [C]
│   │   └── Handling: 10.1 + 10.2 (BP-05)
│   │
│   ├── F10.2 Total lag across partitions hides hot partition — scaling doesn't help [C]
│   │   └── Handling: per-partition evaluation; hot partition = design issue, not scale issue
│   │
│   ├── F10.3 Scaling event triggers rebalance during peak — throughput dip [C]
│   │   └── Handling: cooperative/new protocol; static membership where suitable; scale earlier on trend
│   │
│   └── F10.4 Scale-in during catch-up — backlog grows again [L]
│       └── Handling: scale-in only when time lag at baseline for sustained window
│
├── 11. KAFKA AS THE BUFFER
│   ├── 11.1 Capacity: retention × produce rate within disk with headroom; tiered storage if used
│   ├── 11.2 Retention headroom tracked per consumer group (offset tree AT-RISK)
│   ├── 11.3 Broker quotas
│   │   ├── Byte-rate and request-rate quotas per user / client id [C]
│   │   ├── Enforcement by delaying responses (client observes throttle time, not errors) [C]
│   │   ├── Connection creation rate limits can be per IP [C]
│   │   └── Quotas protect brokers; they push pressure back to producers (and to WAL for relays)
│   │
│   ├── F11.1 Relay throttled by quota or slow brokers — WAL growth — OUTAGE at producer DB [C]
│   │   └── Handling: BP-08; relay quota sized above peak; slot lag alerts linked to broker throttle metrics
│   │
│   ├── F11.2 Broker disk full — produce failures across all topics — OUTAGE [C]
│   │   └── Handling: disk alerts; retention sized; tiered storage; per-topic retention review
│   │
│   └── F11.3 Consumers fetching old data during catch-up evict page cache — producer latency rises [L]
│       └── Handling: catch-up rate limits; fetch-from-follower for catch-up consumers where possible
│
├── 12. EDGE & PRODUCER-SIDE ADMISSION
│   ├── 12.1 Admission control at gateway
│   │   ├── Priority classes per route: T0 transactional > T1 > T2 telemetry
│   │   ├── Shed lowest priority first with 429 + retry-after
│   │   └── Concurrency limits per upstream (adaptive), not only rate limits
│   │
│   ├── 12.2 End-to-end health propagation [L]
│   │   ├── If pipeline headroom is critical (WAL slot lag, retention headroom), signal edge to shed T2 earlier
│   │   └── T0: business decision — accept and queue (durable, delayed processing communicated)
│   │       or reject new commands (protect consistency); decided in advance per flow
│   │
│   ├── F12.1 Clients retry 429 without jitter / ignore retry-after — retry storm [C]
│   │   └── Handling: retry-after mandatory; client SDK enforces jitter; token bucket per client
│   │
│   ├── F12.2 Shedding at edge doesn't relieve the consumer bottleneck (different traffic) — false relief [C]
│   │   └── Handling: map which edge routes feed which saturated consumers; shed relevant routes
│   │
│   └── F12.3 Synchronous producer waits on throttled Kafka in request path — API latency collapse [C]
│       └── Handling: outbox pattern (producer never produces to Kafka in request path)
│
├── 13. INTERACTIONS WITH OTHER TREES
│   ├── Outbox: relay slowdowns → WAL; backpressure incidents check slot lag first
│   ├── Retry: breakers handle failure; backpressure handles slowness; both pause with distinct reasons
│   ├── DLQ: saturation is ENVIRONMENTAL, never quarantined
│   ├── Ordering: pausing preserves order; coalescing only for O-STATE
│   ├── Consumer groups: poll thread separation; pause state on assignment; autoscaler policy
│   └── Offsets: pause duration vs retention headroom; lag evaluation excludes paused-by-reason
│
├── 14. OBSERVABILITY
│   ├── Per (handler, dependency): concurrency limit, in-flight, queueing delay, latency vs baseline, rejections
│   ├── Paused partitions by reason; pause durations; escalations
│   ├── Memory: live heap vs limit, native buffer usage, GC CPU fraction, OOM kills
│   ├── Prefetch bytes per instance; lane queue bytes
│   ├── Connection pool: size, waiters, acquisition time; DB total connections vs budget
│   ├── Coalesced / sampled / expired counts by handler
│   ├── Autoscaler decisions with reasons (including "skipped: dependency saturated")
│   ├── Broker throttle time for relays and producers
│   ├── End-to-end: WAL slot lag, retention headroom, time lag
│   └── Absence alerts on all of the above
│
├── 15. RUNBOOKS
│   ├── RB-01 Dependency slowdown → confirm limiter decreased → confirm scoped pauses → freeze autoscaler → engage dependency owner
│   ├── RB-02 Memory pressure → check prefetch vs lane bytes → reduce fetch caps → restart with ramp if needed
│   ├── RB-03 Autoscaler feedback loop → freeze scaling → scale back to safe replica count → verify connection budget
│   ├── RB-04 Prolonged pause near retention → extend retention → shed T2 → incident escalation
│   ├── RB-05 Relay throttled / WAL growth → raise relay quota or broker capacity → monitor slot lag
│   ├── RB-06 Catch-up after outage → ramped resume → enable coalescing for O-STATE → monitor dependency
│   └── RB-07 Noisy tenant → tenant throttle at edge and lanes → communicate → review quotas
│
└── 16. GO-LIVE TEST MATRIX  (load tests under production-like data sizes)
    ├── Sink DB latency ×5 for 20 min → limiter adapts, scoped pauses, no OOM, no scale-out, no DLQ
    ├── OpenSearch bulk rejections → per-item retries, adaptive bulk concurrency, no loss
    ├── 10× burst for 30 min → Kafka absorbs, time lag within declared catch-up SLO, connections within budget
    ├── Max partition assignment on one instance with max message sizes → memory within budget
    ├── Rebalance during pause → new owner starts at ramp floor, dependency not flooded
    ├── Recovery after outage → ramped resume, no oscillation (pause/resume count bounded)
    ├── Hot key flood → only that lane throttled; other keys' latency unchanged
    ├── T2 telemetry flood → T0 latency unchanged (isolation proof)
    ├── Relay throttled by broker quota → slot lag alerts fire before disk threshold
    ├── Autoscaler with saturated dependency → scale-out suppressed, reason logged
    ├── O-STATE coalescing during catch-up → final state equals non-coalesced replay
    ├── Clients hammered with 429 → retry-after honored, no retry storm
    └── Metrics pipeline down → absence alerts fire
```

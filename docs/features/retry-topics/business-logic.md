# Retry Topics — Business Logic Decision Tree

```
RETRY HANDLING — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-RT   Consumer runtime — record loop, classifier, in-place retry, breaker, held-key drain
│   │   ├── C-CB   Circuit breakers — one per external dependency, shared view per instance
│   │   ├── C-QS   Quarantine/held store (DLQ tree) — attempts, held keys, RETRY_SCHEDULED items, offsets, epochs
│   │   ├── C-SCH  Retry scheduler (inside C-RT) — scans owned held keys whose due time passed
│   │   ├── C-RTP  Retry topics (OPTIONAL; unordered streams only) — per (topic, group, tier)
│   │   ├── C-DLQ  Quarantine path (DLQ tree) — terminal for exhausted / non-retryable
│   │   └── C-MON  Monitoring — retry depth, lateness, budget burn, breaker state, retry-induced reorder
│   │
│   ├── 0.2 Stream ordering classes (declared per (topic, handler); drives everything below)
│   │   ├── O-STRICT  Ordered per aggregate, DELTA semantics (balances, ledgers, inventory counts) — T0/T1
│   │   ├── O-STATE   Ordered per aggregate, full-STATE overwrite with strict version guard (projections, search docs)
│   │   └── O-NONE    No ordering contract (notifications, analytics, independent commands)
│   │
│   └── 0.3 Ownership
│       ├── Consumer team: classification rules, retry policy per handler, budgets
│       ├── Platform: retry library, scheduler, breaker, retry topic provisioning, dashboards
│       └── Dependency owners: published SLOs, retry-after behavior, rate limits
│
├── 1. INVARIANTS  (with enforcement)
│   ├── RT-01 Ordering is never traded for throughput on ordered streams [C]
│   │   └── Enforced by: O-STRICT / O-STATE use held-key scheduled retry (§7); retry topics forbidden by config validation
│   ├── RT-02 Environment failures pause, they don't retry-route [C]
│   │   └── Enforced by: classifier + breaker; environmental errors never consume message retry budget
│   ├── RT-03 Attempt state is durable and keyed by event identity [C]
│   │   └── Enforced by: attempt_record in C-QS keyed by (group, handler, event_id); headers are advisory only
│   ├── RT-04 No sleeping in the poll loop [C]
│   │   └── Enforced by: library offers only pause/resume + scheduled wakeups; static check bans blocking sleeps in handlers
│   ├── RT-05 Retry scope = (topic, consumer group, handler) [C]
│   │   └── Enforced by: naming + ACLs; a group can only produce/consume its own retry artifacts
│   ├── RT-06 Total retry amplification is bounded [C]
│   │   └── Enforced by: single retry layer owns retries; client libraries' internal retries capped and counted
│   ├── RT-07 Retries never outlive business validity [C]
│   │   └── Enforced by: event deadline (valid_until) checked before every attempt → EXPIRED disposition
│   ├── RT-08 Every retry of a side effect reuses the same idempotency key [C]
│   │   └── Enforced by: key derived from (handler, event_id, effect name); never regenerated per attempt
│   ├── RT-09 Retry state and offsets advance atomically [C]
│   │   └── Enforced by: scheduling a retry = held-key write + offset advance in one C-QS transaction
│   ├── RT-10 Retry is observable as latency AND correctness [C]
│   │   └── Enforced by: lateness, budget burn, and retry-induced stale/gap counters all alerting
│   └── RT-11 Recovery is jittered [C]
│       └── Enforced by: jitter on every backoff and on breaker close; release rate caps
│
├── 2. RETRY STRATEGY SELECTION  (decision per failed record)
│   ├── D1 Classify the failure (§3)
│   │   ├── ENVIRONMENTAL      → §6 breaker/pause (all ordering classes)
│   │   ├── CONTENTION         → §5 in-place immediate retry (lock wait, deadlock, serialization failure)
│   │   ├── RATE_LIMITED       → §6.4 honor retry-after; not a failure
│   │   ├── AMBIGUOUS_EFFECT   → §10 retry with same idempotency key; never new key
│   │   ├── CAUSAL_WAIT        → ordering tree §11 causal hold (dependency not yet seen)
│   │   ├── MESSAGE_TRANSIENT  → D2 (message-specific, may succeed later)
│   │   ├── PERMANENT_DATA     → DLQ quarantine (C1/C2) + key hold
│   │   ├── BUSINESS_REJECT    → DLQ quarantine (C3) + key hold + business owner
│   │   ├── HANDLER_BUG        → DLQ quarantine (C4) + key hold
│   │   └── CRASH_POISON       → DLQ C6 (detected at pickup via persisted attempts)
│   │
│   ├── D2 MESSAGE_TRANSIENT — which mechanism?
│   │   ├── Stream O-STRICT  → §7 held-key scheduled retry (mandatory)
│   │   ├── Stream O-STATE   → §7 held-key scheduled retry (default)
│   │   │                     or §8 retry topics ONLY if strict version guard rejects stale and gaps are tolerated
│   │   └── Stream O-NONE    → §8 retry topics allowed (or §7 for uniformity)
│   │
│   └── D3 Deadline check before scheduling
│       └── valid_until earlier than next attempt time → EXPIRED path (§9.4), not retry
│
├── 3. FAILURE CLASSIFICATION
│   ├── 3.1 Typed error taxonomy (handlers throw typed errors; untyped = HANDLER_BUG)
│   │   ├── ENVIRONMENTAL: connection refused/reset, DNS failure, pool exhausted, 503 from dependency,
│   │   │                  broker unavailable, DB failover in progress, read-only replica
│   │   ├── CONTENTION: lock timeout, deadlock detected, serialization failure, optimistic version conflict
│   │   ├── RATE_LIMITED: 429 / quota exceeded with or without retry-after
│   │   ├── AMBIGUOUS_EFFECT: timeout or connection drop AFTER request sent to a non-idempotent target
│   │   ├── CAUSAL_WAIT: referenced entity not yet present locally (and stream ordering says it may arrive)
│   │   ├── MESSAGE_TRANSIENT: dependency healthy for others but fails for this message
│   │   │                      (e.g., downstream entity temporarily locked, external record not yet propagated)
│   │   ├── PERMANENT_DATA: schema/contract/validation failures
│   │   ├── BUSINESS_REJECT: domain rule refuses (insufficient funds, closed account)
│   │   └── HANDLER_BUG: anything untyped
│   │
│   ├── 3.2 Discriminator for ambiguous errors (timeouts, generic 5xx)
│   │   ├── Same dependency failing broadly in window → ENVIRONMENTAL
│   │   ├── Same dependency succeeding for other records → MESSAGE_TRANSIENT (or MESSAGE defect after budget)
│   │   └── Not enough traffic → ENVIRONMENTAL (bias to pausing, never to quarantine)
│   │
│   ├── F3.1 Missing entity treated as permanent — LOSS/CORRUPT [C]
│   │   ├── Impact: event for payment DLQ'd because order not yet projected; resolves itself seconds later
│   │   └── Handling: CAUSAL_WAIT with bounded durable hold; deadline → treat dependency as gap (ordering tree §8)
│   │
│   ├── F3.2 Business reject retried — wasted budget + delayed decision [C]
│   │   └── Handling: typed BUSINESS_REJECT never retried
│   │
│   ├── F3.3 Contention routed to delay tiers — needless latency [C]
│   │   └── Handling: immediate in-place retry with short jitter
│   │
│   ├── F3.4 Validation error that depends on reference data (currency list, product catalog) — misclassified [C]
│   │   ├── Impact: permanent today, valid after reference data sync
│   │   └── Handling: typed REFERENCE_DATA_MISSING → MESSAGE_TRANSIENT with longer schedule + alert to data owner
│   │
│   └── F3.5 Classifier wrong at scale (deploy bug) — mass misrouting [L]
│       └── Handling: classifier changes tested against stored failure samples; class distribution drift alert
│
├── 4. LAYER R0 — CLIENT / LIBRARY RETRIES  (inside drivers and HTTP clients)
│   ├── 4.1 Rule: exactly one layer owns retry decisions; lower layers do minimal, fast, bounded retries
│   ├── F4.1 Retry amplification — OUTAGE of dependency [C]
│   │   ├── Scenario: HTTP client 3× retries × in-place 3× × tiers 3× × N instances
│   │   ├── Impact: degraded dependency receives 27× load per message → collapses fully
│   │   ├── Handling: client retries ≤ 1 for idempotent reads, 0 for writes; consumer layer owns budget
│   │   └── Detection: dependency request rate vs consumer record rate (amplification ratio)
│   ├── F4.2 Driver auto-retries non-idempotent writes — double effect [C]
│   │   └── Handling: disable automatic write retries in clients; idempotency keys (RT-08)
│   └── F4.3 Hidden timeouts longer than poll interval — group eviction [C]
│       └── Handling: every client timeout < per-record budget < poll interval
│
├── 5. LAYER R1 — IN-PLACE BOUNDED RETRY  (same record, same thread, no topic hop)
│   ├── 5.1 Policy [L]
│   │   ├── CONTENTION: up to 5 attempts, 10–100 ms jittered backoff
│   │   ├── MESSAGE_TRANSIENT first stage: up to 3 attempts, 200 ms → 2 s jittered
│   │   └── Total in-place time per record ≤ 10 s and ≤ 20% of poll interval
│   │
│   ├── 5.2 Mechanics
│   │   ├── Attempt persisted BEFORE execution (crash-safe, DLQ tree DL-15)
│   │   ├── Backoff via partition pause + continued polling + timer resume (RT-04)
│   │   └── Other partitions owned by the instance continue during backoff
│   │
│   ├── F5.1 Pause state lost on rebalance — retry hammering by new owner [C]
│   │   └── Handling: new owner reads attempt_record; respects remaining backoff
│   │
│   ├── F5.2 In-memory attempt count resets on crash — crash-loop poison [C]
│   │   └── Handling: persisted attempts (RT-03)
│   │
│   ├── F5.3 Long in-place retries starve partition — lag [C]
│   │   └── Handling: hard cap; then escalate to §7 scheduled retry (key held, partition flows)
│   │
│   └── F5.4 Contention retries on hot aggregate never succeed — livelock [L]
│       └── Handling: escalate to scheduled retry after cap; hot-key metric; aggregate design review
│
├── 6. LAYER R2 — ENVIRONMENTAL FAILURES: CIRCUIT BREAKER & PAUSE
│   ├── 6.1 Breaker per dependency [L]
│   │   ├── OPEN when ≥ 50% failures in last 20 calls or 5 consecutive timeouts
│   │   ├── OPEN → pause partitions whose handler requires that dependency (not all partitions)
│   │   ├── HALF-OPEN probe every 30 s with jitter; 3 successes → CLOSED
│   │   └── CLOSED → resume with ramp (concurrency/rate caps rising over minutes)
│   │
│   ├── 6.2 Pause governance [L]
│   │   ├── > 15 min (T0/T1) → page
│   │   ├── > 25% of topic retention → incident (loss horizon)
│   │   └── > 50% of dedupe / provider idempotency windows → business owner informed
│   │
│   ├── 6.3 Handler-dependency map
│   │   └── Each handler declares dependencies → pausing is precise (a search outage doesn't stop ledger handler)
│   │
│   ├── 6.4 Rate limiting (429)
│   │   ├── Honor retry-after; reduce concurrency (adaptive limiter)
│   │   └── Not counted as breaker failure; not counted against message budget
│   │
│   ├── F6.1 Outage routed through retry tiers — mass DLQ + mass reorder [C]
│   │   └── Handling: RT-02; this is the most common real-world failure of retry-topic designs
│   │
│   ├── F6.2 Breaker per instance only — inconsistent behavior across fleet [L]
│   │   └── Handling: acceptable if each instance probes independently; fleet view in monitoring
│   │
│   ├── F6.3 Thundering herd on recovery — re-outage [C]
│   │   └── Handling: ramped resume; jittered probe; release caps (RT-11)
│   │
│   ├── F6.4 Dependency partially degraded (slow, not failing) — latency collapse [C]
│   │   └── Handling: breaker also opens on latency SLO breach; bulkhead per dependency
│   │
│   └── F6.5 Pause outlasts retention — LOSS [C]
│       └── Handling: 6.2 escalation; topic retention extension runbook
│
├── 7. LAYER R3a — HELD-KEY SCHEDULED RETRY  (ordered streams; recommended default)
│   ├── 7.1 Concept
│   │   ├── Failed record becomes a held item: reason RETRY_SCHEDULED, due_at, attempt number, backoff tier
│   │   ├── Key is HELD: later versions of the aggregate are appended behind it (ordering preserved)
│   │   ├── Partition keeps flowing for other keys (no head-of-line blocking across keys)
│   │   └── Partition owner's scheduler executes due items through the normal handler path
│   │
│   ├── 7.2 Schedule [L]
│   │   ├── Tiers: 5 s → 30 s → 2 min → 10 min → 30 min (with ±20% jitter)
│   │   ├── Total budget measured in TIME, sized to exceed typical message-specific recovery [G: your data]
│   │   └── Exhausted → DLQ quarantine (same held key; reason changes to PARKED_FAILURE)
│   │
│   ├── 7.3 Transaction boundaries
│   │   ├── Scheduling: insert held item + mark key held + advance offset — one tx (RT-09)
│   │   ├── Execution: lock held key, verify epoch, run handler in tx, on success remove item and drain successors
│   │   └── Failure: update attempt/due_at in same tx; no topic hops anywhere
│   │
│   ├── 7.4 Scheduler mechanics
│   │   ├── Index on (group, partition-owned keys, due_at)
│   │   ├── Scan interval [L: 1 s]; batch size bounded; fair across keys
│   │   ├── Runs only for keys of partitions currently owned (epoch-checked)
│   │   └── Rebalance: new owner picks up due items automatically
│   │
│   ├── F7.1 Scheduler runs on non-owner — concurrent writers [C]
│   │   └── Handling: epoch fencing; ownership check per item
│   │
│   ├── F7.2 Clock skew between instances — early/late retries [L]
│   │   └── Handling: due_at computed and compared using DB clock, not host clocks
│   │
│   ├── F7.3 Many held keys due at once (post-incident) — burst [C]
│   │   └── Handling: release rate cap per instance; jitter at scheduling
│   │
│   ├── F7.4 Held item count grows on hot key — DEGRADE [C]
│   │   └── Handling: per-key cap (DLQ tree F5.3) → escalate → pause partition at hard cap
│   │
│   ├── F7.5 Store unavailable — cannot schedule [C]
│   │   └── Handling: ENVIRONMENTAL → pause; never advance offset
│   │
│   └── F7.6 Scheduled retries hide a dependency outage (MESSAGE_TRANSIENT misclassified) [C]
│       └── Handling: discriminator (§3.2) re-evaluated per attempt; reclassify to ENVIRONMENTAL → breaker
│
├── 8. LAYER R3b — RETRY TOPICS  (O-NONE streams; or O-STATE with strict version guard)
│   ├── 8.1 Topology
│   │   ├── Topics per (main topic, consumer group, tier): "<topic>.<group>.retry.<tier>"
│   │   ├── Same partition count as main; same key preserved
│   │   ├── Durability same as main (RF 3, min ISR 2, no unclean election); delete policy
│   │   ├── Timestamp type LogAppendTime on retry topics (broker clock drives delay)
│   │   └── Retention ≥ tier delay + max retry-consumer outage [L: ≥ 3 days]
│   │
│   ├── 8.2 Routing (router inside C-RT)
│   │   ├── Attempt count read from C-QS attempt_record (headers advisory)
│   │   ├── Publish to next tier with idempotent producer, acks from all ISR
│   │   ├── Preserve: original key/value bytes, original headers, event_id; add: tier, due_at, attempt, class code, sanitized error
│   │   ├── Commit source offset ONLY after publish acknowledged
│   │   └── Crash between publish and commit → duplicate in retry tier → idempotent handler absorbs
│   │
│   ├── 8.3 Delay consumer
│   │   ├── Separate consumer group per tier, same handler code version as main
│   │   ├── Record not yet due → pause that partition, keep polling, resume at due time (no sleep)
│   │   ├── Due → process through normal handler path (inbox, version guard, effects)
│   │   └── Fixed-delay tiers keep due times monotonic per partition → no head-of-line problem within tier
│   │
│   ├── F8.1 Retry topic shared across consumer groups — duplicate processing [C]
│   │   └── Handling: RT-05 naming + ACLs
│   │
│   ├── F8.2 Sleep-based delay in poll thread — group eviction [C]
│   │   └── Handling: 8.3 pause/poll/resume
│   │
│   ├── F8.3 CreateTime copied from original — no delay at all [C]
│   │   └── Handling: LogAppendTime or explicit due_at header computed at publish
│   │
│   ├── F8.4 Used on O-STRICT stream — CORRUPT ordering [C]
│   │   ├── Impact: v5 retried after v6–v9 applied; delta double/missing
│   │   └── Handling: config validation rejects retry topics for O-STRICT handlers (RT-01)
│   │
│   ├── F8.5 O-STATE stream: stale retry overwrites newer state — CORRUPT [C]
│   │   └── Handling: strict-greater version guard at sink (store-enforced); stale → skip + count
│   │
│   ├── F8.6 Retry tier consumer down while main healthy — silent backlog [C]
│   │   └── Handling: lag-in-time alert per tier; absence alerts
│   │
│   ├── F8.7 Retry tier consumer on different code version than main — inconsistent behavior [C]
│   │   └── Handling: deployed as one unit with main consumer; version check at startup
│   │
│   ├── F8.8 Retry topic retention shorter than backlog — LOSS [C]
│   │   └── Handling: 8.1 retention rule; lag-vs-retention alert
│   │
│   ├── F8.9 Topic explosion (topics × groups × tiers) — operational DEGRADE [C]
│   │   └── Handling: max 3 tiers; IaC-provisioned; decommission with handler
│   │
│   ├── F8.10 Error message headers carry PII — COMPLIANCE [C]
│   │   └── Handling: sanitized class + fingerprint only; details in restricted logs
│   │
│   └── F8.11 Redrive from DLQ into main topic resets header count — infinite cycles [C]
│       └── Handling: attempts and redrive counts in C-QS (RT-03); redrive via DLQ tree routes only
│
├── 9. BUDGETS, BACKOFF, JITTER & DEADLINES
│   ├── 9.1 Budget dimensions (all enforced)
│   │   ├── Per record: attempts AND total elapsed time
│   │   ├── Per handler: retry traffic ≤ X% of live traffic (token bucket) [L: 10–20%]
│   │   └── Per dependency: concurrent retries cap (bulkhead)
│   │
│   ├── 9.2 Backoff
│   │   ├── Exponential with full jitter; capped maximum interval
│   │   └── Retry-after from dependency overrides computed backoff
│   │
│   ├── 9.3 Budget exhaustion
│   │   └── → DLQ quarantine with key hold (never drop, never retry forever)
│   │
│   ├── 9.4 Business deadlines (RT-07)
│   │   ├── Events carry valid_until when time-sensitive (OTP, price quote, reservation hold, cutoff windows)
│   │   ├── Before each attempt: expired → disposition EXPIRED (business-defined outcome, recorded)
│   │   └── EXPIRED is not silent: counted, alerted for T0, may trigger compensating business event
│   │
│   ├── F9.1 Budget in attempts only — budget burns in seconds during brief blip [C]
│   │   └── Handling: time-based budget with minimum spacing
│   │
│   ├── F9.2 Synchronized retries (no jitter) — periodic load spikes [C]
│   │   └── Handling: full jitter everywhere
│   │
│   └── F9.3 Retry after business deadline — CORRUPT business outcome [C]
│       ├── Impact: expired price honored, cancelled reservation re-confirmed, payout after cutoff
│       └── Handling: 9.4
│
├── 10. SIDE EFFECTS & AMBIGUOUS OUTCOMES
│   ├── 10.1 Rule: every external effect is idempotent by key (RT-08) or executed via consumer-side outbox
│   ├── F10.1 Timeout after payment request sent — unknown outcome — CORRUPT financial [C]
│   │   ├── Handling: AMBIGUOUS_EFFECT → retry with SAME key; provider returns original result
│   │   └── Unresolvable → UNKNOWN_PARTIAL quarantine → reconcile with provider statement
│   ├── F10.2 Partial multi-effect handler fails midway — retry repeats completed effects [C]
│   │   └── Handling: per-effect idempotency keys; or split into separately recorded steps
│   ├── F10.3 Notification sent on each attempt — duplicate messages to customers [C]
│   │   └── Handling: notification provider idempotency or "sent" record in consumer tx before send via outbox
│   └── F10.4 Provider idempotency window shorter than retry horizon — double effect [C]
│       └── Handling: retry horizon < provider window (DLQ tree DL-09)
│
├── 11. INTERACTIONS WITH OTHER TREES
│   ├── Ordering: held-key retry = same mechanism as gap hold and DLQ parking (one store, stacked reasons)
│   ├── Idempotency: every retry passes inbox + version guard; no bypass
│   ├── DLQ: exhaustion converts RETRY_SCHEDULED → PARKED_FAILURE on the same held key (no re-queue)
│   ├── Versioning: retried events may be older schema versions after deploys → upcast on every attempt
│   └── Outbox: producer-side relay retries are separate (relay tree); never conflated with consumer retry
│
├── 12. CAPACITY & COST  [G — needs your numbers]
│   ├── Inputs: peak rate, MESSAGE_TRANSIENT rate, mean recovery time, hot-key rates
│   ├── Held/scheduled items ≈ transient rate × mean time-to-success
│   ├── Scheduler scan cost ≈ due items/s × handler cost (bounded by release caps)
│   ├── Retry topics (if used): storage ≈ retry rate × tier delay × retention multiplier
│   └── Load test: 5% MESSAGE_TRANSIENT for 1 h + 20-minute dependency outage, live p99 within SLO
│
├── 13. CONFIGURATION & DEPLOYMENT
│   ├── Handler manifest declares: ordering class, dependency list, failure classes, budgets, deadlines, tier
│   ├── Validation at deploy: O-STRICT + retry topics → reject; missing dependency map → reject
│   ├── Retry policy changes are versioned; in-flight held items keep their original schedule
│   ├── Rolling deploys: scheduler runs on new and old versions — handler compatibility (versioning tree)
│   └── Decommission: no held RETRY_SCHEDULED items, retry topics drained → then removed
│
├── 14. OBSERVABILITY
│   ├── Per handler: failure class distribution; in-place retry success rate; scheduled retry success by tier
│   ├── Held items by reason (RETRY_SCHEDULED / CAUSAL_WAIT / PARKED_FAILURE / GAP); oldest due age
│   ├── Lateness: actual execution time − due time (scheduler health)
│   ├── Budget burn: retry traffic % of live; exhaustion rate → DLQ
│   ├── Breaker state per dependency; pause duration per partition; amplification ratio
│   ├── EXPIRED dispositions (T0: page)
│   ├── Retry-induced stale skips and gaps (O-STATE / O-NONE with topics) — must be explained
│   ├── Retry topics (if used): lag in time per tier vs retention
│   └── Absence alerts on all of the above
│
├── 15. RECONCILIATION
│   ├── RC-01 Producer versions vs consumer applied versions (lags explained by held keys)
│   ├── RC-02 Held items whose due time passed > threshold without execution (scheduler stall) → page
│   ├── RC-03 Retry topics: records in tier with no attempt_record (or vice versa) → investigate
│   └── RC-04 External effects vs provider records for handlers with AMBIGUOUS_EFFECT history
│
├── 16. RUNBOOKS
│   ├── RB-01 Dependency outage → breaker open, partitions paused → monitor retention horizon → ramped resume
│   ├── RB-02 Retry exhaustion spike → check classifier drift vs real dependency issue → reclassify/fix → redrive
│   ├── RB-03 Scheduler stall (lateness rising) → check C-QS health, epoch conflicts → restart scheduler
│   ├── RB-04 Amplification detected → cap client retries → lower retry budget → protect dependency
│   ├── RB-05 Expired events (T0) → business owner decision → compensating events
│   └── RB-06 Migration away from retry topics on ordered stream → drain tiers → switch handler to held-key mode → reconcile
│
└── 17. GO-LIVE TEST MATRIX
    ├── 20-minute DB outage → partitions paused, zero DLQ entries, ordered resume, no reorder
    ├── Partner 429 storm → adaptive throttle, no breaker flapping, no DLQ
    ├── Deadlock storm on hot aggregate → in-place retries succeed or escalate to held-key
    ├── MESSAGE_TRANSIENT on v5 with v6–v9 arriving → v6–v9 held, executed in order after v5 succeeds
    ├── Crash during scheduled retry → resumes from persisted attempt and due_at
    ├── Rebalance with due items → new owner executes, old owner fenced
    ├── Payment before order (causal) → CAUSAL_WAIT → applied when order arrives, no DLQ
    ├── Payment timeout with unknown outcome → retry with same key → one charge
    ├── Event past valid_until → EXPIRED disposition, alert, no effect
    ├── Retry topic (O-NONE) consumer down 2 h → lag alert, no loss within retention
    ├── Config: O-STRICT handler with retry topics → deploy rejected
    ├── Thundering herd on recovery → ramped release keeps dependency within capacity
    └── Metrics pipeline down → absence alerts fire
```

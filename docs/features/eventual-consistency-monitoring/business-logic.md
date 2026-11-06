# Eventual Consistency Monitoring — Business Logic Decision Tree

```
EVENTUAL CONSISTENCY MONITORING — v2.1 ADDENDUM: CRITICAL PRODUCTION FAILURE MODES
Legend  : [C] Certain   [L] Likely (verify per version/config)   [G] Guessing
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Failure node format: Scenario → Impact → Handling → Detection
│
├── 15. NEW INVARIANTS  (add to §1)
│   ├── EC-12 Three-valued results: MATCH | MISMATCH | UNKNOWN — UNKNOWN is never counted as MATCH [C]
│   │   └── Enforced by: comparator returns UNKNOWN on any error, timeout, partial result, or stale source
│   ├── EC-13 Coverage is a monitored SLO [C]
│   │   └── Enforced by: % of keyspace verified per window, per view, per partition, per tenant class
│   ├── EC-14 Every registered view has active checks [C]
│   │   └── Enforced by: view registry ↔ monitor config reconciliation; unmonitored view = page
│   ├── EC-15 Alerting has an external dead-man's switch [C]
│   │   └── Enforced by: always-firing heartbeat alert consumed by an independent external service
│   ├── EC-16 Consistency ≠ correctness: business invariants are checked independently of the pipeline [C]
│   │   └── Enforced by: domain assertions on source and views (§20)
│   ├── EC-17 All writes to views outside the pipeline are detected and audited [C]
│   │   └── Enforced by: DB audit of non-pipeline roles; break-glass workflow; drift class MANUAL-WRITE
│   ├── EC-18 Canaries are representative and quarantined from business data [C]
│   │   └── Enforced by: canary coverage matrix (§22); canary exclusion flags in every downstream consumer
│   └── EC-19 Silences and disabled checks expire [C]
│       └── Enforced by: mandatory expiry + owner; expired silences cannot be renewed without review
│
├── 16. MONITOR BLINDNESS  (the monitor or alerting fails silently)
│   ├── F16.1 Alerting pipeline down (metrics store, alert manager, paging integration) — every alert silent — OUTAGE masked [C]
│   │   ├── Impact: real drift, stalled pipelines, and monitor crashes all produce zero pages
│   │   ├── Handling: EC-15 dead-man's switch → external service pages when heartbeat stops
│   │   └── Detection: external service only (internal detection is impossible by definition)
│   │
│   ├── F16.2 Correlated dependency — monitor shares Kafka cluster / DB replica / DNS / KMS / network with pipeline [C]
│   │   ├── Impact: the failure that breaks the pipeline also blinds the monitor; absence alerts may not leave the cluster
│   │   ├── Handling: dependency map for monitor; canary readers and alert egress on independent infrastructure;
│   │   │             monitor in a separate failure domain (zone/cluster) for T0
│   │   └── Detection: chaos tests that fail each shared dependency and verify a page still arrives
│   │
│   ├── F16.3 UNKNOWN counted as MATCH (exception swallowed, timeout = pass) — false green during incidents — LOSS masked [C]
│   │   ├── Handling: EC-12; comparator code reviewed for fail-open paths; UNKNOWN rate is a first-class metric
│   │   └── Detection: alert when UNKNOWN ratio > threshold [L: 5%] or coverage drops (EC-13)
│   │
│   ├── F16.4 Coverage decay — sweeps take longer than their interval as data grows — never complete — drift ages invisibly [C]
│   │   ├── Handling: sweep duration vs interval tracked; parallelize/shard sweeps; alert on incomplete sweeps
│   │   └── Detection: "last full sweep completed" age per view
│   │
│   ├── F16.5 Sampling misses concentrated drift (one partition, one tenant, one shard) — late detection [C]
│   │   ├── Handling: stratified sampling — every partition, every tenant class, every shard sampled each window
│   │   └── Detection: per-stratum coverage metrics
│   │
│   ├── F16.6 Checks skipped under load (timeouts at peak) — monitoring biased away from the riskiest periods [C]
│   │   ├── Handling: deferred re-check queue for skipped checks; peak-period coverage reported separately
│   │   └── Detection: coverage by hour-of-day
│   │
│   ├── F16.7 New view deployed without checks; check disabled during incident and never re-enabled [C]
│   │   ├── Handling: EC-14 registry reconciliation; EC-19 expiry on disables
│   │   └── Detection: daily "unmonitored views" report → page if any T0/T1
│   │
│   └── F16.8 Drift tracker store full / lost / reset — drift ages restart at zero — stalls hidden [C]
│       ├── Handling: tracker on durable replicated storage; capacity alerts; on reset, mark all drift ages UNKNOWN
│       └── Detection: tracker write errors; sudden drop of max drift age to 0
│
├── 17. MONITOR WRONG  (false green or false red)
│   ├── F17.1 Source replica stopped replicating — monitor compares against frozen data — mass false drift or false match [C]
│   │   ├── Handling: check replica lag/replay position before every comparison batch; lag > bound → UNKNOWN
│   │   └── Detection: replica lag metric consumed by monitor; replica "last replayed" timestamp
│   │
│   ├── F17.2 OpenSearch returns partial results when shards are unavailable (default allows partial results) [C]
│   │   ├── Impact: counts/buckets from partial results → false MISSING (monitor) and silently incomplete
│   │   │           user-facing search (product) — both look "successful"
│   │   ├── Handling: monitor rejects responses with shard failures (UNKNOWN); user-facing queries decide
│   │   │             partial-result policy explicitly; cluster health gate before sweeps
│   │   └── Detection: shard failure counts in responses; cluster health not green
│   │
│   ├── F17.3 Search replica shards lag primary shards — repeated reads disagree — flapping drift [L]
│   │   └── Handling: comparisons read with consistent routing preference; tolerance window
│   │
│   ├── F17.4 Representation mismatch between stores — false drift or masked real drift [C]
│   │   ├── Cases: decimal vs float storage, timestamp precision (micro vs milli), timezone normalization,
│   │   │          key formatting (UUID case/hyphens), trailing whitespace, null vs missing field, enum casing
│   │   ├── Handling: canonicalization rules per field type in the shared comparison library; money compared
│   │   │             in minor units as integers (never as floats)
│   │   └── Risk: over-normalization hides real precision loss → T0 money fields compared exactly
│   │
│   ├── F17.5 Bucket checksum combined with XOR — duplicate rows cancel out — duplicates invisible — CORRUPT masked [C]
│   │   ├── Handling: combine with modular sum + count (or ordered Merkle tree), never XOR alone
│   │   └── Detection: injected duplicate in test environment must change bucket checksum
│   │
│   ├── F17.6 Schema/field change deployed before monitor update — mass false PROJECTION-BUG — alert storm [C]
│   │   ├── Impact: on-call silences monitor; real issues missed for days
│   │   ├── Handling: monitor comparison rules versioned with view schema; deploy pipeline updates both together;
│   │   │             view version field lets monitor pick correct rule set
│   │   └── Detection: drift spike correlated with deploy → auto-annotate, route to deploying team
│   │
│   ├── F17.7 Aggregate deleted or erased between the two reads — false MISSING/EXTRA [C]
│   │   └── Handling: re-check with deletion/erasure registry before classification
│   │
│   ├── F17.8 Clock jump (NTP failure, VM pause, leap handling) — negative lags, drift ages wrong — false green/red [C]
│   │   ├── Handling: durations from monotonic clocks; drift age from tracker timestamps written by one clock source;
│   │   │             clock offset monitoring per host; offset > bound → freshness results UNKNOWN for that host
│   │   └── Detection: negative latency samples; clock offset alerts
│   │
│   └── F17.9 Monitor reads during active rebuild/cutover — mixed old/new views — false drift [C]
│       └── Handling: monitor subscribes to cutover controller state; switches comparison target atomically
│
├── 18. CONSISTENT BUT WRONG  (every consistency check passes)
│   ├── F18.1 Producer bug writes wrong values to write model AND events — views faithfully wrong — CORRUPT undetected [C]
│   │   ├── Impact: all drift metrics green; customers see wrong balances/prices/statuses
│   │   ├── Handling: EC-16 business invariant checks (§20) on source and views; deploy-correlated anomaly checks
│   │   └── Detection: invariant violations; distribution shift on key business fields after deploys
│   │
│   ├── F18.2 Upcaster/unit change (semantic drift) — consistent across stores, wrong by 100× [C]
│   │   └── Handling: versioning tree F6.1 semantic monitors feed into this monitor's anomaly checks
│   │
│   ├── F18.3 Event dropped BEFORE the write model is affected (rejected command, lost request) — nothing to compare [C]
│   │   ├── Impact: source and views agree; the business action never happened
│   │   └── Handling: business-level reconciliation with external systems (payments provider, partner records)
│   │
│   └── F18.4 Both source and view restored from backups to the same wrong point — consistent rollback of real data [C]
│       └── Handling: post-restore reconciliation against external systems of record (provider statements, ledgers)
│
├── 19. HUMAN & PROCESS FAILURES
│   ├── F19.1 Engineer hand-fixes a view (SQL/admin API) during incident — consumer version state not updated [C]
│   │   ├── Impact: next event gap/stale conflict; monitor sees drift without explanation; or worse, fix masks root cause
│   │   ├── Handling: EC-17 — hand fixes only via break-glass resync tooling (through pipeline); direct writes by
│   │   │             human roles audited and classified MANUAL-WRITE → incident follow-up
│   │   └── Detection: DB audit log for non-pipeline roles on view tables/indices
│   │
│   ├── F19.2 Alert silenced "temporarily" — never expires — view unmonitored for months [C]
│   │   └── Handling: EC-19; weekly report of active silences with owners
│   │
│   ├── F19.3 Alert auto-resolves when drift briefly dips — reopens — never escalates [C]
│   │   ├── Handling: hysteresis on resolve (must stay healthy for N minutes); escalation based on cumulative duration
│   │   └── Detection: flapping alert counter
│   │
│   ├── F19.4 Alert routed to team that no longer owns the view — no response [C]
│   │   └── Handling: routing from view registry ownership (single source); quarterly ownership attestations
│   │
│   ├── F19.5 Maintenance window excludes drift from SLO — real loss during maintenance never reported [C]
│   │   └── Handling: maintenance suppresses paging, never measurement; post-maintenance reconciliation mandatory
│   │
│   └── F19.6 Monitor configuration changed without review (threshold raised to stop noise) — blind spot [C]
│       └── Handling: monitor config as code with review; audit trail; threshold changes on T0 need second approver
│
├── 20. BUSINESS INVARIANT CHECKS  (EC-16 — independent of pipeline correctness)
│   ├── 20.1 Examples (define per domain)
│   │   ├── Ledger: account balance = sum of postings; debits = credits per transaction
│   │   ├── Orders: order total = sum of lines + tax + shipping − discounts; paid ≤ total
│   │   ├── Inventory: on-hand ≥ 0 unless backorders allowed; reserved ≤ on-hand + incoming
│   │   └── Payments: captured ≤ authorized; refunds ≤ captured
│   ├── 20.2 Checked on write model AND on views (catches producer and projection bugs separately)
│   ├── 20.3 Cross-system: internal totals vs provider statements daily (payments, payouts)
│   ├── F20.1 Invariants checked only in tests — production data violations unnoticed — CORRUPT [C]
│   └── F20.2 Invariant checks run on the same stale replica — false confidence [C]
│       └── Handling: freshness-aware execution (same rules as F17.1)
│
├── 21. HEALING PATHOLOGIES
│   ├── F21.1 Heal loop — heal writes version V, pipeline later applies older event, drift reappears, heal again [C]
│   │   ├── Handling: repeat-heal detector per key (> 2 heals in 24 h → stop, incident); version-guarded heals only
│   │   └── Detection: heals per key histogram
│   │
│   ├── F21.2 Heal storm after consumer DB failover — hold state lost — everything classified UNEXPLAINED [C]
│   │   ├── Handling: healing auto-disabled after any pipeline failover/restore until reconciliation completes;
│   │   │             volume breaker (§8.3)
│   │   └── Detection: failover events subscribed by healer
│   │
│   ├── F21.3 Heal executes during producer incident — snapshot API returns inconsistent state — wrong heal [C]
│   │   └── Handling: healing gated on producer health; snapshot responses must carry version + hash
│   │
│   └── F21.4 Heal fixes view but not downstream consumers of the view (caches, exports, partner feeds) — partial repair [C]
│       └── Handling: heals propagate via fan-out (authoritative → derived); external exports re-sent via their own workflow
│
├── 22. CANARY HAZARDS
│   ├── F22.1 Canary covers one partition / one stream — stalled partition 17 invisible — OUTAGE masked [C]
│   │   ├── Handling: canary key set chosen so at least one key hashes to EVERY partition of every monitored stream;
│   │   │             recomputed when topics are created/migrated
│   │   └── Detection: canary coverage matrix (stream × partition × region) with per-cell freshness
│   │
│   ├── F22.2 Canary not representative — small payloads, pooled tenant only, no encryption path [C]
│   │   ├── Impact: large-message failures, dedicated-tenant topics, KMS path failures invisible
│   │   └── Handling: canary variants per size class, tenancy model, encryption, handler, region
│   │
│   ├── F22.3 Canary data counted in business metrics, revenue, billing, analytics — CORRUPT reporting [C]
│   │   └── Handling: EC-18 canary flag honored by every consumer; reports filter canary tenants; reconciliation excludes
│   │
│   ├── F22.4 Canary triggers real external effects (emails, payments, webhooks) — customer/financial impact [C]
│   │   └── Handling: canary effects routed to sandbox providers via effect gateway; tested
│   │
│   └── F22.5 Canary visible to real users (search results, shared dashboards) — trust/SECURITY issue [C]
│       └── Handling: canary tenants isolated; filtered aliases exclude canaries from user queries
│
├── 23. INCIDENT-TIME FAILURES  (monitor fails when it matters most)
│   ├── F23.1 Drift tracker cardinality explosion (millions of keys drifting) — monitor OOM — blind mid-incident [C]
│   │   ├── Handling: bounded per-key tracking; overflow switches to bucket-level aggregation with counts and max ages
│   │   └── Detection: tracker size vs bound; overflow mode metric
│   │
│   ├── F23.2 Monitor adds load to degraded producer (snapshot API, replica reads) — deepens outage [C]
│   │   ├── Handling: monitor sheds itself when targets degrade (adaptive limits); results become UNKNOWN, not retries
│   │   └── Detection: monitor request rate vs target health
│   │
│   ├── F23.3 Paging storm — hundreds of per-view alerts for one root cause — real signal lost [C]
│   │   ├── Handling: alert grouping by hop and root dependency; inhibit downstream alerts when upstream hop is down
│   │   └── Detection: alerts per incident count
│   │
│   └── F23.4 Monitor dashboards depend on the degraded store (queries time out) — no visibility [L]
│       └── Handling: monitor's own results stored in independent time-series storage
│
├── 24. REGION / DR FAILURES
│   ├── F24.1 After regional failover, monitor still compares against old region — mass false drift or UNKNOWN [C]
│   │   └── Handling: monitor targets follow topology registry; failover runbook includes monitor re-targeting
│   ├── F24.2 DR region views never monitored (only primary has checks) — DR data rot discovered at failover [C]
│   │   └── Handling: DR views monitored continuously (freshness + sampled correctness)
│   └── F24.3 Cross-region comparisons with replication lag — permanent false drift [C]
│       └── Handling: per-region comparisons against same-region sources; cross-region parity with lag-aware tolerance
│
└── 25. ADDITIONAL GO-LIVE TESTS
    ├── Kill alert manager → external dead-man's switch pages within SLO
    ├── Fail the Kafka cluster shared by pipeline and canary reader → page still arrives (independent egress)
    ├── Force comparator timeouts → results UNKNOWN, coverage alert fires, nothing counted as MATCH
    ├── Stop replication on source replica → monitor marks UNKNOWN, no false GHOST/drift storm
    ├── Take OpenSearch shard offline → monitor rejects partial results; user-facing partial policy verified
    ├── Inject duplicate row → bucket checksum changes (no XOR cancellation)
    ├── Deploy schema change → monitor rule set switches with deploy; no alert storm
    ├── Producer bug writes wrong totals consistently → business invariant check fires while drift stays green
    ├── Hand-edit a view row as human role → MANUAL-WRITE detected and routed
    ├── Silence an alert without expiry → rejected by policy
    ├── Stall only partition 17 → canary cell for partition 17 pages
    ├── Canary order placed → excluded from revenue reports; no real email sent
    ├── Simulate 5M drifted keys → tracker switches to bucket mode; monitor stays up
    ├── Regional failover → monitor re-targets; DR views were already verified
    └── Clock jump of −10 min on one host → freshness for that host UNKNOWN; no false green
```

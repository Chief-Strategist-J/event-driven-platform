# Consumer Groups — Business Logic Decision Tree

```CONSUMER GROUPS — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-GC   Kafka group coordinator — membership, generations, assignment (server-side in new protocol)
│   │   ├── C-PT   Poll thread — the ONLY thread touching the consumer client; polls continuously
│   │   ├── C-WL   Worker lanes — key-hashed, FIFO per key, execute handlers in DB transactions
│   │   ├── C-RL   Rebalance listener — assigned / revoked / lost handling
│   │   ├── C-OF   Offset & ownership store (consumer DB, same as C-QS) — stored offsets + partition epochs
│   │   ├── C-WD   Watchdog — detects stalls in poll thread and lanes
│   │   ├── C-K8S  Workload controller (StatefulSet or Deployment), PDB, probes, termination grace
│   │   ├── C-AS   Autoscaler (if any) — lag-in-time based, rate-limited
│   │   └── C-MON  Monitoring — lag in time (from stored offsets), rebalances, ownership, evictions
│   │
│   ├── 0.2 Protocol generation (decide first; everything in §4 depends on it)
│   │   ├── P-CLASSIC  Client-side assignors (eager or cooperative); join/sync rounds
│   │   ├── P-NEW      Broker-side assignment, incremental by design (KIP-848) [L: GA in Kafka 4.0; verify]
│   │   └── Requirement: broker version, every client library version, and migration path documented per group
│   │
│   └── 0.3 Ownership
│       ├── Platform: client library defaults, protocol choice, K8s templates, autoscaler policy, dashboards
│       └── Consumer team: handler latency budgets, lane model, shutdown behavior, group naming
│
├── 1. INVARIANTS  (with enforcement)
│   ├── CG-01 Single writer per partition, enforced in the DB, not assumed from Kafka [C]
│   │   └── Enforced by: partition epoch in C-OF bumped on assignment; every tx verifies epoch
│   ├── CG-02 Offsets advance only with effects [C]
│   │   └── Enforced by: stored offset written in the same DB tx as effect / held / quarantine
│   ├── CG-03 Kafka-committed offsets are advisory [C]
│   │   └── Enforced by: on assignment, seek to stored offset; Kafka commit used only for tooling/lag visibility
│   ├── CG-04 Lost partitions are never committed [C]
│   │   └── Enforced by: lost handler aborts in-flight work and discards; no commits
│   ├── CG-05 Poll thread never blocks on business work [C]
│   │   └── Enforced by: processing in worker lanes; poll thread does poll, pause/resume, dispatch only
│   ├── CG-06 Per-key FIFO [C]
│   │   └── Enforced by: key-hash lane assignment; one in-flight record per key
│   ├── CG-07 Bounded stall detection before coordinator eviction [C]
│   │   └── Enforced by: watchdog thresholds < poll interval; self-fencing before eviction
│   ├── CG-08 Group identity is unique per environment and purpose [C]
│   │   └── Enforced by: naming convention + broker ACLs on group resource
│   ├── CG-09 Membership model matches workload type [C]
│   │   └── Enforced by: deploy-time validation (static ids only on StatefulSets with stable names)
│   ├── CG-10 Scaling cannot outrun stability [C]
│   │   └── Enforced by: autoscaler cooldowns, step limits, max replicas ≤ partitions
│   ├── CG-11 Offsets outlive idle periods [C]
│   │   └── Enforced by: stored offsets in C-OF; reset policy "none" for critical groups
│   └── CG-12 Assignment completes before processing [C]
│       └── Enforced by: assignment protocol (§8) — epoch bump, held keys loaded, seek — then dispatch
│
├── 2. GROUP IDENTITY & ISOLATION
│   ├── 2.1 Naming: "<env>.<domain>.<service>.<handler>.v<n>" (version bump = deliberate new group)
│   ├── 2.2 ACLs: each service principal may join only its own groups; read only its subscribed topics
│   │
│   ├── F2.1 Non-prod consumer joins prod group (shared cluster, copied config, laptop) — OUTAGE/CORRUPT [C]
│   │   ├── Impact: steals partitions; processes prod data with wrong code/DB; prod lag
│   │   ├── Handling: env prefix + group ACLs; separate clusters per env for T0
│   │   └── Detection: unknown client host/principal in group membership
│   │
│   ├── F2.2 Two services accidentally share a group id — LOSS for each [C]
│   │   ├── Impact: each processes a subset of partitions; neither sees all events
│   │   └── Detection: members with different client ids/software versions in one group
│   │
│   ├── F2.3 Group renamed in a deploy — mass replay or skip [C]
│   │   ├── Impact: new group starts from reset policy, not from old position
│   │   └── Handling: rename runbook: stop old → copy stored offsets → start new (C-OF makes this trivial)
│   │
│   └── F2.4 Regex subscription matches new topics (retry, DLQ, other team's topics) — CORRUPT [C]
│       ├── Handling: explicit topic lists for business consumers; regex only for tooling
│       └── Detection: subscription set change events
│
├── 3. MEMBERSHIP MODEL  (static vs dynamic — decided per workload)
│   ├── 3.1 Static membership
│   │   ├── Use when: StatefulSet (stable pod names); restart speed matters more than scale-in speed;
│   │   │             large state warm-up (held keys, caches) per partition
│   │   ├── Identity: stable ordinal-based id (unique per replica slot)
│   │   ├── Behavior: restart within session timeout → same partitions, no rebalance [C]
│   │   ├── Behavior: close does not leave group by default → partitions idle until session timeout [C]
│   │   └── Session timeout sized to cover restart time, not crash detection [L: 45–120 s]
│   │
│   ├── 3.2 Dynamic membership
│   │   ├── Use when: Deployment (random names); autoscaling; fast handoff on scale-in preferred
│   │   ├── Behavior: graceful close sends leave → immediate reassignment [C]
│   │   └── Session timeout sized for crash detection [L: 30–45 s]
│   │
│   ├── F3.1 Static ids from random pod names — ghost members [C]
│   │   ├── Impact: each restart is a new member; old member keeps partitions until timeout
│   │   └── Handling: CG-09 validation
│   │
│   ├── F3.2 Duplicate static id (two pods same ordinal during transition, misconfig) — OUTAGE [C]
│   │   ├── Impact: one member fenced; restart loops
│   │   └── Handling: StatefulSet ordered pod management; id from ordinal only
│   │
│   ├── F3.3 Static member permanently removed (scale-in) — partitions idle for session timeout [C]
│   │   ├── Handling: scale-in procedure explicitly removes member (admin API or client leave option where supported)
│   │   └── Detection: assigned partitions with no fetch activity
│   │
│   └── F3.4 Static + long session timeout + crash — slow failover [C]
│       └── Handling: accept by design; alert on partitions with no progress > 2× session timeout
│
├── 4. REBALANCE PROTOCOL  (P-CLASSIC; P-NEW notes inline)
│   ├── 4.1 Assignor choice (P-CLASSIC)
│   │   ├── Cooperative-sticky: incremental; only moved partitions are revoked [C]
│   │   ├── Eager (range/round-robin): all partitions revoked every rebalance [C]
│   │   └── P-NEW: assignment server-side; client-side assignor config ignored [L]
│   │
│   ├── 4.2 Cooperative reality (not "zero pause")
│   │   ├── Two rounds: first revokes moved partitions, second assigns them [C]
│   │   ├── Moved partitions are unprocessed between revoke and assign (seconds) [C]
│   │   └── Callbacks of ALL members are on the critical path of the rebalance [C]
│   │
│   ├── 4.3 Callbacks
│   │   ├── Assigned → §8 assignment protocol (epoch, held keys, seek) before dispatch
│   │   ├── Revoked (graceful) → stop dispatch for those partitions; drain or abort in-flight lane work
│   │   │                        within budget; no new tx; advisory commit only
│   │   └── Lost (session expired / fenced) → abort in-flight; NO commits; drop local state (CG-04)
│   │
│   ├── F4.1 Eager → cooperative switch in one deploy — rebalance failures / errors [C]
│   │   └── Handling: two-phase rollout: both assignors listed (cooperative first), then remove eager
│   │
│   ├── F4.2 Mixed client libraries/versions in one group — protocol mismatch [C]
│   │   └── Handling: one library + version per group; enforced by deploy validation
│   │
│   ├── F4.3 Long revocation callback — whole group stalls [C]
│   │   ├── Impact: other members wait for slow member's callback
│   │   └── Handling: callback budget [L: < 10 s]; in-flight tx aborted if it can't finish
│   │
│   ├── F4.4 Long assignment work (loading thousands of held keys) — slow start [L]
│   │   ├── Handling: indexed loads; lazy per-key check from DB with cache; bounded startup budget
│   │   └── Detection: assignment-to-first-dispatch latency
│   │
│   ├── F4.5 Commit in lost handler — zombie commit [C]
│   │   └── Handling: CG-04
│   │
│   ├── F4.6 Rebalance storm (members flapping) — OUTAGE [C]
│   │   ├── Causes: liveness probe kills, OOM, GC/runtime pauses, autoscaler flapping, poll-interval evictions
│   │   ├── Handling: fix cause; storm detection → freeze autoscaler + page
│   │   └── Detection: rebalances per 10 min > threshold [L: > 3]
│   │
│   └── F4.7 Sticky assignment keeps skewed load — DEGRADE [L]
│       └── Handling: partition load metrics; manual rebalance trigger; hot-partition design review
│
├── 5. TIMEOUTS & LIVENESS
│   ├── 5.1 Relationship rules [C]
│   │   ├── heartbeat interval ≤ 1/3 session timeout
│   │   ├── session timeout within broker's allowed min/max range
│   │   ├── max poll interval ≫ worst-case time between polls (poll thread never processes → small)
│   │   └── every client/DB/HTTP timeout < per-record budget < lane stall threshold < max poll interval
│   │
│   ├── 5.2 Defaults [L]
│   │   ├── Dynamic: session 45 s, heartbeat 15 s
│   │   ├── Static: session 60–120 s (covers restart), heartbeat ≤ 1/3
│   │   ├── Max poll interval: 300 s (poll thread decoupled, so rarely approached)
│   │   └── Per-record budget: from handler p99.9 × safety factor [G: measure]
│   │
│   ├── F5.1 Runtime pauses (GC, CPU throttling under container limits) exceed session timeout — eviction [C]
│   │   ├── Handling: CPU requests sized to avoid throttling; runtime tuning; session timeout margin
│   │   └── Detection: throttled-CPU metric; GC pause histogram; eviction events
│   │
│   ├── F5.2 Heartbeat thread alive while processing hung — partitions held hostage [C]
│   │   ├── Impact: coordinator sees healthy member; no progress until max poll interval
│   │   └── Handling: watchdog on lane progress (§10), not only on poll
│   │
│   └── F5.3 Broker-side limits differ from client settings — join failures [C]
│       └── Handling: startup validation against broker config; fail fast
│
├── 6. POLL LOOP, THREADING & BACKPRESSURE
│   ├── 6.1 Model
│   │   ├── C-PT: poll → dispatch records to lanes by key hash → poll again (short poll timeout)
│   │   ├── C-WL: N lanes; per-lane FIFO; one tx per record (or per small same-key batch)
│   │   ├── Backpressure: lane queue depth > high watermark → pause those partitions; < low watermark → resume
│   │   └── Stored offset per partition = lowest offset not yet durably handled across lanes (contiguous)
│   │
│   ├── 6.2 Batch sizing
│   │   ├── max records per poll sized by lane capacity, not by poll interval (poll thread doesn't process)
│   │   └── Adaptive: shrink when per-record latency rises
│   │
│   ├── 6.3 Memory sizing
│   │   ├── Fetch buffers: total fetch bytes + per-partition fetch bytes × owned partitions
│   │   ├── Lane queues: high watermark × max record size × lanes
│   │   └── Container limit ≥ (fetch + queues + runtime) × safety [G: measure]
│   │
│   ├── F6.1 Processing on poll thread — eviction under slowdowns [C]
│   │   └── Handling: CG-05
│   │
│   ├── F6.2 Unordered parallelism across lanes for same key — reorder [C]
│   │   └── Handling: CG-06 hash by aggregate key (not Kafka partition, not round-robin)
│   │
│   ├── F6.3 Offset advanced past unfinished lane work — LOSS on crash [C]
│   │   └── Handling: contiguous-offset tracking; stored offset in same tx as each record's effect
│   │
│   ├── F6.4 OOM from fetch buffers × partitions — crash loop [C]
│   │   ├── Impact: restart → rebalance → same assignment → OOM again
│   │   ├── Handling: 6.3 sizing; per-partition fetch cap; large messages via claim-check
│   │   └── Detection: OOM kill count; memory vs limit
│   │
│   ├── F6.5 Paused partitions forgotten (resume never called) — silent stall [C]
│   │   ├── Handling: pause reasons tracked; watchdog checks paused-without-reason
│   │   └── Detection: paused partition gauge with reason labels
│   │
│   └── F6.6 Pause state lost on reassignment — new owner floods dependency [C]
│       └── Handling: breaker state consulted on assignment (retry tree §6)
│
├── 7. OFFSET MANAGEMENT
│   ├── 7.1 Source of truth: stored offsets in C-OF (same tx as effects) [C]
│   ├── 7.2 Kafka commits: advisory, async during operation, sync best-effort on graceful revoke/close
│   ├── 7.3 On assignment: seek to stored offset (ignores Kafka committed offset)
│   ├── 7.4 Reset policy: "none" for T0/T1 (fail loudly); explicit bootstrap procedure (ordering tree §7.4)
│   │
│   ├── F7.1 Async commit callbacks complete out of order — confusion in tooling [C]
│   │   └── Handling: advisory only; stored offsets authoritative
│   │
│   ├── F7.2 Commit fails after rebalance (generation changed) — duplicate processing [C]
│   │   └── Handling: expected; idempotent consumers absorb; stored offsets prevent loss
│   │
│   ├── F7.3 Group offsets expire during long idle / scale-to-zero — LOSS or replay [C]
│   │   └── Handling: CG-11; stored offsets
│   │
│   ├── F7.4 Kafka-reported lag wrong because commits are advisory/late — false alerts or blind spots [C]
│   │   └── Handling: lag computed from stored offsets vs log end offsets (§14)
│   │
│   └── F7.5 Manual offset reset via admin tool while consumers stored offsets elsewhere — ignored or harmful [C]
│       └── Handling: offset changes via runbook against C-OF only; tooling aware of stored offsets
│
├── 8. PARTITION ASSIGNMENT PROTOCOL  (per newly assigned partition, before any dispatch)
│   ├── Step 1 Bump partition epoch in C-OF (atomic; returns new epoch)
│   ├── Step 2 Load stored offset; seek
│   ├── Step 3 Load held keys for this partition (DLQ/ordering/retry trees)
│   ├── Step 4 Consult breaker state for handler dependencies; pause if open
│   ├── Step 5 Resume any DRAINING / RESYNCING / due RETRY items owned by this partition
│   └── Step 6 Mark partition ready → dispatch
│
│   ├── F8.1 Dispatch before held keys loaded — reorder [C]
│   │   └── Handling: CG-12
│   ├── F8.2 Epoch bump fails (DB down) — cannot safely own [C]
│   │   └── Handling: keep partition paused; retry; page if prolonged
│   └── F8.3 Previous owner still writing (zombie) — CORRUPT [C]
│       └── Handling: every previous-owner tx fails epoch check after Step 1
│
├── 9. REVOCATION & SHUTDOWN
│   ├── 9.1 SIGTERM sequence
│   │   ├── Step 1 Stop accepting new dispatch; pause all partitions (poll thread keeps polling for heartbeat/membership)
│   │   ├── Step 2 Drain lanes up to drain budget [L: 20 s]; abort remaining in-flight tx
│   │   ├── Step 3 Advisory Kafka commit (stored offsets already durable)
│   │   ├── Step 4 Close client (dynamic: leaves group; static: does not leave unless explicitly configured)
│   │   └── Step 5 Exit
│   │
│   ├── 9.2 Kubernetes settings
│   │   ├── Termination grace > drain budget + close time + margin [L: 60 s]
│   │   ├── Pre-stop hook only if load balancer/sidecar ordering requires it
│   │   ├── Liveness probe tied to poll-thread health, generous during startup and rebalance
│   │   ├── Readiness probe irrelevant for consumers (no traffic routing) — don't gate on it
│   │   └── PodDisruptionBudget: max 1 unavailable per group (or ≤ 10% for large groups)
│   │
│   ├── F9.1 Grace period shorter than drain — SIGKILL mid-transaction [C]
│   │   ├── Impact: DB tx rolled back (safe); external calls in flight → ambiguous effects
│   │   └── Handling: 9.2; idempotency keys for external effects
│   │
│   ├── F9.2 Sidecar (service mesh/proxy) terminates before consumer finishes — failed DB/API calls [C]
│   │   └── Handling: sidecar shutdown ordering; native sidecar lifecycle where available
│   │
│   ├── F9.3 Liveness probe kills consumer during long rebalance/startup — storm [C]
│   │   └── Handling: startup probe with generous budget; liveness on poll-thread heartbeat only
│   │
│   ├── F9.4 Node drain evicts many consumers of one group simultaneously — mass rebalance [C]
│   │   └── Handling: PDB; anti-affinity across nodes/zones
│   │
│   └── F9.5 Rolling deploy restarts every pod → N rebalances [C]
│       ├── Handling: static membership (StatefulSet) or cooperative/new protocol; maxSurge small;
│       │             deploy during low traffic for T0
│       └── Detection: rebalance count per deploy
│
├── 10. HUNG CONSUMER & WATCHDOG
│   ├── 10.1 Signals
│   │   ├── Poll thread: time since last poll
│   │   ├── Lanes: time since last record completed per lane with non-empty queue
│   │   └── In-flight record age per lane
│   │
│   ├── 10.2 Escalation ladder [L]
│   │   ├── Lane stall > per-record budget → interrupt/cancel record (classified as message failure after discriminator)
│   │   ├── Lane stall > 2× budget → self-fence: stop dispatching, abort lane tx, pause partitions of that lane
│   │   ├── Poll thread stall > 50% of max poll interval → graceful shutdown path (§9.1) → exit
│   │   └── Graceful path itself stalls > 30 s → hard exit
│   │
│   ├── F10.1 Hard exit as first response — crash loop + slow static failover [C]
│   │   └── Handling: ladder above; persisted attempts make poison detection survive exits (DLQ tree DL-15)
│   │
│   ├── F10.2 Hung thread holds DB locks — blocks other consumers/producers [C]
│   │   └── Handling: DB statement and idle-in-transaction timeouts; lock timeout on consumer roles
│   │
│   ├── F10.3 Evicted member keeps writing before it notices — zombie writes [C]
│   │   └── Handling: CG-01 epoch fencing (watchdog alone is too late)
│   │
│   └── F10.4 Deadlock inside client library — poll never returns [L]
│       └── Handling: poll-thread watchdog → hard exit after graceful attempt
│
├── 11. SCALING
│   ├── 11.1 Hard limits
│   │   ├── Active consumers ≤ partitions; extra members idle [C]
│   │   ├── Partition count fixed for ordered streams (ordering tree F5.4) [C]
│   │   └── Scale lanes (vertical) before members (horizontal) when partitions are the limit
│   │
│   ├── 11.2 Autoscaling policy [L]
│   │   ├── Signal: lag in TIME (from stored offsets), not record count
│   │   ├── Scale-out step ≤ 25% of current; cooldown ≥ 5 min; scale-in cooldown ≥ 15 min
│   │   ├── Max replicas ≤ partitions; min replicas ≥ 2 (T0/T1)
│   │   └── Freeze autoscaling during rebalance storms and deploys
│   │
│   ├── F11.1 Autoscaler flapping — continuous rebalances — OUTAGE [C]
│   │   └── Handling: cooldowns, hysteresis, freeze on storm
│   │
│   ├── F11.2 Scale-to-zero — offsets expire, held items unattended, retries never fire [C]
│   │   └── Handling: forbidden for T0/T1; T2 only with stored offsets and no scheduled work
│   │
│   ├── F11.3 Scale-out beyond partitions — idle pods, false sense of capacity [C]
│   │   └── Handling: 11.2 max replicas
│   │
│   ├── F11.4 Hot partition can't be scaled out — persistent lag on one partition [C]
│   │   └── Handling: lane parallelism by key inside partition; aggregate re-model if one key dominates
│   │
│   └── F11.5 Scale-in removes member holding many held keys — slow assignment on survivors [L]
│       └── Handling: F4.4 indexed loading; scale-in during low traffic
│
├── 12. BROKER-SIDE EVENTS
│   ├── F12.1 Coordinator broker restart/failover — rebalance + commit errors [C]
│   │   └── Handling: expected; stored offsets; retry commits; alert only if prolonged
│   ├── F12.2 Rolling broker upgrade — repeated coordinator moves [C]
│   │   └── Handling: schedule; consumers tolerate; watch rebalance counters
│   ├── F12.3 Leader movement / unavailable partitions — fetch errors, uneven lag [C]
│   │   └── Handling: client retries; alert on under-replicated / offline partitions
│   └── F12.4 Fetch-from-follower misconfigured rack id — cross-zone traffic cost / latency [L]
│       └── Handling: client rack set from node zone label; verify
│
├── 13. SECURITY & CREDENTIALS
│   ├── F13.1 Credential rotation drops connections — all members rejoin at once [C]
│   │   └── Handling: staggered rotation; overlapping validity
│   ├── F13.2 Principal can join any group — hijack risk [C]
│   │   └── Handling: group-level ACLs (CG-08)
│   └── F13.3 Consumer reads topics beyond its need — data exposure [C]
│       └── Handling: topic read ACLs per service
│
├── 14. OBSERVABILITY
│   ├── Lag in time per partition (stored offset timestamp vs now), max across group
│   ├── Rebalance count, duration, cause (join/leave/timeout/eviction) per group
│   ├── Members per group, client versions, hosts; unexpected members (F2.1)
│   ├── Partition ownership map; partitions assigned with no progress
│   ├── Epoch-fenced aborts (zombie detector)
│   ├── Poll-thread stall, lane stall, in-flight age, interrupts
│   ├── Paused partitions with reasons
│   ├── Assignment-to-first-dispatch latency
│   ├── OOM kills, CPU throttling, runtime pause histograms
│   ├── Autoscaler decisions and freezes
│   └── Absence alerts on all of the above
│
├── 15. RUNBOOKS
│   ├── RB-01 Rebalance storm → freeze autoscaler → identify cause (probes, OOM, throttling, evictions) → fix → unfreeze
│   ├── RB-02 Partition stuck (assigned, no progress) → check paused reasons, held caps, lane stalls → remediate
│   ├── RB-03 Zombie writes detected → confirm fencing aborts → inspect evicted member → fix stall cause
│   ├── RB-04 Unknown member in prod group → revoke credentials → remove member → audit
│   ├── RB-05 Group rename / migration → stop old → copy stored offsets → start new → verify parity
│   ├── RB-06 Static member scale-in → explicit member removal → confirm reassignment
│   └── RB-07 Protocol migration (eager→cooperative or classic→new) → two-phase rollout → verify rebalance metrics
│
└── 16. GO-LIVE TEST MATRIX
    ├── Rolling deploy of all pods under load → lag-in-time stays within SLO; zero duplicates beyond idempotent absorption
    ├── Kill -9 one pod → partitions reassigned (dynamic) / resumed on restart (static); no loss
    ├── Freeze a pod's process (stop signal) longer than session timeout → evicted; on resume, writes fenced
    ├── Handler hang → watchdog ladder: interrupt → self-fence → graceful exit; no zombie writes
    ├── Slow dependency (10× latency) → lanes backpressure, partitions paused, no eviction
    ├── Large messages near fetch limits on many partitions → no OOM
    ├── Node drain → PDB respected; one member at a time
    ├── Autoscaler under oscillating load → no more than configured rebalances per window
    ├── Scale-in of static member → explicit removal procedure reassigns promptly
    ├── Coordinator broker restart → recovery without loss; stored offsets used
    ├── Credential rotation → staggered reconnects; no storm
    ├── Non-prod client with prod group name → rejected by ACL
    └── Metrics pipeline down → absence alerts fire
```

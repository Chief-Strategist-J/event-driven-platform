# Event-Driven Communication — Business Logic Decision Tree

```EVENT-DRIVEN COMMUNICATION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, verify per library)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-RPC   Synchronous RPC (ConnectRPC) — deadlines, idempotency levels, retries policy, breakers
│   ├── C-OBX   Outbox per service — for producers AND reacting consumers
│   ├── C-INB   Inbox per consumer — dedupe + version guard in the same tx as effects
│   ├── C-CMDQ  Command topics — async commands with exactly one handler group
│   ├── C-RPLY  Reply events — correlated outcomes of async commands
│   ├── C-ORCH  Saga orchestrator — durable saga state, timeouts, compensations (when orchestration chosen)
│   ├── C-REPL  Local replicas — other services' data materialized from their events (ECST)
│   ├── C-CTX   Context propagation — trace, correlation, causation captured in business tx
│   ├── C-CAT   Event & command catalog — owners, consumers, contracts (domain event tree)
│   └── C-MON   Flow monitoring — end-to-end business flow tracking, stuck flows, saga dashboards
│
├── 1. INVARIANTS  (with enforcement)
│   ├── EC-01 Every state change that emits a message does so atomically via outbox — producers and reacting consumers [C]
│   │   └── Enforced by: no Kafka producer clients in services; consumer framework exposes outbox only
│   ├── EC-02 Every message handler is idempotent [C]
│   │   └── Enforced by: inbox + version guard (idempotency tree); RPC mutation idempotency keys
│   ├── EC-03 Every waiting state has a timeout and an owner [C]
│   │   └── Enforced by: saga/request-reply frameworks require deadline + timeout handler at definition
│   ├── EC-04 Compensation is a business action, defined per step, idempotent, and retried to completion or escalation [C]
│   │   └── Enforced by: saga definition schema requires compensation policy per step (or "non-compensable" + pivot rule)
│   ├── EC-05 Late replies are handled explicitly [C]
│   │   └── Enforced by: reply handlers must define behavior for "saga already completed/compensated"
│   ├── EC-06 Synchronous call chains are bounded [C]
│   │   └── Enforced by: deadline propagation; max depth policy; no sync calls on T0 hot paths without fallback policy
│   ├── EC-07 Retries exist in exactly one layer per hop [C]
│   │   └── Enforced by: mesh retries disabled or coordinated; client libraries' retries capped (retry tree RT-06)
│   ├── EC-08 Fallbacks are classified by data use [C]
│   │   └── Enforced by: per-call policy: DISPLAY (stale OK) | DECISION (fail closed) | OPTIONAL (omit)
│   ├── EC-09 Trace context never drives business logic and is captured at the source [C]
│   │   └── Enforced by: outbox stores trace context; lint forbids reading trace ids in domain code
│   ├── EC-10 No message loops [C]
│   │   └── Enforced by: causation depth limits + loop detection (domain event tree §11)
│   └── EC-11 Async chains have end-to-end latency budgets [C]
│       └── Enforced by: per-flow SLO; per-hop budgets; flow monitoring
│
├── 2. INTERACTION PATTERN SELECTION
│   └── Trigger: Service A needs something from/with Service B
│       │
│       ├── Check 2.1: What does A need?
│       │   ├── N1 Data owned by B, to display or read
│       │   │   ├── Check: called on a hot path at high volume, or needed when B is down?
│       │   │   │   ├── YES → local replica from B's events (C-REPL, §4)
│       │   │   │   └── NO → sync RPC query (§3) with DISPLAY fallback policy
│       │   ├── N2 Data owned by B, to make a DECISION (limits, eligibility, stock)
│       │   │   ├── Must reflect B's latest state at decision time? → sync RPC (§3), fail closed on error
│       │   │   ├── Bounded staleness acceptable? → local replica with freshness check (§4.3)
│       │   │   └── Must be atomic with B's state? → B makes the decision (send B a command; §5)
│       │   ├── N3 B must DO something, and A needs the outcome before proceeding
│       │   │   ├── Outcome needed within user request latency (< ~1 s) and B reliably fast → sync RPC command
│       │   │   │                                                          (idempotency key, deadline)
│       │   │   └── Otherwise → async command + reply (§5); user sees "processing"
│       │   ├── N4 B must DO something; A doesn't need the outcome → async command (§5) without waiting
│       │   ├── N5 A records a fact; others (including B) may react → domain event pub/sub (§6)
│       │   └── N6 Multi-step process across services with business rollback semantics → saga (§7)
│       │
│       ├── Check 2.2: Availability coupling acceptable?
│       │   ├── A must work while B is down → async (N4/N5) or local replica (N1/N2)
│       │   └── A can fail when B fails → sync allowed, with breaker and bounded deadline
│       │
│       └── Edge cases
│           ├── E2.1 Cross-domain notification via sync RPC fan-out — A's latency = slowest consumer; A fails when any fails — OUTAGE [C]
│           ├── E2.2 Decision made from local replica without freshness bound — stale eligibility — CORRUPT [C]
│           ├── E2.3 Command disguised as event (broadcast) — multiple handlers execute — duplicate effects [C]
│           └── E2.4 Synchronous chain A→B→C→D on a user request — tail latency and failure multiply — OUTAGE [C]
│               └── Handling: EC-06 depth limit; replicas or async for deeper hops
│
├── 3. SYNCHRONOUS RPC
│   ├── 3.1 Deadlines
│   │   ├── Caller sets deadline; propagated; callee checks remaining budget before expensive work
│   │   ├── Each hop's own timeout < remaining caller deadline (budget subtraction)
│   │   └── E3.1a Callee keeps working after caller's deadline expired — wasted work + duplicate on retry [C]
│   │
│   ├── 3.2 Retries & idempotency
│   │   ├── Queries (side-effect free) → retry allowed within deadline
│   │   ├── Mutations → retry only with idempotency key; otherwise no automatic retry [C]
│   │   ├── One retry layer (EC-07): mesh vs client vs app — pick one
│   │   ├── Hedged requests only for side-effect-free calls
│   │   └── E3.2a Mesh retries + client retries + app retries — amplification on degraded callee — OUTAGE [C]
│   │
│   ├── 3.3 Circuit breakers & fallbacks (EC-08)
│   │   ├── Breaker per callee (+ per method for very different methods)
│   │   ├── Opens on error rate and latency SLO breach (not only consecutive timeouts)
│   │   ├── Fallback by data use:
│   │   │   ├── DISPLAY → cached value with staleness indicator
│   │   │   ├── DECISION → fail closed (reject or defer command with clear status)
│   │   │   └── OPTIONAL → omit section of response
│   │   └── E3.3a Cached fallback used for credit/fraud decisions — approvals during outage — CORRUPT financial [C]
│   │
│   ├── 3.4 Bulkheads & load shedding
│   │   ├── Separate connection pools/concurrency limits per callee
│   │   ├── Callee sheds by priority when overloaded (backpressure tree §12)
│   │   └── E3.4a One slow callee exhausts shared pool — all outbound calls fail — OUTAGE [C]
│   │
│   ├── 3.5 Query patterns
│   │   ├── Batch endpoints for lists (avoid N+1 per item)
│   │   └── E3.5a Per-item RPC in loops — load storms on callee — DEGRADE [C]
│   │
│   └── 3.6 Contracts
│       └── RPC schemas versioned with same rules as events (schema evolution tree; Protobuf rules)
│
├── 4. LOCAL REPLICAS OF OTHER SERVICES' DATA  (C-REPL)
│   ├── 4.1 Source: owner's public integration events (STATE semantics preferred)
│   ├── 4.2 Rules: read-only; never modified locally; projected with version guard; bootstrapped via snapshot
│   ├── 4.3 Freshness for decisions
│   │   ├── Replica records owner's version and applied time
│   │   ├── Decision allowed only if freshness ≤ bound for that decision [G: per decision]
│   │   └── Otherwise → sync call or defer
│   └── Edge cases
│       ├── E4.1 Replica treated as authoritative for writes — conflicting truths — CORRUPT [C]
│       ├── E4.2 Replica built from private events — breaks on owner refactor — OUTAGE [C]
│       └── E4.3 Replica silently stale (consumer stopped) — decisions on old data — CORRUPT [C]
│           └── Detection: freshness metric per replica; decisions blocked on stale
│
├── 5. ASYNC COMMANDS & REQUEST-REPLY
│   ├── 5.1 Command message
│   │   ├── Sent via outbox of the sender (atomic with sender's state, e.g., "saga step started")
│   │   ├── Command topic owned by the HANDLER service; exactly one handler consumer group
│   │   ├── Contains: command id (idempotency), correlation id, reply-to (topic/stream id), deadline (valid_until)
│   │   └── Handler: idempotent by command id; may REJECT (business) — rejection is a reply, not an error
│   │
│   ├── 5.2 Reply
│   │   ├── Reply event emitted via handler's outbox atomically with its effect
│   │   ├── Outcome: SUCCEEDED | REJECTED(reason) | FAILED(retriable/permanent)
│   │   └── Sender correlates by command id; sender's processing idempotent
│   │
│   ├── 5.3 Timeouts (EC-03)
│   │   ├── Sender records deadline durably (not in-memory timer)
│   │   ├── Deadline passed without reply → TIMED_OUT state → policy: retry command (same id) | compensate | escalate
│   │   └── Timeout scheduler runs on durable state (retry tree §7 scheduler model)
│   │
│   ├── 5.4 Late replies (EC-05)
│   │   ├── Reply arrives after sender moved on (timed out, compensated, cancelled)
│   │   ├── Policy per command:
│   │   │   ├── Late SUCCEEDED of a reversible effect → trigger compensation (e.g., release reservation, refund)
│   │   │   ├── Late SUCCEEDED of irreversible effect → escalate to human / business process
│   │   │   └── Late REJECTED/FAILED → record; no action
│   │   └── E5.4a Payment captured after order timed out and was cancelled — customer charged for cancelled order — CORRUPT financial [C]
│   │
│   ├── 5.5 Deadlines inside commands
│   │   ├── Handler checks valid_until before acting; expired → REJECTED(EXPIRED) (retry tree RT-07)
│   │   └── E5.5a Handler executes hours-old command after backlog — stale intent executed — CORRUPT [C]
│   │
│   └── Edge cases
│       ├── E5.1 Two handler groups subscribe to a command topic — double execution — CORRUPT [C]
│       │   └── Handling: ACL: one consumer group principal per command topic (authorization tree §6)
│       ├── E5.2 In-memory timeout timers — lost on restart — waits forever — OUTAGE (stuck flows) [C]
│       └── E5.3 Reply topic shared by many senders without correlation filtering — misrouted replies — CORRUPT [C]
│
├── 6. PUB/SUB FACTS
│   ├── 6.1 Publication via outbox only (EC-01); producer independent of consumers
│   ├── 6.2 Reacting consumers (consume → effect → emit)
│   │   ├── One transaction: inbox record + version guard + effect + outbox rows for follow-up events
│   │   ├── Follow-up events carry causation_id = source event id; same correlation_id
│   │   └── E6.2a Consumer publishes follow-up directly to Kafka after commit — lost or phantom follow-ups — LOSS/CORRUPT [C]
│   │
│   ├── 6.3 Fan-out
│   │   ├── Each consumer has independent group, DLQ, retries, freshness SLO
│   │   └── E6.3a Producer waits for consumers (sync acks via RPC) — reintroduces coupling — OUTAGE [C]
│   │
│   ├── 6.4 Chains & latency (EC-11)
│   │   ├── End-to-end latency = Σ(hop relay lag + consumer lag + processing)
│   │   ├── Per-flow SLO split into hop budgets; flow monitor measures actual
│   │   └── E6.4a Five-hop chain with seconds of lag per hop — user-visible minutes — DEGRADE [C]
│   │
│   ├── 6.5 Loops (EC-10)
│   │   ├── A reacts to B's event by emitting an event B reacts to, and so on
│   │   ├── Detection: causation depth > limit; repeated (service, event type) in causation chain
│   │   └── E6.5a Ping-pong after a code change — exponential event storm — OUTAGE [C]
│   │
│   └── 6.6 Ordering across services: none guaranteed (ordering tree §11); causal references where needed
│
├── 7. SAGAS
│   ├── 7.1 Orchestration vs choreography
│   │   ├── Check: steps > 3, or compensation logic non-trivial, or timeouts per step, or business visibility needed?
│   │   │   ├── YES → orchestration (C-ORCH) with durable saga state
│   │   │   └── NO (2–3 steps, simple reactions) → choreography with a tracking view (§7.7)
│   │   └── E7.1a Choreographed saga with 6 services — nobody owns timeouts/compensation — stuck flows — CORRUPT [C]
│   │
│   ├── 7.2 Step classification (EC-04)
│   │   ├── COMPENSABLE — has a defined business compensation (release reservation, void authorization)
│   │   ├── PIVOT — the point of no return (capture payment, ship goods); after it, saga must go forward
│   │   ├── RETRIABLE — after pivot; must eventually succeed (retry until success or human)
│   │   └── Ordering rule: compensable steps → pivot → retriable steps
│   │       └── E7.2a Irreversible step (email, payout) placed before pivot — cannot compensate on failure — CORRUPT [C]
│   │
│   ├── 7.3 Compensation semantics
│   │   ├── Business actions, not undo (refund ≠ un-charge: fees, FX, statements differ)
│   │   ├── Idempotent (compensation id derived from saga id + step)
│   │   ├── Retried until success; after budget → human task with full context
│   │   ├── Order: reverse of completed compensable steps, but only steps that actually completed
│   │   └── Edge cases
│   │       ├── E7.3a Compensation sent for step that never executed — e.g., refund without capture — CORRUPT [C]
│   │       │   └── Handling: compensate only steps with recorded SUCCEEDED reply; unknown outcome → query/verify first
│   │       ├── E7.3b Compensation fails permanently (account closed) — saga stuck — needs human — OUTAGE (flow) [C]
│   │       └── E7.3c Compensation races with late forward success — both apply — CORRUPT [C]
│   │           └── Handling: step state machine per step; late replies processed through EC-05 rules
│   │
│   ├── 7.4 Isolation anomalies (no ACID across services) [C]
│   │   ├── Dirty reads: others see saga's intermediate state (reserved stock, pending order)
│   │   ├── Lost updates: concurrent sagas overwrite each other's effects
│   │   ├── Countermeasures:
│   │   │   ├── Semantic locks / pending states ("RESERVED", "PENDING_PAYMENT") visible and respected
│   │   │   ├── Commutative updates where possible (increments vs overwrites)
│   │   │   ├── Re-read and version checks at each step (optimistic concurrency per aggregate)
│   │   │   └── Pessimistic ordering for high-contention resources
│   │   └── E7.4a Two sagas reserve the last unit after reading availability from replicas — oversell — CORRUPT [C]
│   │       └── Handling: reservation decided by the owning aggregate (command), not by replicas
│   │
│   ├── 7.5 Timeouts per step (EC-03)
│   │   ├── Each step has deadline; timeout → retry command (same id) | compensate | escalate (defined per step)
│   │   └── Saga-level deadline (customer promise) → business escalation
│   │
│   ├── 7.6 Orchestrator durability
│   │   ├── Saga state in DB; transitions + commands emitted via outbox atomically
│   │   ├── Orchestrator itself consumes replies idempotently (inbox)
│   │   ├── Saga definition versioning: in-flight sagas complete on the version they started
│   │   └── E7.6a Orchestrator deploy changes step order — in-flight sagas resume with wrong definition — CORRUPT [C]
│   │
│   ├── 7.7 Visibility
│   │   ├── Saga tracking view: state, current step, waiting since, deadline, compensations executed
│   │   └── Stuck saga detection: waiting beyond deadline + grace → page owner
│   │
│   └── 7.8 Human intervention
│       ├── Task queue with saga context, allowed actions (retry, skip with approval, manual compensation recorded)
│       └── All manual actions audited (audit tree T-ADMIN)
│
├── 8. PRODUCER DELIVERY  (outbox relay; outbox tree authoritative)
│   ├── Idempotent producer, acks from all in-sync replicas, bounded in-flight (≤ 5 with idempotence) [C]
│   ├── Batching (linger/batch size) tuned per throughput/latency SLO [L]
│   ├── Compression: zstd supported by modern brokers/clients [C]; encrypted fields don't compress (encryption tree §5.6)
│   ├── Delivery timeout → producer instance reset; resend from earliest unacknowledged per key in order;
│   │   duplicates absorbed downstream (ordering tree §5.2)
│   └── E8.1 Relay marks rows "processed" before broker ack — LOSS on crash [C]
│
├── 9. CONTEXT PROPAGATION  (EC-09)
│   ├── 9.1 Capture at source
│   │   ├── Trace context, correlation id, causation id captured when outbox row is written (inside request span)
│   │   ├── Stored in outbox row; relay copies to headers (relay's own span links to it)
│   │   └── E9.1a Trace extracted from relay's current span — every trace breaks at outbox — DEGRADE (ops) [C]
│   │
│   ├── 9.2 Consumption
│   │   ├── Single-record processing: consumer span child of (or linked to) producer context
│   │   ├── Batch processing: one batch span with links to each record's context [L: OpenTelemetry messaging guidance]
│   │   └── Async gaps (minutes/hours later): link, don't parent (avoids misleading trace durations)
│   │
│   ├── 9.3 Baggage
│   │   ├── No personal data or secrets in baggage (propagates to every service and vendor) [C]
│   │   └── E9.3a User email in baggage — leaked to logs, APM vendors, partners — COMPLIANCE [C]
│   │
│   └── 9.4 Business identifiers
│       ├── correlation_id and causation_id are business fields in the envelope (domain event tree §8)
│       └── E9.4a Business flow keyed on trace id — sampled/dropped traces break workflows — CORRUPT [C]
│
├── 10. CONSUMER PIPELINE  (summary; details in respective trees)
│   ├── Raw bytes → deserialize/upcast (versioning tree) → decrypt (encryption tree §7)
│   ├── Inbox + version guard + effect + outbox + stored offset in one tx (idempotency, offset trees)
│   ├── Failure classification: environmental → pause/breaker; message → retry schedule with key hold;
│   │   permanent → quarantine with key hold (retry, DLQ trees)
│   ├── Sync RPC inside consumers
│   │   ├── Consumer depends on callee availability → callee outage pauses partitions (environmental)
│   │   ├── Prefer local replicas for lookups inside consumers
│   │   └── E10.1 Per-event RPC to slow service — consumer lag + poll evictions — OUTAGE [C]
│   └── E10.2 DLQ + commit without key hold — later events applied out of order — CORRUPT [C]
│
├── 11. CROSS-SERVICE FAILURE DYNAMICS
│   ├── Cascading failure: slow callee → caller pools exhausted → caller's callers time out
│   │   └── Bulkheads, deadlines, breakers, shedding (§3)
│   ├── Async backlog cascades: consumer slow → producer unaffected but downstream freshness collapses
│   │   └── Flow SLOs surface it (EC-11)
│   ├── Retry storms across services after recovery → jitter, ramped resume (backpressure tree §4.4)
│   ├── Partial outages (one partition, one tenant) → per-partition/tenant monitoring
│   └── E11.1 Recovery of one service triggers synchronized retries from many — re-outage [C]
│
├── 12. CONTRACTS & OWNERSHIP
│   ├── Catalog: events (owner, consumers), commands (handler owner, senders), RPC APIs (owner, callers)
│   ├── Consumer registry for every event stream (schema evolution tree C-CRG)
│   ├── Command topics owned by handler; senders registered (authorization: Write by senders, Read by handler only)
│   └── E12.1 Unknown consumers of an event — breaking changes break unknown parties — OUTAGE [C]
│
├── 13. OBSERVABILITY
│   ├── Flow monitor: per business flow (correlation id) — start, steps, current wait, latency vs SLO
│   ├── Sagas: by state, stuck count, compensation counts, human tasks open
│   ├── Request-reply: timeouts, late replies by type, rejection rates
│   ├── RPC: latency/error per callee, breaker states, retries, amplification ratio
│   ├── Loops: causation depth histogram, loop detections
│   ├── Replicas: freshness per replica, decisions blocked by staleness
│   ├── Tracing: % of consumer spans linked to producer context (detects broken propagation)
│   └── Absence alerts on all of the above
│
├── 14. RUNBOOKS
│   ├── RB-01 Stuck sagas → identify waiting step → check handler health/command delivery → retry or compensate
│   ├── RB-02 Late success after compensation → apply late-reply policy → customer-facing correction
│   ├── RB-03 Event loop storm → stop offending consumer → identify causation pattern → fix → drain
│   ├── RB-04 Cascading RPC failure → open breakers/shed → stabilize callee → ramped recovery
│   ├── RB-05 Broken traces at outbox → verify capture-at-insert → backfill not possible; fix forward
│   └── RB-06 Replica stale → decisions fall back/defer → fix replica consumer → verify freshness
│
└── 15. TEST MATRIX
    ├── Reacting consumer crashes between effect and follow-up → follow-up emitted exactly once after recovery
    ├── Async command timeout → retry with same command id → single effect
    ├── Late success after timeout → compensation/escalation per policy
    ├── Compensation for step that never succeeded → not sent
    ├── Compensation failure → retried → human task created with context
    ├── Two concurrent sagas on last unit → no oversell (owner decides)
    ├── Orchestrator redeploy with new definition → in-flight sagas finish on old version
    ├── Credit service down → decision fails closed; display data shows stale indicator
    ├── Mesh + client retries → amplification test shows single retry layer
    ├── Deadline expired mid-chain → downstream stops work
    ├── Trace continuity through outbox → consumer span linked to originating request
    ├── Baggage with PII → stripped/blocked
    ├── Causal loop introduced → detected and halted
    ├── Command topic second consumer group → denied by ACL
    └── Five-hop flow under load → end-to-end latency within flow SLO; per-hop budgets visible
```

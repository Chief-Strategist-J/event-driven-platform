# Domain Event Design — Business Logic Decision Tree

```
DOMAIN EVENT DESIGN — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default)   [G] Guessing (needs your domain input)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-DOM   Domain model — aggregates, invariants, commands, decisions
│   ├── C-CAT   Event catalog — every event type: owner, meaning, semantics class, consumers, PII tags, lifecycle
│   ├── C-ENV   Envelope library — builds/validates envelope inside the business transaction
│   ├── C-OBX   Outbox — the only publication path (outbox tree)
│   ├── C-ACL   Integration translation layer — maps private domain events to public integration events
│   ├── C-REG   Schema registry — contracts per event type (versioning/schema evolution trees)
│   └── C-REV   Design review checklist — enforced in PR templates and CI lint
│
├── 1. INVARIANTS  (with enforcement)
│   ├── DE-01 Events are never mutated in place; erasure happens via crypto-shredding/retention, correction via new events [C]
│   │   └── Enforced by: no update paths to published data; erasure workflow (encryption tree); correction event patterns (§10)
│   ├── DE-02 aggregate_version is gapless per aggregate (previous + 1) [C]
│   │   └── Enforced by: conditional update on aggregate row; uniqueness (aggregate, version) (ordering tree OR-01)
│   ├── DE-03 Every event type declares its semantics class: STATE | DELTA | NOTIFICATION [C]
│   │   └── Enforced by: catalog field required; consumers' handlers bound to declared class
│   ├── DE-04 Facts carry the reference data that defined them at fact time [C]
│   │   └── Enforced by: review checklist ("prices, rates, tax rules, names as of the fact"); contract tests
│   ├── DE-05 Envelope is complete, schemaed, and built by the library [C]
│   │   └── Enforced by: C-ENV; envelope schema shared type; lint forbids hand-built envelopes
│   ├── DE-06 Validation happens before or inside the business transaction [C]
│   │   └── Enforced by: C-ENV called by command handler before outbox insert; relay never validates-and-drops
│   ├── DE-07 Only the outbox publishes domain events [C]
│   │   └── Enforced by: no Kafka producer clients in business services (outbox tree F1.1)
│   ├── DE-08 Private domain events are not public contracts [C]
│   │   └── Enforced by: public topics carry integration events produced by C-ACL; catalog marks visibility
│   ├── DE-09 Business-significant counts never travel through telemetry pipelines [C]
│   │   └── Enforced by: classification gate (§2); usage/billing events are domain events
│   ├── DE-10 Event identity is stable across retries [C]
│   │   └── Enforced by: event_id derived from (command idempotency key, aggregate, version) or generated once in tx
│   └── DE-11 Personal data is minimized and encrypted per field class [C]
│       └── Enforced by: PII tags + encryption tree EN-01..EN-04; review: "does any consumer need this field?"
│
├── 2. MESSAGE CLASSIFICATION
│   └── Trigger: Team proposes a new message
│       ├── Check 2.1: What is it?
│       │   ├── FACT — something that happened and was decided by the owning aggregate → domain event
│       │   ├── COMMAND — a request for one specific handler to do something (may be rejected)
│       │   │   ├── Synchronous (needs immediate answer) → RPC
│       │   │   └── Asynchronous (work queue, one handler) → command message on a command topic
│       │   │       (single consumer group, explicit reply/result event; not broadcast)
│       │   ├── QUERY — request for data → never a message on event streams
│       │   ├── TELEMETRY — operational signals (latency, errors, resource usage) → observability pipeline
│       │   ├── BUSINESS METRIC / USAGE — billable usage, quotas, consumption → DOMAIN EVENT (DE-09)
│       │   └── NOTIFICATION TO HUMANS — email/SMS content → effect of a fact, produced by a consumer
│       │
│       ├── Check 2.2: Is it a fact the producer actually decided?
│       │   ├── YES → domain event owned by the deciding aggregate
│       │   └── NO (producer observed something another system decided) → the owner publishes it;
│       │       the observer publishes only its own facts ("PaymentConfirmationReceived" ≠ "PaymentCaptured")
│       │
│       └── Edge cases
│           ├── E2.1 Command disguised as event ("SendEmailRequested" broadcast) — every consumer may act — duplicate effects [C]
│           ├── E2.2 Usage events sent to telemetry — sampling/drops — under-billing — LOSS (revenue) [C]
│           ├── E2.3 Telemetry sent to domain topics — volume floods T0 streams — DEGRADE [C]
│           └── E2.4 Fact published by a non-owner (two services emit "OrderShipped") — conflicting truths — CORRUPT [C]
│               └── Handling: one owner per fact type in catalog; single-writer ACLs (authorization tree AZ-04)
│
├── 3. PRIVATE DOMAIN EVENTS vs PUBLIC INTEGRATION EVENTS  (DE-08)
│   ├── Check 3.1: Who consumes it?
│   │   ├── Only the owning bounded context (projections, internal reactions) → private domain event
│   │   │   └── Free to evolve with the domain model (still versioned)
│   │   └── Other teams/contexts → public integration event via C-ACL
│   │       ├── Stable, coarse-grained, documented semantics; slower evolution
│   │       ├── Hides internal structure (no internal ids/enums/state machines leaked)
│   │       └── Produced in the same outbox transaction (atomic) or derived from private stream by translator
│   │
│   └── Edge cases
│       ├── E3.1 Internal events exposed as public — every refactor becomes a cross-team breaking change — OUTAGE [C]
│       ├── E3.2 Translator derives public events from private stream with bugs — public facts wrong — CORRUPT [C]
│       │   └── Handling: same-transaction emission for T0; translator tested with golden scenarios
│       └── E3.3 Public event exposes internal state names (e.g., "PENDING_FRAUD_REVIEW") — leaks security logic — SECURITY [C]
│
├── 4. NAMING & IDENTIFICATION
│   ├── 4.1 Past tense, business language, specific intent: "OrderPlaced", "PaymentCaptured", "AddressCorrected"
│   ├── 4.2 Avoid generic "<Entity>Updated" carrying diffs — consumers must infer which business fact happened
│   ├── 4.3 "Created" acceptable when creation is the actual business fact (e.g., "AccountOpened" preferred,
│   │       but blanket bans push teams to invent misleading names)
│   ├── 4.4 Distinguish lifecycle stages: "PaymentAuthorized" vs "PaymentCaptured" vs "PaymentSettled"
│   ├── 4.5 Failures are facts: "PaymentDeclined", "ShipmentFailed" (with reason codes)
│   ├── 4.6 Requested vs completed: "RefundRequested" (accepted for processing) vs "RefundIssued" (done)
│   │
│   └── Edge cases
│       ├── E4.1 "OrderUpdated" with status change inside — consumers miss cancellations — CORRUPT [C]
│       ├── E4.2 Same name used for different meanings across contexts ("Completed") — CORRUPT [C]
│       │   └── Handling: fully qualified names (domain.aggregate.EventName) in catalog
│       └── E4.3 Event named for a UI action ("CheckoutButtonClicked") — fact vs telemetry confusion [C]
│
├── 5. SCOPE & GRANULARITY
│   ├── 5.1 One command → one or more events on the SAME aggregate
│   │   ├── Each event gets its own version (n+1, n+2) in the same transaction
│   │   ├── All share causation id (the command) and command id
│   │   └── Consumers process them in order; each is independently meaningful
│   │
│   ├── 5.2 One command → events on MULTIPLE aggregates (same transaction)
│   │   ├── Different partitions → consumers see them independently (no atomicity downstream) [C]
│   │   ├── Envelope: transaction group id + group size → consumers needing completeness can wait (bounded)
│   │   └── Prefer designs where each aggregate's events are self-sufficient
│   │
│   ├── 5.3 Granularity check
│   │   ├── Too fine (field-level events: "EmailChanged", "PhoneChanged" for one form submit) → consumers see
│   │   │   intermediate states; event storms
│   │   ├── Too coarse ("CustomerChanged" for everything) → semantics lost
│   │   └── Rule: one event per business decision (what a domain expert would name)
│   │
│   ├── 5.4 Size
│   │   ├── Collections bounded per event [L: define max items]; claim-check above size threshold
│   │   └── Large facts split into header event + item events with explicit completeness markers
│   │
│   └── Edge cases
│       ├── E5.1 Consumer acts on first of two related events before the second arrives — wrong decision — CORRUPT [C]
│       │   └── Handling: group id + size; or merge into one event if always emitted together
│       ├── E5.2 Bulk command emits 100k events in one transaction — relay/decoding/consumer burst — DEGRADE [C]
│       └── E5.3 Event split across messages without completeness marker — partial processing — CORRUPT [C]
│
├── 6. PAYLOAD SEMANTICS  (DE-03, DE-04)
│   ├── 6.1 Semantics class
│   │   ├── STATE — complete current state of the aggregate (or of a declared sub-structure)
│   │   │   └── Consumers: strictly-newer wins; coalescing allowed (backpressure tree §9.2)
│   │   ├── DELTA — change relative to previous state (amount added, item removed)
│   │   │   └── Consumers: exact-next only; no skipping; inbox mandatory
│   │   └── NOTIFICATION — identifiers only; consumer must fetch (§12)
│   │
│   ├── 6.2 Fact-time snapshots (DE-04)
│   │   ├── Include values that defined the fact: unit price, discount rule id + values, tax rate, FX rate,
│   │   │   product name/sku at the time, policy version applied
│   │   ├── References to other aggregates: id + version observed (when the version matters)
│   │   └── Never require consumers to look up mutable reference data to interpret a historical fact
│   │
│   ├── 6.3 Derived values
│   │   ├── Include totals the producer computed (with the invariant documented: total = Σ lines + tax − discounts)
│   │   └── Consumers verify invariant for T0 (business invariant checks)
│   │
│   ├── 6.4 Money, units, time (schema evolution tree §7)
│   │   ├── Amount in minor units + currency code; rounding mode documented
│   │   ├── Quantities with explicit units
│   │   └── Times with explicit time zone semantics (§7)
│   │
│   ├── 6.5 Data minimization (DE-11)
│   │   └── Each field justified by at least one consumer need; PII encrypted per field class
│   │
│   └── Edge cases
│       ├── E6.1 "Snapshot delta" hybrid — consumers apply as state or as delta inconsistently — CORRUPT [C]
│       ├── E6.2 Order event carries product id only — consumer uses today's price for historical revenue — CORRUPT [C]
│       ├── E6.3 STATE event omits fields "unchanged" — consumers overwrite with nulls — LOSS [C]
│       │   └── Handling: STATE means complete; partial structures declared as patch with explicit semantics
│       ├── E6.4 Totals included but computed differently than consumers — reconciliation disputes — CORRUPT [C]
│       └── E6.5 PII included "just in case" — expanded erasure scope and breach impact — COMPLIANCE [C]
│
├── 7. TIME SEMANTICS
│   ├── 7.1 Required times
│   │   ├── effective_at — when the fact takes business effect (may differ from recording: backdated adjustments,
│   │   │                  scheduled changes, effective-dated prices)
│   │   ├── recorded_at — when the producer committed it (system time at commit)
│   │   └── Publication time — Kafka timestamp (transport concern; not business time)
│   ├── 7.2 Rules
│   │   ├── Reporting/period assignment uses effective_at with late-fact handling (materialized views tree §8)
│   │   ├── Ordering never by timestamps (ordering tree §11.3)
│   │   └── Future-dated facts: "PriceChangeScheduled" (fact now) vs "PriceChanged" (fact at effective time)
│   │
│   └── Edge cases
│       ├── E7.1 Single timestamp used for both — backdated adjustments land in wrong period — CORRUPT [C]
│       ├── E7.2 Local time without zone — DST and cross-region errors — CORRUPT [C]
│       └── E7.3 Future-dated event treated as already effective by consumers — premature effects — CORRUPT [C]
│
├── 8. ENVELOPE SPECIFICATION  (DE-05)
│   ├── 8.1 Fields (schemaed shared type; headers mirror a minimal subset for routing only)
│   │   ├── event_id — unique; stable across retries (DE-10)
│   │   ├── event_type — fully qualified name
│   │   ├── schema reference — registry id embedded by serializer (authoritative); human version optional
│   │   ├── aggregate_type, aggregate_id (opaque), aggregate_version (gapless, 64-bit)
│   │   ├── tenant_id
│   │   ├── effective_at, recorded_at
│   │   ├── actor — principal + on_behalf_of chain (authorization tree §3.4) — a record, not a credential
│   │   ├── source — producing service + build version
│   │   ├── command_id / idempotency key — ties event to the command that produced it
│   │   ├── causation_id, correlation_id
│   │   ├── transaction_group_id + group_size (when multiple aggregates in one tx)
│   │   ├── semantics class (STATE/DELTA/NOTIFICATION) and visibility (private/public)
│   │   ├── data classification (highest PII class present)
│   │   └── trace context (W3C) — observability only; never business logic
│   │
│   ├── 8.2 Identity rules
│   │   ├── UUIDv7 acceptable for uniqueness; never used for ordering across hosts [C]
│   │   └── event_id generated once inside the transaction and stored in outbox (retries reuse it)
│   │
│   └── Edge cases
│       ├── E8.1 event_id regenerated by relay on retry — dedupe fails — duplicates — CORRUPT [C]
│       ├── E8.2 Envelope fields only in headers — dropped by intermediaries — routing/idempotency lost [C]
│       ├── E8.3 Trace context used as correlation for business logic — sampled/dropped traces break flows [C]
│       ├── E8.4 aggregate_version from DB sequence — false gaps — consumers hold keys forever — OUTAGE [C]
│       └── E8.5 actor missing on system-generated events — audit cannot attribute — COMPLIANCE [C]
│           └── Handling: explicit system principal with job/run identity
│
├── 9. VALIDATION  (DE-06)
│   ├── 9.1 Where: command handler, before outbox insert, inside the transaction
│   ├── 9.2 What: envelope completeness, schema conformance (serialize with registered schema), tenant consistency
│   │   (multi-tenant MT-02), PII fields encrypted, business invariants of the payload, size limits
│   ├── 9.3 Failure → command fails (business tx rolls back) — never "commit state, drop event"
│   └── E9.1 Validation at relay drops invalid events after commit — state without event — LOSS [C]
│
├── 10. CORRECTIONS, REVERSALS, DELETIONS & ERASURE  (DE-01)
│   ├── 10.1 Wrong fact recorded
│   │   ├── Correction event referencing the corrected event_id: "InvoiceCorrected" (new values + reason)
│   │   ├── Financial: reversal + re-post (never edit amounts) — ledger semantics
│   │   └── Consumers: corrections are normal events in sequence (next version)
│   ├── 10.2 Aggregate deletion / closure
│   │   ├── Terminal event ("AccountClosed"); later versions forbidden (ordering tree F7.5)
│   │   └── Ids never reused (multi-tenant MT-12)
│   ├── 10.3 Personal data erasure
│   │   ├── Crypto-shredding of subject keys (encryption tree §9.4); envelope contains no personal data
│   │   └── Optional "SubjectErased" integration event so consumers purge derived plaintext
│   ├── 10.4 Retention
│   │   └── Topic retention / archive retention per data class; legal holds honored
│   │
│   └── Edge cases
│       ├── E10.1 "Immutable forever" policy blocks lawful erasure — COMPLIANCE [C]
│       ├── E10.2 Correction implemented by re-publishing original event_id with new content — integrity alarms /
│       │         silently ignored as duplicate — CORRUPT [C]
│       └── E10.3 Reversal without reference to original — reconciliation can't pair them — CORRUPT [C]
│
├── 11. CAUSALITY & CORRELATION
│   ├── 11.1 correlation_id — one per business flow (user request, saga instance); propagated through every hop
│   ├── 11.2 causation_id — the immediate cause (command id or triggering event id)
│   ├── 11.3 Cross-aggregate dependencies — optional "depends_on: aggregate@version" for consumers needing causal order
│   ├── 11.4 Sagas — saga id = correlation scope; step events carry step identifiers
│   │
│   └── Edge cases
│       ├── E11.1 Causal cycles (A triggers B triggers A) — infinite event loops — OUTAGE [C]
│       │   └── Handling: loop detection via causation depth limit and repeated (aggregate, event type) in chain
│       ├── E11.2 Correlation id regenerated at async boundaries — flows untraceable — DEGRADE (ops) [C]
│       └── E11.3 Fan-out reactions without causation — duplicate downstream reactions untraceable [C]
│
├── 12. FAT vs THIN DECISION
│   ├── Check 12.1: Do consumers need state to act or project?
│   │   ├── YES → STATE (fat) event (default)
│   │   └── NO (pure trigger) → NOTIFICATION acceptable
│   ├── Check 12.2: Is the data sensitive?
│   │   └── Use field-level encryption + entitlements, not thin events, as the access-control mechanism
│   ├── Check 12.3: If NOTIFICATION, does the source API support version-pinned reads?
│   │   ├── YES → consumers request state at version N (or ≥ N with version returned)
│   │   └── NO → not allowed for projections/replays (can't reconstruct history)
│   ├── Check 12.4: Fan-out × callback load acceptable for the source? (capacity check)
│   │
│   └── Edge cases
│       ├── E12.1 Thin event + read-back returns newer state — consumer applies future state to past event — CORRUPT [C]
│       ├── E12.2 Thin event + deleted aggregate — 404 — consumer drops event — LOSS [C]
│       ├── E12.3 Replay of thin events — every callback returns today's state — history unrecoverable — CORRUPT [C]
│       └── E12.4 Producer outage blocks all thin-event consumers — coupled OUTAGE [C]
│
├── 13. DELIVERY & CONSUMPTION CONTRACT  (references; details in respective trees)
│   ├── Publication only through outbox (DE-07); producer config per outbox tree §5
│   ├── Consumers: idempotency tree (inbox + version guard by semantics class)
│   ├── Gaps: durable hold + re-emit/resync (ordering tree §8) — never in-memory buffering
│   └── Duplicates: absorbed; same version different content → integrity incident
│
├── 14. GOVERNANCE & LIFECYCLE
│   ├── Catalog entry required before first publication (owner, meaning, class, visibility, PII, consumers)
│   ├── Design review checklist (naming, scope, semantics class, fact-time snapshots, time fields, PII minimization)
│   ├── Deprecation via versioning tree §13; public events have longer notice periods
│   └── E14.1 Events published without catalog entries — unknown consumers, unowned semantics — CORRUPT later [C]
│       └── Handling: CI blocks registration of schemas without catalog entries
│
├── 15. OBSERVABILITY
│   ├── Events per type per producer; rejected commands due to event validation
│   ├── Catalog coverage (types on topics without catalog entries → alert)
│   ├── Correction/reversal rates per type (signals upstream quality issues)
│   ├── Causation depth distribution; loop detections
│   ├── Thin-event callback load and error rates
│   └── Group completeness waits (transaction groups) and timeouts
│
└── 16. TEST MATRIX
    ├── Rolled-back command → no event; invalid event → command fails (no state change)
    ├── Concurrent commands on one aggregate → gapless versions, one conflict
    ├── Retry of command → same event_id, no duplicate fact
    ├── Order with price change after placement → consumers use fact-time price
    ├── STATE event with unchanged fields → complete; consumers never null-overwrite
    ├── DELTA event redelivered → applied once
    ├── Backdated adjustment → assigned by effective_at; recorded_at preserved
    ├── Multi-aggregate command → group id + size let consumers wait for completeness
    ├── Correction event → consumers update; original remains; reconciliation pairs them
    ├── Subject erasure → envelopes contain no PII; payload fields unreadable; SubjectErased consumed
    ├── Usage events → in domain pipeline; zero sampling
    ├── Async command message → single handler; no broadcast side effects
    ├── Causal loop → detected and stopped
    ├── Thin event replay → blocked unless version-pinned reads available
    └── Public integration event → no internal states/ids leaked; private refactor doesn't change it
```

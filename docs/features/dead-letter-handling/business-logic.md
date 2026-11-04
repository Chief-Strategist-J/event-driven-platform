# Dead-Letter Handling — Business Logic Decision Tree

```
DEAD-LETTER HANDLING — PRODUCTION DECISION TREE (v2)
Legend : [C] Certain   [L] Likely   [G] Guessing
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Every failure node reads:  Scenario → Impact → Handling → Detection
│
├── 0. INVARIANTS  (each needs owner + automated test + live monitor)
│   ├── DL-01 No silent disposition [C]
│   │   └── Every quarantined event ends in exactly one recorded outcome:
│   │       REDRIVEN-SUCCESS | COMPENSATED | DISCARDED-WITH-APPROVAL — never "expired"
│   ├── DL-02 Ordering preservation [C]
│   │   └── While any event of an aggregate is quarantined, NO later event of that aggregate is applied
│   ├── DL-03 Handler scope [C]
│   │   └── A DLQ belongs to one (topic, consumer group, handler); redrive reaches only that handler
│   ├── DL-04 Lossless capture [C]
│   │   └── Original key bytes, value bytes, and original headers preserved byte-for-byte
│   ├── DL-05 Commit only after durable quarantine [C]
│   │   └── Source offset advances only after DLQ/parking write is acknowledged by all in-sync replicas
│   ├── DL-06 Identity preserved [C]
│   │   └── Redriven event carries original event_id, aggregate_id, aggregate_version unchanged
│   ├── DL-07 Redrive goes through the same guards [C]
│   │   └── Redrive passes the version guard and inbox like live traffic; no "force apply" path
│   ├── DL-08 DLQ is for message defects, not system outages [C]
│   │   └── Dependency/infrastructure failures pause consumption; they never quarantine messages
│   ├── DL-09 Windows chain [C]
│   │   └── Max quarantine age < consumer dedupe window AND < external provider idempotency window
│   ├── DL-10 Quarantine is sensitive data [C]
│   │   └── Same access control, encryption, and erasure obligations as the source topic (or stricter)
│   ├── DL-11 Bounded redrive attempts [C]
│   │   └── An event that fails redrive N times escalates to human decision; no infinite loops
│   └── DL-12 Auditability [C]
│       └── Every quarantine, triage, redrive, discard records who/what/when/why
│
├── 1. ENTRY DECISION  (what happens to a failing event — DLQ is only one branch)
│   ├── D1 Is the failure caused by the message, or by the environment?
│   │   ├── ENVIRONMENT (sink down, partner outage, timeout, lock wait, rate limit, network) →
│   │   │   ├── Transient → bounded in-place retry with jittered backoff
│   │   │   └── Persistent → circuit breaker OPEN → pause partitions → resume on health → NO DLQ (DL-08)
│   │   └── MESSAGE → D2
│   ├── D2 Can the event be read at all?
│   │   ├── NO (deserialization, corrupt bytes, unknown schema id) → D3
│   │   └── YES → D4
│   ├── D3 Unreadable event
│   │   ├── Is the failure affecting EVERY message? (schema rollout, wrong converter, bad deploy)
│   │   │   ├── YES → HALT consumer + page; mass-DLQ forbidden (it's a deploy defect, not data)
│   │   │   └── NO → quarantine raw bytes; key parking uses key bytes / header aggregate_id
│   │   └── Requires aggregate_id + event_id in HEADERS (not only inside payload) to park correctly
│   ├── D4 Readable but rejected
│   │   ├── Validation failure (missing field, invalid value) → quarantine + park key
│   │   ├── Business rule rejection (e.g., refund > captured) → quarantine + park key + business owner
│   │   ├── Handler bug (exception in our code) → quarantine + park key + engineering owner
│   │   ├── Version gap (from idempotent-consumer rule) → HOLD key (not DLQ); upstream re-publish
│   │   └── Same version / different content → quarantine + park + incident
│   └── D5 Volume circuit breaker on quarantine itself
│       └── Quarantine rate above threshold → treat as systemic → HALT, do not keep quarantining
│
├── 2. QUARANTINE WRITE PATH
│   ├── 2.0 Normal flow
│   │   ├── Step 1: Build quarantine envelope (original bytes + original headers + diagnostics)
│   │   ├── Step 2: Write envelope to quarantine topic keyed by ORIGINAL key (keeps per-key order in DLQ)
│   │   ├── Step 3: Record key as PARKED in parking store (durable DB table, not only a topic)
│   │   ├── Step 4: Wait for full acknowledgement of Steps 2 and 3
│   │   └── Step 5: Commit source offset
│   │
│   ├── 2.1 Envelope contents (minimum)
│   │   ├── Original: topic, partition, offset, timestamp, key bytes, value bytes, all original headers
│   │   ├── Identity: event_id, aggregate_type, aggregate_id, aggregate_version, event_type, schema version
│   │   ├── Context: consumer group, handler name, handler build version, host/instance, trace id
│   │   ├── Failure: classification code (from §4), exception class, SANITIZED message, error fingerprint hash
│   │   ├── Attempts: in-place retry count, retry-tier count, redrive count, first-failure and last-failure time
│   │   └── Stack traces → NOT in headers; write to log store with a reference id in the envelope
│   │
│   ├── F2.1 DLQ publish fails (topic missing, ACL, size, broker down) — LOSS if offset committed [C]
│   │   ├── Handling: never commit; stop the partition; page; DL-05
│   │   └── Detection: quarantine publish error counter (must alert on first occurrence)
│   │
│   ├── F2.2 Envelope exceeds message size limit — OUTAGE/LOSS [C]
│   │   ├── Impact: original near limit + diagnostic headers > limit → publish rejected forever
│   │   ├── Handling: DLQ topic limit ≥ source limit + diagnostic budget; large bodies via claim-check
│   │   └── Detection: envelope size histogram vs limit
│   │
│   ├── F2.3 Original bytes re-serialized instead of preserved — CORRUPT [C]
│   │   ├── Impact: lossy conversion; unreadable messages "fixed" into something different; hash mismatch
│   │   └── Handling: raw bytes in, raw bytes out (DL-04)
│   │
│   ├── F2.4 DLQ topic misconfigured (RF 1, min ISR 1, auto-created, compacted) — LOSS [C]
│   │   └── Handling: same durability as source topic; pre-created; delete policy; IaC-enforced
│   │
│   ├── F2.5 Quarantine topic keyed differently / random partitioning — CORRUPT ordering [C]
│   │   └── Handling: same key, so per-key events stay ordered inside the DLQ too
│   │
│   ├── F2.6 Shared DLQ across topics or handlers — CORRUPT redrive [C]
│   │   ├── Impact: redrive tooling cannot target the right handler; cross-handler duplicates
│   │   └── Handling: one DLQ per (source topic, consumer group); naming encodes both (DL-03)
│   │
│   ├── F2.7 Diagnostics leak sensitive data — COMPLIANCE [C]
│   │   ├── Impact: exception text with PII, SQL with values, tokens, card fragments
│   │   ├── Handling: sanitizer on message text; allow-listed diagnostic fields only; traces by reference
│   │   └── Detection: periodic scan of quarantine headers for sensitive patterns
│   │
│   ├── F2.8 Crash between DLQ write and offset commit — duplicate quarantine [C]
│   │   ├── Impact: same event quarantined twice
│   │   └── Handling: parking store keyed by (handler, event_id) — idempotent quarantine
│   │
│   └── F2.9 Crash between DLQ write and parking-store write — CORRUPT [C]
│       ├── Impact: event in DLQ but key NOT parked → later versions applied → DL-02 broken
│       ├── Handling: parking-store write FIRST (DB tx), then DLQ publish, then commit;
│       │             on restart, any parked record without DLQ copy is re-published
│       └── Detection: reconciliation parking store ↔ DLQ topic
│
├── 3. KEY PARKING & ORDERING  (the part v1 is missing entirely)
│   ├── 3.0 Normal flow
│   │   ├── Every incoming event: is its aggregate key parked for this handler?
│   │   │   ├── NO  → process normally
│   │   │   └── YES → append to parking store behind the quarantined event; commit offset; do NOT apply
│   │   └── On resolution: drain parked events for that key in version order, through all guards
│   │
│   ├── F3.1 Parked-key lookup on every event is slow — DEGRADE [L]
│   │   ├── Handling: in-memory set of parked keys per partition, rebuilt from store on assignment
│   │   └── Detection: lookup latency; cache rebuild time on rebalance
│   │
│   ├── F3.2 Rebalance loses in-memory parked set — CORRUPT [C]
│   │   ├── Impact: new owner doesn't know key is parked → applies later versions
│   │   ├── Handling: rebuild parked set from durable store BEFORE processing any record after assignment
│   │   └── Detection: test: rebalance while keys parked
│   │
│   ├── F3.3 Parking store growth from one bad aggregate with high traffic — DEGRADE [C]
│   │   ├── Handling: per-key parked-event cap → escalate; parked-event storage budget per tenant
│   │   └── Detection: parked events per key; oldest parked age
│   │
│   ├── F3.4 Parking used for entity with cross-aggregate dependents — CORRUPT [C]
│   │   ├── Impact: aggregate A parked; aggregate B's events referencing A proceed
│   │   ├── Handling: dependents tolerate missing/stale references (idempotent-consumer F4.6)
│   │   └── Detection: unresolved-reference metrics
│   │
│   ├── F3.5 Partition-wide blocking chosen instead (simpler alternative) — DEGRADE [C]
│   │   ├── When acceptable: low-volume, low-tenant-count topics; ops can respond quickly
│   │   └── Cost: one bad event stalls every key on the partition; lag alert must be tight
│   │
│   └── F3.6 Drain after resolution applies parked events out of order — CORRUPT [C]
│       └── Handling: drain strictly by aggregate_version; gaps → hold (idempotent-consumer IC-05)
│
├── 4. CLASSIFICATION & TRIAGE
│   ├── 4.0 Classification taxonomy (stable codes; drives routing, ownership, alerting)
│   │   ├── C1 UNREADABLE — deserialization / schema id unknown / corrupt bytes → owner: producer team
│   │   ├── C2 CONTRACT — readable but violates contract (missing field, wrong type) → owner: producer team
│   │   ├── C3 BUSINESS-REJECT — valid contract, business rule refuses → owner: business domain
│   │   ├── C4 HANDLER-BUG — exception in consumer logic → owner: consumer team
│   │   ├── C5 INTEGRITY — same version/different content, impossible state → owner: incident
│   │   └── C6 UNKNOWN — unclassified → owner: consumer team; must be reclassified within SLA
│   │   (No "partner down" class — that is DL-08, handled by pause)
│   │
│   ├── F4.1 Classification by exception class alone — misrouting [C]
│   │   ├── Impact: generic exceptions (null reference, illegal state) hide distinct root causes
│   │   ├── Handling: error fingerprint = hash(class + normalized message + top frames); group by fingerprint
│   │   └── Detection: share of C6 UNKNOWN (should trend to 0)
│   │
│   ├── F4.2 Schema poison treated as per-message — OUTAGE→mass DLQ [C]
│   │   ├── Impact: v1 remedy "hotfix upcaster, re-drive" after thousands quarantined
│   │   ├── Handling: D3 systemic check → halt; fix producer contract or deploy upcaster; resume (no redrive)
│   │   └── Detection: quarantine rate by fingerprint
│   │
│   ├── F4.3 Integrity/corrupt marked "permanently non-retryable" and archived — LOSS [C]
│   │   ├── Impact: producer and consumer state permanently diverge for that aggregate
│   │   ├── Handling: requires producer-side correction event OR consumer compensation OR
│   │   │             explicit discard approved by business owner (DL-01)
│   │   └── Detection: integrity cases open > SLA
│   │
│   ├── F4.4 Business-reject with no business owner — LOSS by neglect [C]
│   │   └── Handling: routing to domain queue with SLA; engineering cannot "decide" business outcomes alone
│   │
│   └── F4.5 Triage happens in chat, not in a system — LOSS of audit [C]
│       └── Handling: triage state lives in the disposition ledger (§6); chat is notification only
│
├── 5. ALERTING
│   ├── 5.0 Signals that matter (in priority order)
│   │   ├── Oldest unresolved quarantine age vs SLA (primary)
│   │   ├── Parked aggregate count and oldest parked age
│   │   ├── New fingerprint first seen (novel failure)
│   │   ├── Quarantine rate vs baseline (systemic detection, D5)
│   │   └── Quarantine publish failures (F2.1) — page on first occurrence
│   │
│   ├── F5.1 Rate-only paging (e.g., > 10/min) — LOSS by slow drip [C]
│   │   ├── Impact: 1 event/hour for a month never pages; each is a diverged aggregate
│   │   └── Handling: page on age and on parked count, not only rate
│   │
│   ├── F5.2 Alert per event to chat — alert fatigue → ignored [C]
│   │   └── Handling: notify per NEW fingerprint; aggregate counts per fingerprint
│   │
│   ├── F5.3 Severity independent of domain — misprioritization [C]
│   │   └── Handling: financial/regulatory handlers page on FIRST quarantine; others by age SLA
│   │
│   └── F5.4 Alerts depend on DLQ producer metric only — blind spot [C]
│       ├── Impact: consumer crashing before quarantine emits nothing
│       └── Handling: absence alerts; canary; consumer lag alerts as independent signal
│
├── 6. DISPOSITION LEDGER  (durable state machine per quarantined event)
│   ├── 6.0 States
│   │   QUARANTINED → TRIAGED → { REDRIVE-PENDING → REDRIVEN → RESOLVED
│   │                             | COMPENSATION-PENDING → COMPENSATED → RESOLVED
│   │                             | DISCARD-PENDING-APPROVAL → DISCARDED → RESOLVED }
│   │   Any → ESCALATED (attempt cap, SLA breach, integrity class)
│   │
│   ├── 6.1 Required fields
│   │   └── handler, event_id, aggregate key/version, classification, fingerprint, owner,
│   │       state, state history (who/when/why), redrive count, linked incident/ticket, resolution note
│   │
│   ├── F6.1 Ledger only in Kafka topic — no state, no query, no audit [C]
│   │   └── Handling: transactional store for ledger; topic is transport only
│   │
│   ├── F6.2 Discard without approval — LOSS + COMPLIANCE [C]
│   │   └── Handling: two-person rule for financial/regulated handlers; reason mandatory
│   │
│   └── F6.3 Ledger and parking store diverge — CORRUPT [C]
│       └── Handling: same DB/transaction; reconciliation job
│
├── 7. REDRIVE
│   ├── 7.0 Pre-flight checks (all must pass; recorded in ledger)
│   │   ├── PF1 Root cause fixed AND deployed to every consumer instance (build version verified)
│   │   ├── PF2 Downstream healthy with headroom; circuit breakers closed
│   │   ├── PF3 Redrive within windows: dedupe window and provider idempotency window (DL-09)
│   │   ├── PF4 Target is the handler-specific redrive route (DL-03)
│   │   ├── PF5 Ordering: redrive set sorted per key by aggregate_version; includes parked successors
│   │   ├── PF6 Side-effect policy decided: effects allowed (normal) or suppressed (state-only correction)
│   │   ├── PF7 Rate limit and stop condition (abort on N consecutive failures) configured
│   │   └── PF8 Dry run on a sample with result preview
│   │
│   ├── 7.1 Modes
│   │   ├── Single event (one key) — investigation and verification
│   │   ├── By fingerprint — all events sharing one root cause
│   │   ├── By key — one aggregate including all parked successors
│   │   └── Bulk — only after single + fingerprint mode succeeded on samples
│   │
│   ├── F7.1 Redrive into main topic — CORRUPT (all consumer groups re-receive) [C]
│   │   └── Handling: handler-specific redrive topic, or direct re-invocation of the handler from ledger
│   │
│   ├── F7.2 Redrive while live events for same key still flowing — CORRUPT [C]
│   │   ├── Impact: race between redriven v5 and live v9 if key was not parked
│   │   └── Handling: key stays parked until its full drain completes, then unparked atomically
│   │
│   ├── F7.3 Redrive in DLQ offset order, not version order — CORRUPT [C]
│   │   └── Handling: sort per key by version; DLQ order is irrelevant across keys
│   │
│   ├── F7.4 Redrive applied as "force" bypassing guards — CORRUPT [C]
│   │   └── Handling: no bypass path exists in code (DL-07); stale results reported, not forced
│   │
│   ├── F7.5 Redrive tool run twice / two operators concurrently — duplicates [C]
│   │   ├── Handling: ledger state transition is a compare-and-set; idempotent consumer absorbs remainder
│   │   └── Detection: concurrent redrive lock per handler/fingerprint
│   │
│   ├── F7.6 Redriven event fails again — loop [C]
│   │   ├── Handling: increment redrive count; after cap → ESCALATED (DL-11); never auto-requeue indefinitely
│   │   └── Detection: re-quarantine rate per fingerprint after redrive
│   │
│   ├── F7.7 Fix deployed to some instances only — partial failure [C]
│   │   └── Handling: PF1 checks build version of every instance in the group before redrive starts
│   │
│   ├── F7.8 Old event shape vs new handler code — CORRUPT [C]
│   │   └── Handling: upcasters versioned and tested against quarantined samples before redrive
│   │
│   ├── F7.9 Aggregate deleted/closed since quarantine — CORRUPT [C]
│   │   └── Handling: handler defines behavior for events on terminal aggregates (skip + record vs reopen)
│   │
│   ├── F7.10 External side effects fire late — CORRUPT, financial [C]
│   │   ├── Impact: a charge or payout executes days later than the customer expects; may be wrong now
│   │   ├── Handling: business approval for delayed financial effects; PF3 window check; PF6 policy
│   │   └── Detection: financial reconciliation after redrive
│   │
│   ├── F7.11 Redrive storm competes with live traffic — DEGRADE [C]
│   │   └── Handling: separate consumer resources or strict rate cap; priority to live
│   │
│   └── F7.12 Redrive without audit — COMPLIANCE [C]
│       └── Handling: operator identity, reason, scope, results stored in ledger (DL-12)
│
├── 8. RETRY TIER BEFORE DLQ  (if delayed retry topics exist)
│   ├── F8.1 Non-blocking retry topics without key parking — CORRUPT [C]
│   │   └── Handling: same parking rule applies from the first retry, not only at DLQ
│   │
│   ├── F8.2 Delay implemented by sleeping in the consumer — OUTAGE (poll interval exceeded) [C]
│   │   └── Handling: pause partition until due time; or due-time check + seek back
│   │
│   ├── F8.3 Retry budget consumed by environmental failures — mass DLQ [C]
│   │   └── Handling: environmental errors don't count toward message retry budget (DL-08)
│   │
│   └── F8.4 Retry topics per tier per handler multiply topic count — DEGRADE [L]
│       └── Handling: few tiers; consistent naming; IaC-managed
│
├── 9. RETENTION, ARCHIVAL & COMPLIANCE
│   ├── F9.1 Retention as dead-end policy — LOSS [C]
│   │   └── Handling: retention ≥ max SLA for resolution + buffer; unresolved at threshold → ESCALATED
│   │
│   ├── F9.2 Archive is write-only (never re-drivable) — LOSS [C]
│   │   └── Handling: archive format preserves full envelope; redrive tool can read archive
│   │
│   ├── F9.3 Personal data erasure misses DLQ, parking store, archive, logs — COMPLIANCE [C]
│   │   └── Handling: erasure workflow enumerates every quarantine location; crypto-shredding preferred
│   │
│   ├── F9.4 Archive encryption, access, and retention unmanaged — COMPLIANCE [C]
│   │   └── Handling: same classification as source data; lifecycle policy; access logged
│   │
│   └── F9.5 Quarantine age exceeds dedupe/provider windows — duplicates/double effects [C]
│       └── Handling: DL-09 enforced by redrive pre-flight PF3; older items → manual compensation path
│
├── 10. SECURITY & ACCESS
│   ├── F10.1 Broad read access to DLQ — COMPLIANCE [C]
│   │   └── Handling: least-privilege ACLs; payload views masked in triage UI
│   ├── F10.2 Anyone can redrive or discard — CORRUPT + COMPLIANCE [C]
│   │   └── Handling: role-based permissions; approvals for bulk and financial handlers
│   └── F10.3 Redrive tool can write to arbitrary topics — CORRUPT [C]
│       └── Handling: tool allow-listed to handler redrive routes only
│
├── 11. OBSERVABILITY
│   ├── Per handler: quarantine rate, unresolved count, oldest unresolved age, parked keys, parked events
│   ├── Per fingerprint: first seen, count, last seen, owner, state
│   ├── Ledger state distribution; items breaching SLA; ESCALATED count
│   ├── Redrive: success, stale-skip, re-quarantine, duration
│   ├── Quarantine publish errors; envelope size vs limit
│   └── Absence alerts on all of the above
│
├── 12. CORRECTNESS PROOF
│   ├── Reconciliation: parking store ↔ DLQ topic ↔ ledger (every quarantined event in all three)
│   ├── Reconciliation: producer aggregate versions ↔ consumer versions (parked keys explain every lag)
│   ├── Financial handlers: unresolved quarantine amounts reported daily to the business owner
│   └── Zero-tolerance metric: events in no state (lost between quarantine steps) must be 0
│
├── 13. LIFECYCLE & CHANGE MANAGEMENT
│   ├── L1 New handler: DLQ, redrive route, ledger entries, alerts created with it (not after first failure)
│   ├── L2 Handler renamed / consumer group changed: ledger and parking migrate; old DLQ drained first
│   ├── L3 Schema evolution: upcasters tested against current quarantine contents before release
│   └── L4 Decommission: DLQ must be empty and ledger fully RESOLVED before removal
│
├── 14. RUNBOOKS
│   ├── R1 Systemic quarantine spike → halt → identify fingerprint → fix → resume (no mass redrive needed)
│   ├── R2 Single fingerprint fixed → sample redrive → fingerprint redrive → verify → unpark keys
│   ├── R3 Integrity case → incident → producer correction event or compensation → resolve with approval
│   ├── R4 Quarantine publish failing → stop partitions → restore DLQ path → resume
│   ├── R5 Item approaching DL-09 window → escalate → manual decision before window closes
│   └── R6 Erasure request → purge from parking store, DLQ, archive, logs → record evidence
│
└── 15. GO-LIVE GATE  (each must end with reconciliation clean and zero events in no state)
    ├── Poison v5 followed by v6–v9 for same key → v5 quarantined, v6–v9 parked, none applied
    ├── Fix + redrive by key → v5–v9 applied in order, key unparked, other handlers unaffected
    ├── Rebalance while keys parked → new owner respects parking
    ├── Kill consumer between parking write and DLQ publish → recovered on restart
    ├── DLQ topic unavailable → partition stops, no offset committed, page fires
    ├── Envelope near size limit → still quarantined (or claim-checked), never rejected
    ├── Partner outage simulation → pause, zero quarantines, auto-resume
    ├── Bad schema deploy → halt on systemic detection, not mass quarantine
    ├── Redrive executed twice concurrently → one effect only
    ├── Item aged past dedupe window → redrive blocked by pre-flight
    └── Erasure request → removed from every quarantine location
```

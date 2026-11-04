# Event Versioning — Business Logic Decision Tree

```
EVENT VERSIONING — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-REG  Schema Registry (Apicurio) — schemas, compatibility rules, artifact metadata, IDs
│   │   ├── C-GOV  Schema Governance Pipeline (CI) — lint, compatibility, semantic review, contract tests
│   │   ├── C-SER  Producer serialization layer — explicit schema, writes schema id into every event
│   │   ├── C-OBX  Outbox (from outbox tree) — stores serialized bytes + schema id at WRITE time
│   │   ├── C-DES  Consumer deserialization layer — resolves writer schema by id, reader schema = compiled
│   │   ├── C-UPC  Upcaster library — versioned, pure, deterministic transforms per event type
│   │   ├── C-DWN  Downcaster/adapter (only during major migrations) — new → old for lagging consumers
│   │   ├── C-ES   Event Store / Archive — retained raw bytes + schema id (readability horizon owner)
│   │   ├── C-QS   Quarantine Store (from DLQ tree) — raw bytes + schema id for failed events
│   │   └── C-MON  Monitoring — version mix, upcast rates, unknown-schema, semantic drift checks
│   │
│   ├── 0.2 Ownership
│   │   ├── Producer team owns: schema evolution, upcasters for its events, correction events, migration plans
│   │   ├── Consumer team owns: reader schema upgrades, tolerant-reader behavior, migration cutover
│   │   ├── Platform owns: registry, CI gates, serialization libraries, DR replication of registry
│   │   └── Architecture review owns: breaking changes, semantic changes, aggregate re-modeling
│   │
│   └── 0.3 Event criticality tiers (same as DLQ tree)
│       └── T0 financial/regulatory, T1 customer-visible state, T2 derived/rebuildable
│
├── 1. INVARIANTS  (with enforcement mechanism)
│   ├── EV-01 One aggregate, one ordered stream [C]
│   │   ├── Rule: all event types of an aggregate share one topic; key = aggregate_id
│   │   └── Enforced by: topic naming policy in CI; producer library refuses per-event-type topics
│   ├── EV-02 Three version kinds are never conflated [C]
│   │   ├── Stream version (major, breaking re-model) — in topic name, rare
│   │   ├── Schema version (structure) — registry-assigned id carried in every message
│   │   └── Aggregate version (state sequence) — per aggregate instance, used for ordering/idempotency
│   │   └── Enforced by: separate named fields; lint rejects ambiguous "version" fields
│   ├── EV-03 Writer schema is always identifiable from the bytes [C]
│   │   └── Enforced by: registry id embedded in every serialized payload; header "version" is advisory only
│   ├── EV-04 Compatibility is transitive and checked before production [C]
│   │   └── Enforced by: registry rule FULL_TRANSITIVE (or BACKWARD_TRANSITIVE with documented reason);
│   │                   auto-registration disabled in production; registration only via C-GOV
│   ├── EV-05 Meaning never changes under an existing name [C]
│   │   └── Enforced by: semantic change policy (§6) — new meaning = new field or new event type
│   ├── EV-06 Upcasters are pure, deterministic, total, and fact-preserving [C]
│   │   └── Enforced by: library constraints (no I/O, no clock, no randomness); property tests; review
│   ├── EV-07 Readability horizon [C]
│   │   ├── Rule: every schema version referenced by any retained byte stays readable and upcastable
│   │   └── Enforced by: retirement gate scanning C-ES, C-QS, C-OBX, archives, backups policy
│   ├── EV-08 Deploy order is enforced, not remembered [C]
│   │   └── Enforced by: compatibility mode dictates order; pipeline blocks producer release until
│   │                   consumers declare support (§7.3)
│   ├── EV-09 Aggregate version continuity across stream migrations [C]
│   │   └── Enforced by: new stream continues the same aggregate version sequence; never resets
│   ├── EV-10 Original bytes are immutable [C]
│   │   └── Enforced by: upcasting happens on read; storage never rewritten in place
│   ├── EV-11 Same IDs everywhere data travels [C]
│   │   └── Enforced by: single registry replicated to DR with identical ids, or id-translation at mirror
│   └── EV-12 Unknown is explicit [C]
│       └── Enforced by: nullable/optional with null default for "not recorded"; no fabricated defaults
│
├── 2. VERSION TAXONOMY  (definitions that every team uses identically)
│   ├── 2.1 Stream version (major)
│   │   ├── Lives in: topic name suffix, e.g. aggregate-level stream "orders.order.v1" (no event type in name)
│   │   ├── Changes only when: aggregate re-modeled (split/merge), key changes, ordering scope changes,
│   │   │                     or a change impossible to express compatibly
│   │   └── Frequency expectation [L]: years, not sprints
│   │
│   ├── 2.2 Schema version (structure)
│   │   ├── Lives in: registry; global/content id embedded in each message
│   │   ├── Granularity: per event type (artifact per event type within the stream)
│   │   ├── Subject/artifact naming: record-name based (multiple event types per topic) — NOT topic-name based
│   │   └── Header "schema_version": human/debug convenience only; never trusted over embedded id
│   │
│   ├── 2.3 Event type
│   │   ├── Name: past-tense business fact (OrderPlaced); stable forever
│   │   └── Rename = new event type + deprecation of old (never in-place rename)
│   │
│   ├── 2.4 Aggregate version (sequence)
│   │   ├── Per aggregate instance, gapless, starts at 1, assigned under row lock (outbox tree INV-04)
│   │   └── Independent of schema changes; a schema upgrade does NOT reset or skip it
│   │
│   └── 2.5 Legacy unversioned events (pre-registry era)
│       ├── Treated as schema "v0" via explicit legacy decoder
│       └── F2.5.1 v0 events without schema id arrive at registry-based deserializer — OUTAGE [C]
│           └── Handling: magic-byte / header detection → legacy path; never "guess" a registry schema
│
├── 3. TOPIC & ARTIFACT STRATEGY
│   ├── 3.1 Topic layout
│   │   ├── One topic per aggregate per stream version: "<domain>.<aggregate>.v<major>"
│   │   ├── Event type in header + schema record name
│   │   └── Partition count fixed for stream lifetime (resize = new stream version)
│   │
│   ├── F3.1 Per-event-type topics — CORRUPT ordering [C]
│   │   ├── Impact: cross-type ordering lost; consumers merge streams with no total order
│   │   ├── Handling: consolidate into aggregate topic (requires §10 migration)
│   │   └── Detection: version-guard gap/stale counters spike on multi-type consumers
│   │
│   ├── F3.2 Topic-name subject strategy with multiple event types — OUTAGE [C]
│   │   ├── Impact: second event type fails registration or overwrites compatibility lineage
│   │   └── Handling: record-name (or topic+record) artifact strategy, consistent across all clients
│   │
│   ├── F3.3 Clients configured with different artifact strategies — OUTAGE/CORRUPT [C]
│   │   ├── Impact: producer registers under one artifact, consumer resolves another; lineage split
│   │   └── Handling: strategy fixed in shared serialization library; config drift test in CI
│   │
│   └── F3.4 Event type reused across aggregates with different shapes — CORRUPT [C]
│       └── Handling: fully qualified record names (namespace includes domain + aggregate)
│
├── 4. FORMAT-SPECIFIC COMPATIBILITY RULES  (the registry checks structure; these are the traps)
│   ├── 4.1 Avro
│   │   ├── Safe: add field WITH default; remove field that HAD default; int→long, float→double promotions
│   │   ├── Unsafe: add field without default; rename without alias; change type outside promotion rules
│   │   ├── F4.1.1 New enum symbol — OUTAGE for old readers [C]
│   │   │   └── Handling: enum default symbol declared from day one (spec 1.9+); or string with allowed-values doc
│   │   ├── F4.1.2 Union reordering / null-default rules — registration or read failure [C]
│   │   │   └── Handling: null first in optional unions; lint rule
│   │   ├── F4.1.3 Logical types (decimal precision/scale, timestamp precision) changed — CORRUPT [C]
│   │   │   └── Handling: precision/scale/unit treated as breaking; lint blocks
│   │   └── F4.1.4 Default used ambiguously ("0" means unknown or real zero) — CORRUPT [C]
│   │       └── Handling: EV-12; optional with null default
│   │
│   ├── 4.2 Protobuf
│   │   ├── Safe: add new field numbers; deprecate fields; reserve removed numbers AND names
│   │   ├── F4.2.1 Field number reuse — CORRUPT (silent misread) [C]
│   │   │   └── Handling: reserved ranges mandatory on removal; lint blocks reuse
│   │   ├── F4.2.2 Scalar presence (proto3 zero value vs unset) — CORRUPT [C]
│   │   │   └── Handling: explicit presence ("optional") for every business field where unset ≠ zero
│   │   ├── F4.2.3 Type change int32↔int64, enum↔int — CORRUPT across languages [C]
│   │   │   └── Handling: treated as breaking
│   │   └── F4.2.4 Unknown enum values handled differently per language — CORRUPT [L]
│   │       └── Handling: every enum has explicit UNSPECIFIED = 0; consumers handle unknown explicitly
│   │
│   ├── 4.3 JSON Schema
│   │   ├── F4.3.1 Compatibility checking weaker and open-content-model dependent — CORRUPT [L]
│   │   │   └── Handling: closed/open content model decided explicitly; stricter lint; contract tests mandatory
│   │   ├── F4.3.2 Numbers parsed as floats — CORRUPT financial (outbox tree INV-12) [C]
│   │   └── F4.3.3 Inferred schemas (CDC converters inferring from JSON) — schema churn [L]
│   │       ├── Impact: optional field null in one event, present in next → different inferred schema →
│   │       │           registry version explosion / compatibility failures at relay
│   │       └── Handling: serialize with explicit schema in application (C-SER) before outbox; never infer
│   │
│   └── 4.4 Cross-format
│       └── One format per stream; format change = stream version change
│
├── 5. CHANGE CLASSIFICATION  (every schema PR is classified before merge)
│   ├── 5.1 Decision tree
│   │   ├── Q1 Does any field change MEANING (unit, sign, scale, tax basis, timezone, inclusive/exclusive)?
│   │   │   └── YES → SEMANTIC change → §6 (never same field name)
│   │   ├── Q2 Does ordering scope, key, or aggregate boundary change?
│   │   │   └── YES → STREAM change → §10 (new stream version)
│   │   ├── Q3 Passes FULL_TRANSITIVE compatibility in registry dry-run?
│   │   │   ├── YES → MINOR change → §7 normal rollout
│   │   │   └── NO → Q4
│   │   ├── Q4 Can it be expressed as add-new-field + deprecate-old (expand/contract)?
│   │   │   ├── YES → EXPAND/CONTRACT → §8 multi-release
│   │   │   └── NO → STREAM change → §10
│   │   └── Q5 Does it touch PII classification, money, or regulated fields?
│   │       └── YES → additional review (T0 owner + data protection)
│   │
│   ├── 5.2 Change catalogue (common real changes → class)
│   │   ├── Add optional field with null default → MINOR
│   │   ├── Make optional field required → EXPAND/CONTRACT (never in place)
│   │   ├── Remove field → EXPAND/CONTRACT (deprecate → stop reading → stop writing → remove)
│   │   ├── Rename field → EXPAND/CONTRACT (new name, dual-write both, migrate readers, drop old)
│   │   ├── Restructure nested object → EXPAND/CONTRACT or upcaster (if fact-preserving)
│   │   ├── Change unit/precision/currency handling → SEMANTIC
│   │   ├── Split one event type into two → new event types + deprecation (MINOR at stream level)
│   │   ├── Split/merge aggregates → STREAM
│   │   └── Change partition key → STREAM
│   │
│   └── F5.1 Classification skipped ("small change") — CORRUPT [C]
│       └── Handling: PR template + CI requires class label; semantic-impact checkbox signed by owner
│
├── 6. SEMANTIC CHANGES  (structure-preserving meaning changes — the silent killer)
│   ├── 6.1 Rule: new meaning → new field name (amount_minor_units) or new event type
│   ├── 6.2 Old field keeps old meaning until fully contracted (§8)
│   ├── 6.3 Units are explicit in names or companion fields (amount + currency + scale)
│   │
│   ├── F6.1 Unit change under same name — CORRUPT financial [C]
│   │   ├── Impact: consumers off by 100×; passes every compatibility check
│   │   ├── Handling: §6.1; schema docs require unit annotation; lint requires unit metadata on numeric money fields
│   │   └── Detection: semantic drift monitor — distribution shift of numeric fields per schema version
│   │
│   ├── F6.2 Enum value meaning changed (e.g., "CANCELLED" now includes refunds) — CORRUPT [C]
│   │   └── Handling: new enum value; old value semantics frozen and documented
│   │
│   ├── F6.3 Timestamp semantics changed (local → UTC, event time → processing time) — CORRUPT [C]
│   │   └── Handling: new field; timezone-aware types only
│   │
│   ├── F6.4 Nullability meaning changed (null used to mean "none", now "unknown") — CORRUPT [C]
│   │   └── Handling: EV-12 explicit unknown; documented per field
│   │
│   └── F6.5 Business rule change reflected only in values (tax rate model) — CORRUPT [L]
│       └── Handling: event carries rule/version identifier (e.g., tax_model) so consumers interpret correctly
│
├── 7. PRODUCER SIDE
│   ├── 7.1 Serialization point
│   │   ├── Serialize in application at outbox write (C-SER), storing bytes + schema id in outbox row
│   │   └── Relay ships bytes untouched (no re-serialization, no inference)
│   │
│   ├── 7.2 Schema resolution
│   │   ├── Producer compiled against a schema; id looked up from registry (cached), never auto-registered in prod
│   │   ├── F7.2.1 Registry unavailable at producer startup — OUTAGE of writes [C]
│   │   │   ├── Impact: cannot resolve id → cannot serialize → business tx fails
│   │   │   ├── Handling: id pinned at build/deploy time (artifact bundles schema + id); local cache persisted;
│   │   │   │             registry HA; startup does not depend on live registry for known schemas
│   │   │   └── Detection: registry lookup errors; producer startup failures
│   │   ├── F7.2.2 Auto-registration in production — CORRUPT [C]
│   │   │   ├── Impact: unreviewed schema enters lineage; may break transitive compatibility for future changes
│   │   │   └── Handling: disabled via config + registry ACL (producers read-only)
│   │   └── F7.2.3 Schema registered in staging but not production — OUTAGE at deploy [C]
│   │       └── Handling: promotion of schemas is a pipeline step before producer deploy, verified by id lookup
│   │
│   ├── 7.3 Deploy ordering by change class
│   │   ├── MINOR (new optional field): consumers tolerant → producer can deploy first (FULL compat)
│   │   ├── Consumer must USE new field: producer deploys first, consumers later (they read null before)
│   │   ├── EXPAND/CONTRACT: strictly staged releases (§8)
│   │   ├── F7.3.1 Mixed producer versions mid-rollout — interleaved schema versions per aggregate [C]
│   │   │   ├── Impact: v5 written by new code (schema 3), v6 by old instance (schema 2)
│   │   │   ├── Handling: consumers handle any supported version per event; aggregate version unaffected
│   │   │   └── Detection: version-mix metric per aggregate stream
│   │   └── F7.3.2 Rollback of producer after new schema emitted — consumers see new then old [C]
│   │       └── Handling: FULL_TRANSITIVE guarantees old readers/new data both ways; rollback tested in staging
│   │
│   ├── 7.4 Outbox interaction
│   │   ├── Outbox rows carry schema id at write time; backlog after deploy contains old ids — expected
│   │   └── F7.4.1 Relay or SMT rewrites payload format — CORRUPT [C]
│   │       └── Handling: relay is byte-transparent for payload
│   │
│   └── 7.5 Producer-side validation
│       └── Serializer validates against schema + business lint (money as minor units, required ids) before outbox insert
│
├── 8. EXPAND / CONTRACT  (multi-release evolution without breaking anyone)
│   ├── Phase E1 Expand: add new field (optional); producer writes BOTH old and new representations
│   ├── Phase E2 Migrate readers: consumers switch to new field; still tolerate old-only events (upcaster fills new from old)
│   ├── Phase E3 Verify: telemetry shows zero reads of old field across all consumers [needs read instrumentation]
│   ├── Phase C1 Contract writes: producer stops writing old field (old field remains in schema, optional)
│   ├── Phase C2 Contract schema: remove old field only when readability horizon allows (EV-07)
│   │
│   ├── F8.1 Producer writes old and new inconsistently — CORRUPT [C]
│   │   ├── Handling: both derived from one source value in one function; property test equality
│   │   └── Detection: consumer-side consistency check between fields during E1–E2
│   │
│   ├── F8.2 Phase skipped (remove before readers migrated) — OUTAGE/CORRUPT [C]
│   │   └── Handling: each phase is a tracked migration record with exit criteria; CI blocks out-of-order steps
│   │
│   ├── F8.3 "Zero reads" measured by consumer connections, not field usage — wrong decision [C]
│   │   └── Handling: field-level read telemetry in consumer libraries, or explicit consumer attestations
│   │
│   └── F8.4 Offline / batch / DR / replay consumers not counted — OUTAGE later [C]
│       └── Handling: consumer registry (who reads which stream, which versions supported, run cadence)
│
├── 9. CONSUMER SIDE — DESERIALIZATION & UPCASTING (live AND replay)
│   ├── 9.1 Read pipeline
│   │   ├── Step 1 Raw bytes (DLQ tree §3 Step 1)
│   │   ├── Step 2 Detect format/legacy (magic byte / id present)
│   │   ├── Step 3 Resolve writer schema by embedded id (cache; registry fallback)
│   │   ├── Step 4 Decode with writer schema → resolve to consumer's reader schema (format resolution)
│   │   ├── Step 5 Upcast chain to the consumer's CURRENT logical model (event type + version)
│   │   ├── Step 6 Hand to handler (idempotency / version guard / effect)
│   │   └── Any failure Step 2–5 → classify C1/C2 → DLQ tree quarantine + parking
│   │
│   ├── 9.2 Tolerant reader rules
│   │   ├── Ignore unknown fields
│   │   ├── Unknown enum values → explicit UNKNOWN handling path (not exception, not silent default)
│   │   ├── Unknown event TYPES on the stream → documented policy per handler: skip+record or park
│   │   └── Never fail on optional fields being absent
│   │
│   ├── F9.1 Writer schema id unknown to consumer's registry — OUTAGE/quarantine [C]
│   │   ├── Scenario: registry DR mismatch, mirrored topic, deleted schema, wrong registry URL per env
│   │   ├── Handling: quarantine + park (single) or halt (systemic); never guess schema
│   │   └── Detection: unknown-schema-id counter (page on any for T0)
│   │
│   ├── F9.2 Registry lookup on hot path during registry outage — OUTAGE [C]
│   │   ├── Handling: persistent local cache of all ids seen; preload all ids of subscribed artifacts at startup
│   │   └── Detection: cache-miss rate; registry error rate
│   │
│   ├── F9.3 Consumer behind producer (new schema arrives first) — handled by FULL compat [C]
│   │   └── Detection: "newer-than-reader" event count per consumer → drives consumer upgrade priority
│   │
│   ├── F9.4 Header "version" disagrees with embedded schema id — CORRUPT if header trusted [C]
│   │   └── Handling: embedded id wins; mismatch counter → producer bug ticket
│   │
│   ├── F9.5 Upcasting cost on high-volume live path — DEGRADE [L]
│   │   ├── Handling: upcasters compiled/registered once; chain length budget; cache resolved reader per writer id
│   │   └── Detection: per-record upcast latency; chain-length histogram
│   │
│   └── F9.6 Upcasted event fails handler — misattributed [C]
│       └── Handling: quarantine envelope records writer id + upcast chain applied + upcaster versions
│
├── 10. UPCASTER ENGINEERING RULES
│   ├── 10.1 Contract of an upcaster
│   │   ├── Input: one event at version N; Output: same event at N+1 (or explicit "cannot upcast")
│   │   ├── Pure: no DB, no network, no clock, no config lookups, no randomness
│   │   ├── Total: defined for every valid input of version N (property-tested over generated inputs)
│   │   ├── Fact-preserving: never invents business facts; unknown stays unknown (EV-12)
│   │   ├── Identity-preserving: event_id, aggregate_id, aggregate_version, occurred_at untouched
│   │   └── Versioned and immutable once released (bug fix = new upcaster revision, recorded)
│   │
│   ├── 10.2 Chain management
│   │   ├── Chain completeness verified in CI against EVERY schema id in registry lineage (not just latest)
│   │   ├── Chain composition tested end-to-end: v1→current equals expected golden output
│   │   └── Long chains may be compacted: direct v1→current upcaster validated against composed chain
│   │
│   ├── F10.1 Fabricated default (currency = USD) — CORRUPT financial [C]
│   │   └── Handling: forbidden by review rule; UNKNOWN + producer correction event or consumer business rule
│   │
│   ├── F10.2 Upcaster consults current state (DB lookup for missing field) — CORRUPT [C]
│   │   ├── Impact: replay produces different results than original processing; non-deterministic
│   │   └── Handling: enrichment belongs in handler with explicit "as-of" semantics, not upcaster
│   │
│   ├── F10.3 Upcaster bug fixed after events already projected — CORRUPT persisted [C]
│   │   ├── Impact: read models built with buggy transform remain wrong
│   │   └── Handling: upcaster revision recorded per projection; affected projections rebuilt or resynced
│   │
│   ├── F10.4 Missing upcaster discovered in production — OUTAGE [C]
│   │   ├── Prevention: CI gate (10.2) — missing path blocks consumer release
│   │   └── Runtime: quarantine + park (single) / halt (systemic) per DLQ tree
│   │
│   ├── F10.5 Upcaster changes identity fields — CORRUPT idempotency [C]
│   │   └── Handling: library forbids writes to identity fields (enforced by type)
│   │
│   └── F10.6 Lossy restructure (two fields merged, original split not recoverable) — LOSS of fact [C]
│       └── Handling: lossy transforms forbidden; restructure must be reversible or carry originals
│
├── 11. STREAM (MAJOR) MIGRATION  (new topic version; breaking re-model)
│   ├── 11.1 When required: key/ordering change, aggregate split/merge, partition count change,
│   │                       incompatible change not expressible as expand/contract, format change
│   │
│   ├── 11.2 Dual publishing
│   │   ├── Producer writes BOTH v1 and v2 outbox rows in the SAME business transaction (atomic, not dual-write)
│   │   ├── Both carry same event_id lineage (v2 event references v1 event_id) and same aggregate version
│   │   └── Optional: v2 produced by transformer service from v1 stream (single source, derived stream)
│   │
│   ├── 11.3 Consumer cutover (per consumer)
│   │   ├── Start v2 consumer in shadow (separate group, shadow projection or dry-run)
│   │   ├── Parity check: v1-derived state == v2-derived state per aggregate
│   │   ├── Cutover rule per aggregate: on v2, apply only events with aggregate_version > last applied from v1
│   │   │   (version continuity EV-09 makes cutover exact, no offset mapping needed)
│   │   └── Stop v1 consumer after v2 applied version ≥ v1 applied version for all aggregates
│   │
│   ├── 11.4 Producer contraction
│   │   └── Stop v1 publishing only after consumer registry shows zero v1 consumers (including batch/DR/replay)
│   │
│   ├── F11.1 Dual publish from two transactions or two services — divergence [C]
│   │   └── Handling: same tx (11.2) or derived-stream; never independent writes
│   │
│   ├── F11.2 Aggregate version restarted on v2 — CORRUPT idempotency and cutover [C]
│   │   └── Handling: EV-09; CI check on producer; consumer rejects version regressions
│   │
│   ├── F11.3 Aggregate split (one id → two ids) — CORRUPT if version lineage unclear [C]
│   │   ├── Handling: split event on old stream; new aggregates start at version 1 with lineage reference;
│   │   │             consumers handle split explicitly; no silent continuation
│   │   └── Detection: orphan references after migration
│   │
│   ├── F11.4 Parity check skipped — CORRUPT discovered late [C]
│   │   └── Handling: automated per-aggregate parity report is a cutover gate
│   │
│   ├── F11.5 Cost of dual publishing (2× storage, 2× throughput) — DEGRADE [C]
│   │   └── Handling: bounded migration window with deadline; capacity planned before start
│   │
│   └── F11.6 Downstream DLQ/parking state on v1 at cutover — LOSS of resolution [C]
│       └── Handling: all parked keys on v1 resolved or migrated to v2 parking before v1 consumer stops
│
├── 12. SNAPSHOTS, EVENT STORE & READ MODELS
│   ├── 12.1 Snapshots carry: snapshot schema id + aggregate version + upcaster revisions used
│   ├── F12.1 Snapshot schema evolves incompatibly — rebuild storm [C]
│   │   └── Handling: snapshot schema versioned; incompatible change → invalidate and rebuild lazily from events
│   ├── F12.2 Snapshot built with buggy upcaster (F10.3) — CORRUPT [C]
│   │   └── Handling: invalidate snapshots by upcaster revision
│   ├── F12.3 Read model built from mixed upcaster revisions — CORRUPT [L]
│   │   └── Handling: read model records revision set; rebuild when set changes for T0/T1
│   └── F12.4 Event store retention forever → readability horizon forever (EV-07) [C]
│       └── Handling: retired schemas kept read-only in registry + exported registry snapshots with archive
│
├── 13. DEPRECATION & SUNSET  (event type or schema version)
│   ├── 13.1 Phases
│   │   ├── S1 Announce: deprecation record (what, why, replacement, deadline) in registry metadata + consumer registry
│   │   ├── S2 Instrument: producer emits deprecation header; consumers' libraries log usage of deprecated types
│   │   ├── S3 Migrate: consumers move to replacement; tracked per consumer with attestation
│   │   ├── S4 Stop producing: producer stops emitting deprecated type/version
│   │   ├── S5 Readability window: deprecated schema remains readable for EV-07 horizon
│   │   └── S6 Retire: schema marked retired (read-only, not deleted) once no retained byte references it
│   │
│   ├── 13.2 Retirement gate (all must be true)
│   │   ├── No live topic data with that schema id (retention elapsed since last emission)
│   │   ├── No outbox backlog with that id
│   │   ├── No quarantined/parked events with that id
│   │   ├── No event store / archive / snapshot referencing that id — or archive includes exported schema
│   │   ├── No consumer in registry declaring dependency
│   │   └── Backups policy reviewed (restores may resurrect old ids)
│   │
│   ├── F13.1 Schema deleted from registry — LOSS of readability [C]
│   │   ├── Impact: quarantined envelopes, archives, backups unreadable; DR replays fail
│   │   └── Handling: never hard-delete; retired state only; registry export stored with archives
│   │
│   ├── F13.2 "Zero active consumers" via connection counts — wrong decision [C]
│   │   └── Handling: consumer registry + field/type usage telemetry + attestations
│   │
│   ├── F13.3 Archive to columnar format loses original bytes — LOSS of fidelity [C]
│   │   ├── Impact: Parquet conversion re-serializes; original schema id and exact bytes gone
│   │   └── Handling: archive raw envelopes (bytes + id); columnar copy is an additional analytics derivative
│   │
│   └── F13.4 Deadline missed by a critical consumer — business pressure to break anyway [C]
│       └── Handling: deadline extension is an explicit decision recorded; producer continues S3 state
│
├── 14. READABILITY HORIZON & RETENTION
│   ├── Horizon = max of: Kafka retention, outbox max backlog age, DLQ max unresolved age,
│   │                     event store retention, archive retention, backup retention, regulatory replay needs
│   ├── Upcasters and schemas for every version inside horizon are maintained and tested
│   └── F14.1 Horizon unknown → teams delete upcasters "because nobody uses v1" — OUTAGE on replay/restore [C]
│       └── Handling: horizon published per stream; upcaster removal gated on it
│
├── 15. REGISTRY OPERATIONS, DR & MULTI-CLUSTER
│   ├── 15.1 Registry HA: replicated storage, multiple instances, backed up with point-in-time restore
│   ├── 15.2 DR: same ids in DR (replicate registry storage) — EV-11
│   ├── F15.1 Separate registries per cluster with different ids — OUTAGE after failover/mirroring [C]
│   │   ├── Impact: mirrored bytes reference ids that don't exist or point to different schemas in DR
│   │   └── Handling: single logical registry replicated; or mirror-time id translation (complex; avoid)
│   ├── F15.2 Registry restored from older backup — ids missing / reused [C]
│   │   ├── Impact: newer ids absent; dangerous if id sequence reissues ids for different schemas
│   │   └── Handling: id sequence never reset; restore verified against id inventory exported daily
│   ├── F15.3 Compatibility rule changed (e.g., to NONE to unblock a deploy) — CORRUPT later [C]
│   │   └── Handling: rule changes require architecture approval; audited; alert on any rule change
│   ├── F15.4 Registry ACL too broad — unreviewed schemas [C]
│   │   └── Handling: writes only from C-GOV pipeline identity
│   └── F15.5 Environment drift (staging lineage ≠ prod lineage) — false-green CI [C]
│       └── Handling: CI checks compatibility against PRODUCTION registry lineage (read-only)
│
├── 16. GOVERNANCE PIPELINE (C-GOV)
│   ├── G1 Lint: naming, namespaces, docs on every field, unit annotation on money/quantity, PII tags,
│   │            no floats for money, enum defaults, reserved numbers (proto), null-first unions (avro)
│   ├── G2 Compatibility: FULL_TRANSITIVE dry-run against production lineage
│   ├── G3 Change class label + semantic checkbox (§5); SEMANTIC/STREAM routes to architecture review
│   ├── G4 Upcaster completeness: chain exists from every lineage version to new current
│   ├── G5 Golden tests: stored sample events of every version → upcast → expected output
│   ├── G6 Consumer-driven contract tests: each registered consumer's expectations run against new schema
│   ├── G7 Quarantine sample test: current quarantined/parked envelopes upcast successfully
│   ├── G8 Promotion: register in prod registry → verify id → only then producer deploy proceeds
│   └── F16.1 Pipeline bypass (manual registry change) — CORRUPT [C]
│       └── Handling: registry write ACL; audit alert on any non-pipeline registration
│
├── 17. INTERACTIONS WITH OTHER TREES
│   ├── Outbox: bytes + schema id stored at write; relay byte-transparent; no inferred schemas
│   ├── Idempotent consumers: aggregate version independent of schema version; upcast before version guard
│   ├── DLQ: unknown schema id / missing upcaster → C1/C2 quarantine; redrive gated on upcaster tests (G7)
│   └── Reconciliation: comparisons done on upcasted canonical model, never on raw bytes across versions
│
├── 18. SECURITY & COMPLIANCE
│   ├── PII fields tagged in schema → drive encryption/crypto-shredding and masking in triage tools
│   ├── F18.1 PII field added without tag — COMPLIANCE [C]
│   │   └── Handling: G1 lint requires classification on every new field
│   ├── F18.2 Removing a PII field from schema doesn't remove it from retained data — COMPLIANCE [C]
│   │   └── Handling: erasure via crypto-shredding of per-subject keys, not schema change
│   └── F18.3 Schema docs leak sensitive business logic to broad audiences [L]
│       └── Handling: registry read ACL scoped by domain where needed
│
├── 19. OBSERVABILITY
│   ├── Version mix per stream: % of events by schema id (producer side and consumer side)
│   ├── Newer-than-reader events per consumer (upgrade urgency)
│   ├── Upcast rate, chain length, upcast latency, upcast failures (must be 0)
│   ├── Unknown schema id count (page on any, T0)
│   ├── Header/id mismatch count
│   ├── Deprecated type emission and consumption counts
│   ├── Registry: availability, lookup latency, cache hit rate, rule changes, registrations (audit)
│   ├── Semantic drift: distribution of key numeric fields per schema id (detect unit changes)
│   └── Absence alerts on all of the above
│
├── 20. RUNBOOKS
│   ├── RB-01 Unknown schema id in production → identify source (registry env, mirror, deletion) → restore id → redrive
│   ├── RB-02 Missing upcaster discovered → hotfix upcaster through G4/G5 → redrive quarantined/parked
│   ├── RB-03 Semantic drift alert → freeze consumers of field → confirm producer change → correction plan
│   ├── RB-04 Bad schema registered (bypassed review) → mark disabled for new writes → producer rollback → lineage note
│   ├── RB-05 Upcaster bug → new revision → identify projections built with old revision → rebuild/resync
│   ├── RB-06 Stream migration cutover → parity → per-aggregate cutover → v1 drain → contraction
│   └── RB-07 Registry outage → producers/consumers run from pinned/cached ids → no new registrations → restore
│
└── 21. GO-LIVE TEST MATRIX
    ├── Mixed producer versions emit interleaved schema ids for one aggregate → consumer applies all in version order
    ├── Producer ahead of consumer (new optional field) → consumer ignores, no failure
    ├── Producer rollback after new schema emitted → old consumers and new consumers both succeed
    ├── New enum value → old consumer takes UNKNOWN path, no crash
    ├── v1 event replayed through full chain → equals golden v-current output
    ├── Upcaster fed generated random valid inputs → never throws, never alters identity fields
    ├── Unknown schema id → single event quarantined; 50+ keys → systemic halt
    ├── Registry down at producer startup → producer starts with pinned ids
    ├── Registry down during consumption → cached ids keep consuming
    ├── DR failover with mirrored topics → every message decodes with same ids
    ├── Retired schema → quarantined envelope from 6 months ago still decodes and upcasts
    ├── Expand/contract rename → readers switch without any failure window
    ├── Stream migration v1→v2 → parity 100%, cutover by aggregate version, zero duplicates or gaps
    ├── Semantic change attempted under same field name → blocked at G3 review
    └── Compatibility rule change attempt → alert + blocked without approval
```

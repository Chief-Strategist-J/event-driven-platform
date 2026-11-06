# Schema Evolution — Business Logic Decision Tree

```SCHEMA EVOLUTION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (verify per registry/library version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
Scope: evolution mechanics. Taxonomy, upcasters, stream migration → Event Versioning tree (authoritative).
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-REG   Schema registry (Apicurio) — artifacts per event type, compatibility rules, ids, references, metadata
│   ├── C-GOV   Governance pipeline (CI) — lint, transitive compatibility vs PROD lineage, golden samples, contract tests
│   ├── C-CRG   Consumer registry — consumers per stream, supported schema versions, subscription, run cadence
│   ├── C-CGN   Code generation — per language, pinned generator versions, generated artifacts versioned
│   ├── C-SER   Producer serialization library — explicit schema, embedded id, no auto-register
│   ├── C-DES   Consumer deserialization library — raw bytes, writer-schema resolution, reader schema, upcast hook
│   ├── C-TEL   Field-usage telemetry — which consumers read which fields/types (drives contraction decisions)
│   ├── C-SMP   Golden sample store — real (masked) production payloads per schema version, incl. quarantined ones
│   └── C-MON   Monitoring — version mix, deserialization failures by fingerprint, newer-than-reader counts
│
├── 1. INVARIANTS  (with enforcement)
│   ├── SE-01 Compatibility is transitive and checked against production lineage [C]
│   │   └── Enforced by: registry rule FULL_TRANSITIVE per artifact; CI dry-run against PROD registry (read-only)
│   ├── SE-02 Registry rules cannot be loosened silently [C]
│   │   └── Enforced by: rule changes only via governance pipeline with architecture approval; audit alert on change
│   ├── SE-03 No auto-registration in production [C]
│   │   └── Enforced by: producer config + registry ACL (only C-GOV identity can register)
│   ├── SE-04 Defaults never invent business facts [C]
│   │   └── Enforced by: lint — optional fields default null; non-null defaults require "true for all history" justification
│   ├── SE-05 Meaning never changes under an existing name [C]
│   │   └── Enforced by: semantic-change classification gate (versioning tree §6); unit/scale metadata lint
│   ├── SE-06 Deploy order follows the change class, enforced by the pipeline [C]
│   │   └── Enforced by: producer release blocked until C-CRG shows all consumers declare support (when required)
│   ├── SE-07 Decommission is gated on consumer registry + subscription/usage telemetry, never on lag [C]
│   │   └── Enforced by: retirement checklist (§8.5) evaluated by tooling
│   ├── SE-08 Retained bytes stay readable [C]
│   │   └── Enforced by: schemas never deleted; readability horizon gate (versioning tree §14)
│   ├── SE-09 Headers and keys are contracts too [C]
│   │   └── Enforced by: header/key contracts versioned in the stream contract; changes go through C-GOV
│   ├── SE-10 Every language decodes identically [C]
│   │   └── Enforced by: cross-language round-trip tests with golden samples in CI
│   └── SE-11 Schema errors are classified single vs systemic before handling [C]
│       └── Enforced by: consumer runtime + DLQ systemic detector
│
├── 2. COMPATIBILITY MODES — SEMANTICS & DEPLOY ORDER
│   ├── 2.1 Definitions [C]
│   │   ├── BACKWARD — new schema can read data written with the previous schema → upgrade CONSUMERS first
│   │   ├── FORWARD  — previous schema can read data written with the new schema → upgrade PRODUCERS first
│   │   ├── FULL     — both directions → any order
│   │   ├── *_TRANSITIVE — checked against ALL previous versions, not just the latest
│   │   └── NONE     — no checks (forbidden for business streams)
│   │
│   ├── 2.2 Why FULL_TRANSITIVE for event streams
│   │   ├── Producers and consumers deploy independently (either order happens)
│   │   ├── Rollbacks happen (new → old producer: old readers meet new data; new readers meet old data)
│   │   ├── Replays, archives, DLQ redrives, lagging consumers meet ANY historical version
│   │   └── Non-transitive modes allow v3 ⟂ v1 while v3 ~ v2 ~ v1 pairwise
│   │
│   ├── 2.3 When a weaker mode is acceptable (documented exception)
│   │   └── BACKWARD_TRANSITIVE: consumers strictly always deploy first AND no producer rollbacks — rare; requires SE-06 gating
│   │
│   ├── F2.1 BACKWARD (non-transitive) on event streams — old data unreadable by new readers after 2+ hops — OUTAGE on replay [C]
│   ├── F2.2 Mode chosen without deploy-order implications — producer ships first under BACKWARD — consumers fail [C]
│   ├── F2.3 Rule set to NONE "temporarily" to unblock a release — lineage poisoned permanently — CORRUPT [C]
│   │   └── Handling: SE-02; any NONE window requires architecture incident record and full lineage re-validation
│   └── F2.4 Compatibility checked per artifact but consumers read multiple artifacts as one stream — gaps [L]
│       └── Handling: stream-level contract test covering all event types on the topic
│
├── 3. CHANGE CATALOGUE BY FORMAT  (structural; semantics in §4)
│   ├── 3.1 Avro
│   │   ├── Add field with default → FULL-compatible [C]; default should be null for "not recorded" (SE-04)
│   │   ├── Add field without default → breaks BACKWARD (new reader, old data) [C]
│   │   ├── Remove field that had default → FULL-compatible [C]
│   │   ├── Remove field without default → breaks FORWARD (old reader expects it) [C]
│   │   ├── Rename → reader-side alias supports renames [C]; still treat as expand/contract across languages [L]
│   │   ├── Type promotion (int→long, float→double, int→double) → reader-side only; reverse breaks [C]
│   │   ├── Enum: add symbol → old readers fail unless enum default declared [C]
│   │   ├── Union: add branch → old readers fail on new branch values [C]; null must be first for null defaults [C]
│   │   ├── Logical types (decimal precision/scale, timestamp units) → treat as breaking [C]
│   │   └── Nested records/maps/arrays → same rules recursively; defaults for records must be complete [C]
│   │
│   ├── 3.2 Protobuf
│   │   ├── Add field with new number → compatible [C]
│   │   ├── Remove field → reserve number AND name forever [C]
│   │   ├── Reuse field number → silent misinterpretation — CORRUPT [C]
│   │   ├── int32 ↔ int64 / uint ↔ int → wire-compatible but values truncate or change sign in old readers [C]
│   │   ├── Change field type otherwise → breaking [C]
│   │   ├── Scalar presence: proto3 implicit presence makes unset = zero → use explicit presence ("optional") [C]
│   │   ├── Enum: unknown values preserved/handled differently per language [L]; UNSPECIFIED = 0 mandatory
│   │   ├── oneof: adding new members OK; moving existing fields into/out of a oneof → unsafe [L]
│   │   ├── repeated packed/unpacked → parsers accept both [C]
│   │   └── Message rename / package change → affects type URLs and registry artifact naming → breaking for references [L]
│   │
│   ├── 3.3 JSON Schema
│   │   ├── Compatibility depends on open vs closed content model [C]
│   │   ├── Adding properties to closed model breaks old validators; removing from open model hides data [C]
│   │   ├── Numbers as floats in most runtimes → money/IDs as strings (outbox tree INV-12) [C]
│   │   └── Weakest automated checking → contract tests mandatory; avoid for T0 streams [L]
│   │
│   ├── F3.1 Avro enum without default + new symbol — every old consumer fails — OUTAGE [C]
│   ├── F3.2 Protobuf number reuse after deletion — old events decoded into wrong field — CORRUPT [C]
│   ├── F3.3 int32 → int64 widened for amounts — old consumers truncate large values — CORRUPT financial [C]
│   ├── F3.4 Decimal scale changed (2 → 4) — values misread ×100 — CORRUPT financial [C]
│   └── F3.5 Proto3 implicit presence on business fields — "0" vs "not set" indistinguishable — CORRUPT [C]
│
├── 4. WHAT THE REGISTRY DOES NOT CHECK  (must be covered elsewhere)
│   ├── 4.1 Semantics: units, currency, scale, timezone, inclusive/exclusive, sign conventions → semantic gate (SE-05)
│   ├── 4.2 Default meaning: "0" vs "unknown" vs "none" → lint + review (SE-04)
│   ├── 4.3 Enum value meaning changes → frozen semantics documentation per value
│   ├── 4.4 Cross-field invariants (total = sum of lines) → contract tests + business invariant checks
│   ├── 4.5 Headers: names, formats, meanings (event_id, tenant, schema hints) → header contract (SE-09)
│   ├── 4.6 Keys: format and partitioning → partitioning tree; key change = stream migration
│   ├── 4.7 Ordering/aggregate boundaries → versioning tree §5 (STREAM changes)
│   ├── 4.8 PII classification and encryption tags → multi-tenant tree §9
│   └── F4.1 "Registry passed" treated as "safe to ship" — semantic/header/key breaks reach production — CORRUPT [C]
│
├── 5. CI / CD PIPELINE  (C-GOV)
│   ├── G1 Lint: naming, docs on every field, unit/scale metadata, null defaults, enum defaults/UNSPECIFIED,
│   │            reserved numbers, no floats for money, PII tags, no non-null fact defaults
│   ├── G2 Change classification label (MINOR / EXPAND-CONTRACT / SEMANTIC / STREAM) — versioning tree §5
│   ├── G3 Transitive compatibility dry-run against PRODUCTION lineage (SE-01)
│   ├── G4 Golden samples: decode every stored sample (all versions, incl. quarantined/parked) with new reader;
│   │                      encode new samples and decode with every supported old reader
│   ├── G5 Cross-language round-trip: encode in each producer language, decode in each consumer language (SE-10)
│   ├── G6 Consumer-driven contract tests from C-CRG consumers
│   ├── G7 Upcaster completeness (versioning tree §10.2)
│   ├── G8 Registration: register in each environment in order (dev → staging → prod) via pipeline identity;
│   │      verify returned id/version; producer artifact pins the verified schema
│   ├── G9 Release gating: producer deploy allowed only after G8 in prod AND (if required) consumer readiness (SE-06)
│   │
│   ├── F5.1 Compatibility checked only in staging registry — prod lineage differs — false green [C]
│   ├── F5.2 Golden samples synthetic only — real payload shapes (nulls, large arrays, edge enums) untested [C]
│   │   └── Handling: masked production samples refreshed per release
│   ├── F5.3 Schema registered in prod after producer deploy — serialization failures at runtime — OUTAGE [C]
│   └── F5.4 Pipeline bypass (manual registry UI change) — unreviewed schema in lineage — CORRUPT [C]
│       └── Handling: SE-03 ACL; audit alert on non-pipeline registrations
│
├── 6. ROLLOUT SEQUENCING
│   ├── 6.1 By change class
│   │   ├── MINOR (add nullable field, FULL-compatible) → any order; consumers using the field deploy after producer
│   │   ├── EXPAND/CONTRACT → multi-release (§7), each phase gated
│   │   ├── SEMANTIC → new field/type; treated as EXPAND/CONTRACT with explicit semantic notes
│   │   └── STREAM → new stream version (§8)
│   │
│   ├── 6.2 Consumer readiness (SE-06)
│   │   ├── Consumers declare supported schema versions in C-CRG (from their compiled reader + upcasters)
│   │   ├── Producer release checks: every registered consumer supports the new writer schema
│   │   └── Offline/batch/DR consumers included (run cadence recorded)
│   │
│   ├── 6.3 Producer canary
│   │   ├── New schema emitted by a canary subset first (mixed versions on stream are normal under FULL)
│   │   ├── Watch: consumer deserialization failures by fingerprint, newer-than-reader counts, business invariants
│   │   └── Abort: roll back producer (FULL guarantees old/new readers both OK)
│   │
│   ├── 6.4 Rollback rules
│   │   ├── Producer rollback always safe under FULL_TRANSITIVE
│   │   ├── Consumer rollback safe if old consumer still supports all writer versions now on the stream
│   │   └── Events already written with new schema remain forever — rollback never "removes" them
│   │
│   ├── F6.1 Consumer using new field deploys before producer — reads null — wrong business decision [C]
│   │   └── Handling: consumers treat null as "not yet available", never as a value
│   ├── F6.2 Rollback of consumer that can't read events written meanwhile — OUTAGE [C]
│   └── F6.3 Batch consumer runs monthly with old reader — first run after change fails [C]
│       └── Handling: 6.2 includes run cadence; pre-run compatibility check job
│
├── 7. EXPAND / CONTRACT DETAILS  (rename, type change, required ↔ optional, restructure)
│   ├── 7.1 Phases (versioning tree §8): expand → migrate readers → verify → contract writes → contract schema
│   ├── 7.2 Required → optional: add new optional field; dual-write; readers migrate; deprecate old (never flip in place)
│   ├── 7.3 Optional → required: impossible for historical data; enforce at producer validation, schema stays optional
│   ├── 7.4 Type change: new field with new type; producer writes both from one source value (property-tested equality)
│   ├── 7.5 Verification uses C-TEL field-usage telemetry or consumer attestations — never connection counts or lag
│   ├── 7.6 Contract-schema phase only within readability horizon rules (SE-08)
│   │
│   ├── F7.1 Old and new fields written inconsistently during expand — consumers disagree — CORRUPT [C]
│   ├── F7.2 Field removed while a replay/archive consumer still reads it — replay OUTAGE [C]
│   └── F7.3 "Optional → required" flipped in schema — old data unreadable — OUTAGE [C]
│
├── 8. BREAKING CHANGE — NEW STREAM VERSION  (versioning tree §11 authoritative)
│   ├── 8.1 Topic: "<domain>.<aggregate>.v<N+1>" (aggregate stream; never per event type)
│   ├── 8.2 Artifacts registered fresh for v<N+1> (new lineage); old lineage retained read-only
│   ├── 8.3 Dual publish from the same outbox transaction (or derived-stream transformer); same event lineage ids
│   ├── 8.4 Consumer cutover
│   │   ├── New consumer group for v<N+1> with explicit start positions (recorded at dual-publish start)
│   │   ├── Shadow processing + parity; per-aggregate cutover by aggregate version (no reset policy involved)
│   │   └── Old group stopped only after new group applied ≥ old group's versions for all aggregates
│   ├── 8.5 Decommission gate for v<N> (SE-07) — all must hold
│   │   ├── C-CRG: zero registered consumers on v<N> (including batch/DR/replay/analytics)
│   │   ├── Broker-side: zero active consumer groups subscribed to v<N> over [L: 14–30 days], incl. ad-hoc groups
│   │   ├── C-TEL: zero reads of v<N> events in that window
│   │   ├── Owners of every former consumer attested migration
│   │   └── Announcement + final deadline passed; stop dual publish; topic retained until retention/readability horizon
│   ├── 8.6 Archive: raw envelopes (bytes + schema id + offsets) — not columnar re-serialization (replay tree §3.3)
│   │
│   ├── F8.1 Lag-based decommission — caught-up v<N> consumers silently starved — LOSS [C]
│   ├── F8.2 Subscription switch without start position — mass replay or skipped window [C]
│   ├── F8.3 Dual publish to per-event-type topics — ordering broken in new stream — CORRUPT [C]
│   ├── F8.4 Fixed 30-day sunset regardless of consumer state — forced breakage — OUTAGE downstream [C]
│   │   └── Handling: deadline + gate; extension is an explicit recorded decision (versioning tree F13.4)
│   └── F8.5 "Upcaster v1→v2" assumed possible for a redesign — information missing in v1 — CORRUPT [C]
│       └── Handling: classify: lossless (upcaster) vs lossy (snapshot-based migration / UNKNOWN fields)
│
├── 9. CONSUMER DESERIALIZATION & TOLERANT READER
│   ├── 9.1 Pipeline: raw bytes → detect format/id → resolve writer schema (cached) → decode to reader schema →
│   │                upcast → handler (versioning tree §9.1)
│   ├── 9.2 Tolerant reader rules (corrected)
│   │   ├── Decode only needed fields (reader schema projection) — unknown fields ignored [C]
│   │   ├── Missing optional fields → explicit UNKNOWN / null semantics, never invented "domain defaults" (SE-04)
│   │   ├── Unknown enum values → explicit UNKNOWN path (documented per field)
│   │   ├── Unknown event types on the stream → per-handler policy: skip + record, or hold (never crash)
│   │   └── Business decisions on UNKNOWN values → explicit rules (reject, hold, or neutral behavior)
│   │
│   ├── 9.3 Failure classification (SE-11)
│   │   ├── Single message (rare corruption, single bad producer instance) → quarantine raw bytes + hold key
│   │   ├── Systemic (fingerprint across many keys in window) → halt consumer group + one page (DLQ tree F4.4)
│   │   └── Unknown schema id → registry/environment issue → halt or hold (versioning tree F9.1)
│   │
│   ├── F9.1 DLQ-and-continue without key hold — later versions applied, ordering broken — CORRUPT [C]
│   ├── F9.2 Page per message during systemic failure — alert storm — slow response [C]
│   ├── F9.3 Domain default substituted for missing field (e.g., currency, country) — fabricated facts — CORRUPT [C]
│   └── F9.4 Unknown enum mapped to a real value (e.g., default case → "ACTIVE") — wrong state — CORRUPT [C]
│
├── 10. HEADERS, KEYS & ENVELOPE EVOLUTION  (SE-09)
│   ├── 10.1 Header contract: names, encodings, required/optional, meaning — versioned alongside stream contract
│   ├── 10.2 Header changes: additive only; renames via expand/contract; consumers tolerate missing optional headers
│   ├── 10.3 Key format changes → partitioning change → new stream version (partitioning tree PT-03/PT-04)
│   ├── 10.4 Envelope fields (event_id, aggregate version, tenant, occurred_at) frozen; never repurposed
│   │
│   ├── F10.1 Header renamed by a framework/connector upgrade — idempotency/tenant routing broken — CORRUPT [L]
│   │   └── Handling: header contract tests in upgrade pipelines (CDC tree §11.2)
│   └── F10.2 Key format "cleanup" (case normalization) — same aggregate on two partitions — CORRUPT [C]
│
├── 11. SHARED TYPES & SCHEMA REFERENCES
│   ├── 11.1 Shared types (Money, Address, Tenant envelope) referenced by many artifacts [L]
│   ├── 11.2 References pin specific versions; upgrading a shared type is a change to every referencing artifact
│   ├── 11.3 Shared type changes run G3–G6 for ALL referencing artifacts before release
│   ├── F11.1 Shared type changed once, breaks dozens of streams — wide OUTAGE [C]
│   └── F11.2 Copy-pasted "shared" types drift per service — semantic divergence (two Money definitions) — CORRUPT [C]
│       └── Handling: single source of shared types; lint forbids local redefinitions
│
├── 12. CODE GENERATION & MULTI-LANGUAGE
│   ├── 12.1 Generator versions pinned per language; regenerated artifacts reviewed as part of schema PR
│   ├── 12.2 Behavior differences to test (SE-10): unknown field preservation, enum unknowns, default materialization,
│   │        presence semantics, decimal/time mapping, map ordering, max sizes
│   ├── 12.3 Generated code published as versioned packages; consumers upgrade deliberately
│   ├── F12.1 Go and Kotlin generators map optional/presence differently — one language reads "0", other "unset" — CORRUPT [L]
│   ├── F12.2 Generator upgrade changes field naming/casing in JSON mapping — downstream JSON contracts break [L]
│   └── F12.3 Stale generated package in a consumer — newer fields silently dropped on re-serialization — LOSS [C]
│       └── Handling: consumers never re-serialize events they don't own; forwarders preserve raw bytes
│
├── 13. DOWNSTREAM SINK COUPLING  (schema changes ripple beyond consumers)
│   ├── 13.1 Materialized views: new fields need view schema expand + version-guarded backfill (views tree §10)
│   ├── 13.2 Search mappings: new field types fixed at first index → mapping conflicts on type changes (reindex)
│   ├── 13.3 Warehouse/lake tables: column adds safe; type changes need new columns; partitioning columns frozen
│   ├── 13.4 Raw CDC topics: DB DDL becomes event schema change (CDC tree §7.6)
│   ├── F13.1 Search index receives field with conflicting type — indexing failures for all documents — OUTAGE [C]
│   └── F13.2 Warehouse silently casts changed type — analytics corrupted — CORRUPT [C]
│
├── 14. RETAINED DATA READABILITY  (SE-08; versioning tree §13–§14)
│   ├── Locations holding old schema ids: Kafka (retention), outbox backlog, quarantine/held stores, archives,
│   │   event stores, snapshots, backups, replay targets
│   ├── Schemas never hard-deleted; retired = read-only; registry exports stored with archives
│   ├── Golden samples (C-SMP) kept for every version inside horizon
│   └── F14.1 Schema deleted after "no consumers" — DLQ redrive and DR replay fail — OUTAGE/LOSS [C]
│
├── 15. GOVERNANCE & COMMUNICATION
│   ├── Schema owner per artifact (producer team); consumer list from C-CRG
│   ├── Change announcements: class, timeline, consumer actions required, deadline
│   ├── Deprecation metadata in registry (replacement, deadline, contact)
│   ├── Breaking changes require architecture approval + migration plan + rollback plan
│   └── Post-change review: incidents, consumer friction, time-to-migrate metrics
│
├── 16. OBSERVABILITY
│   ├── Version mix per stream (producer side and consumer side)
│   ├── Newer-than-reader events per consumer (upgrade urgency)
│   ├── Deserialization failures by fingerprint; single vs systemic classification
│   ├── UNKNOWN enum / UNKNOWN field occurrences per consumer
│   ├── Unknown schema id count (page on any for T0)
│   ├── Registry: rule changes, registrations by identity, lookup errors, cache hit rates
│   ├── Dual-publish health (both streams emitting equal counts per window)
│   ├── Decommission gate status per old stream
│   └── Absence alerts on all of the above
│
├── 17. RUNBOOKS
│   ├── RB-01 Systemic deserialization failure after deploy → halt consumers → identify producer release → roll back producer
│   │         → resume → verify no parked keys remain
│   ├── RB-02 Single malformed event → quarantine + key hold → producer investigation → redrive after fix
│   ├── RB-03 Compatibility rule change detected → revert rule → audit registrations made during window
│   ├── RB-04 Shared type change failing downstream → pin previous version in references → coordinated rollout
│   ├── RB-05 Stream v<N> decommission → evaluate §8.5 gate → extension or proceed → retain topic to horizon
│   └── RB-06 Search mapping conflict → stop fan-out → new index with corrected mapping → reindex from authoritative view
│
└── 18. TEST MATRIX
    ├── v3 vs v1 incompatibility (with v2 bridging) → caught by transitive check in CI
    ├── Staging lineage differs from prod → CI uses prod lineage; mismatch caught
    ├── Add enum symbol without default → lint blocks
    ├── Protobuf field number reuse → lint blocks
    ├── int32 → int64 amount change → classified SEMANTIC/STREAM, not MINOR
    ├── Producer canary emits new schema → old and new consumers both succeed; rollback verified
    ├── Monthly batch consumer with old reader → pre-run compatibility job blocks or upgrades it
    ├── Missing optional field → UNKNOWN path, no fabricated default
    ├── Systemic bad schema deploy → halt + single page, not mass DLQ
    ├── Cross-language golden samples → identical decoded values in Go and Kotlin
    ├── Header rename by library upgrade → contract test fails in pipeline
    ├── Stream v1→v2 migration → explicit start positions, version-based cutover, zero gaps
    ├── Decommission attempt with a caught-up v1 consumer still subscribed → gate blocks
    ├── DLQ envelope from retired schema → still decodes and upcasts
    └── Shared type change → all referencing artifacts validated before release
```

```
SCHEMA EVOLUTION — v3: NESTED DECISION PATHS & EDGE CASES
Legend  : [C] Certain   [L] Likely (verify per library/version)   [G] Guessing
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Edge node format: Scenario → Impact → Handling → Detection
│
├── 1. CHANGE INTAKE & PATH RESOLUTION
│   └── Trigger: Schema PR opened (any artifact, any shared type)
│       │
│       ├── Check 1.1: WHERE does the change live? (express every change as a path)
│       │   ├── P-TOP      Top-level field ............................ "status"
│       │   ├── P-NESTED   Field inside nested record/message ......... "shipping.address.city"
│       │   ├── P-ITEM     Field inside collection element ............ "lines[*].unit_price"
│       │   ├── P-MAPVAL   Field inside map value ..................... "attributes{*}.value"
│       │   ├── P-MAPKEY   Map key type/format ........................ "attributes{key}"
│       │   ├── P-BRANCH   Field inside union / oneof / variant ....... "payment<card>.last4"
│       │   ├── P-SHARED   Field inside a shared referenced type ...... "Money.amount" (used at N paths)
│       │   ├── P-OPAQUE   Content inside bytes/string blob ........... "metadata_json" (schema-less)
│       │   └── P-ENVELOPE Header / key / envelope field ............... see v2 §10
│       │
│       ├── Check 1.2: EXPAND the change to every concrete path it affects
│       │   ├── P-SHARED → enumerate all artifacts AND all paths referencing the type
│       │   │   ├── Same type used at depth 1 and depth 4 → evaluate each path separately
│       │   │   ├── Same type inside an array AND as a singular field → both rule sets apply
│       │   │   └── Edge: type referenced transitively (A → B → Money) → include transitive referrers
│       │   │       └── F1.1 Shared type change validated only on direct referrers — transitive streams break — OUTAGE [C]
│       │   │           ├── Handling: registry reference graph walked transitively in CI
│       │   │           └── Detection: CI report lists every (artifact, path) evaluated
│       │   ├── P-OPAQUE → registry cannot validate → route to §8 (opaque content governance)
│       │   └── Recursive types → path is unbounded → §3.6
│       │
│       ├── Check 1.3: WHO reads each affected path? (C-TEL path-level usage + C-CRG declarations)
│       │   ├── No consumer reads path ............ lower risk (still FULL_TRANSITIVE)
│       │   ├── Some consumers read path .......... list them; contract tests per consumer
│       │   ├── Unknown (no telemetry) ............ treat as "all consumers read it"
│       │   └── Path used in keys, filters, field masks, search mappings, view columns → §9 coupling
│       │
│       ├── Check 1.4: WHAT structurally changes at the path? → §2 (fields), §3 (nesting), §4 (collections),
│       │                                                          §5 (unions/polymorphism), §6 (enums), §7 (special types)
│       ├── Check 1.5: Does MEANING change anywhere along the path? → §8 (semantic, incl. parent meaning)
│       └── Check 1.6: Classification → MINOR | EXPAND-CONTRACT | SEMANTIC | STREAM → rollout §10
│
├── 2. FIELD-LEVEL CHANGES  (apply at ANY depth; nested context adds §3 conditions)
│   │
│   ├── 2.1 ADD FIELD
│   │   ├── Check: format?
│   │   │   ├── Avro
│   │   │   │   ├── Has default?
│   │   │   │   │   ├── NO → breaks new readers on old data (BACKWARD) [C] → reject
│   │   │   │   │   └── YES → Check: default type
│   │   │   │   │       ├── null (union null-first) → MINOR [C]
│   │   │   │   │       ├── non-null scalar → is it TRUE for all historical events?
│   │   │   │   │       │   ├── YES (provably) → MINOR with justification
│   │   │   │   │       │   └── NO / unsure → reject (SE-04 fabricated fact)
│   │   │   │   │       └── record/array/map default → see §3.1 (full-object defaults)
│   │   │   │   └── Edge: parent record is itself optional (null in some old events)
│   │   │   │       └── New field resolves only when parent present; consumers must handle parent-null path [C]
│   │   │   ├── Protobuf
│   │   │   │   ├── New, never-used field number? → YES → MINOR [C] | NO (reused) → reject (F3.2 v2)
│   │   │   │   ├── Scalar business field? → explicit presence required (unset ≠ zero) [C]
│   │   │   │   └── Added inside a oneof? → §5.2
│   │   │   └── JSON Schema
│   │   │       ├── Parent object open (additionalProperties allowed — default) → old validators accept [C]
│   │   │       ├── Parent object closed → old validators reject new data → breaking [C]
│   │   │       └── Added to nested "required" list → breaks old data validation → reject
│   │   │
│   │   └── Edge cases
│   │       ├── E2.1a Field added to shared type at path used as map value → every map entry gains it; size growth [C]
│   │       ├── E2.1b Field added with same name as a removed historical field (Avro by name) — old data resolves
│   │       │         into new field with old meaning — CORRUPT [C]
│   │       │         └── Handling: removed names reserved forever (lint: name history per record)
│   │       └── E2.1c Field added that consumers must use for correctness (e.g., currency) — old events lack it
│   │                 └── Handling: SEMANTIC; explicit UNKNOWN handling; producer backfill via correction events if needed
│   │
│   ├── 2.2 REMOVE FIELD
│   │   ├── Check: consumers read path? (1.3)
│   │   │   ├── YES/UNKNOWN → EXPAND-CONTRACT (deprecate → verify zero reads → stop writing → remove)
│   │   │   └── NO (telemetry-proven) → still requires: old readers have default (Avro) / number reserved (proto)
│   │   ├── Avro: removed field must have had a default in old reader schemas, else old readers fail on new data [C]
│   │   ├── Proto: reserve number AND name [C]; JSON mapping clients also lose the field
│   │   └── Edge cases
│   │       ├── E2.2a Field removed from nested record inside array — old readers of every element affected [C]
│   │       ├── E2.2b Field removed but still referenced by view field-mask / search mapping / view column — §9
│   │       └── E2.2c Field removed that participates in content hash / dedupe / idempotency — duplicates or misses
│   │                 └── Handling: hash/idempotency definitions list their input paths; CI blocks removal
│   │
│   ├── 2.3 RENAME FIELD
│   │   ├── Check: format
│   │   │   ├── Avro → reader-side aliases resolve old name [C]; but every language/tool must support aliases [L]
│   │   │   ├── Proto → wire uses numbers (rename wire-safe) [C], BUT breaks:
│   │   │   │   ├── JSON mapping (field names in JSON) [C]
│   │   │   │   ├── FieldMask paths (names) [C]
│   │   │   │   └── Generated code (source break for consumers)
│   │   │   └── JSON → always breaking (name is the key)
│   │   ├── Decision → EXPAND-CONTRACT by default (add new, dual-write, migrate, deprecate old)
│   │   └── Edge cases
│   │       ├── E2.3a Rename of a nested field used in a FieldMask/patch event — patches silently apply nothing — LOSS [C]
│   │       ├── E2.3b Rename in shared type → rename at every referencing path (1.2) — partial adoption — CORRUPT [C]
│   │       └── E2.3c Case-only rename (unitPrice → unit_price) — JSON mappers treat as different fields [C]
│   │
│   ├── 2.4 TYPE CHANGE (scalar → scalar)
│   │   ├── Avro promotion table (reader side) [C]
│   │   │   ├── int → long, float, double ✓ | long → float, double ✓ | float → double ✓
│   │   │   ├── string ↔ bytes ✓ (encoding assumptions!) 
│   │   │   └── anything else ✗
│   │   ├── Protobuf [C]
│   │   │   ├── int32 ↔ int64 ↔ uint32 ↔ uint64 ↔ bool: wire-compatible, values TRUNCATE/sign-change in old readers
│   │   │   ├── sint* ↔ int*: NOT compatible (zigzag) 
│   │   │   ├── string ↔ bytes: compatible only for valid UTF-8
│   │   │   └── fixed32 ↔ sfixed32, fixed64 ↔ sfixed64: compatible
│   │   ├── Decision: any widening of a business value (amounts, counts, ids) → SEMANTIC/EXPAND-CONTRACT
│   │   │             (old readers truncate; promotion is one-directional)
│   │   └── Edge cases
│   │       ├── E2.4a int → long for amounts inside array elements — old consumers truncate per element — CORRUPT financial [C]
│   │       ├── E2.4b string → bytes where producers send non-UTF-8 — new decoders fail — OUTAGE [C]
│   │       └── E2.4c float → double "harmless" promotion — old events retain float rounding; consumers assume double precision [C]
│   │
│   ├── 2.5 SCALAR ↔ STRUCTURED
│   │   ├── scalar → record/message (wrap "address" string into Address record) → breaking all formats [C]
│   │   ├── record → scalar (flatten) → breaking [C]
│   │   └── Decision → new field with new structure (EXPAND-CONTRACT); upcaster maps old scalar → new structure
│   │       only if lossless parsing is guaranteed (E2.5a)
│   │       └── E2.5a Upcaster parses legacy free-text into structure — heuristic parsing fabricates components — CORRUPT [C]
│   │           └── Handling: structure fields UNKNOWN for legacy; keep original text field
│   │
│   ├── 2.6 CARDINALITY (singular ↔ repeated)
│   │   ├── Protobuf [C]
│   │   │   ├── string/bytes/message: repeated data read by singular reader → LAST element taken
│   │   │   │   (messages: merged) — silent data loss in old readers
│   │   │   └── numeric packed/unpacked nuances [L]
│   │   ├── Avro → breaking (array vs scalar types differ) [C]
│   │   ├── JSON → breaking (array vs value) [C]
│   │   └── Decision → new repeated field (EXPAND-CONTRACT); never change cardinality in place
│   │       └── E2.6a Proto singular → repeated "because wire-compatible" — old consumers see only last item — LOSS [C]
│   │
│   └── 2.7 OPTIONALITY & DEFAULT CHANGES
│       ├── optional → required: never in schema (historical data) → producer-side validation only (v2 §7.3)
│       ├── required → optional: EXPAND-CONTRACT (old readers expect value)
│       ├── Default value change (Avro): changes resolved value for ALL historical events lacking the field [C]
│       │   └── E2.7a Default changed from null to "INR" — every historical event retroactively gains a currency — CORRUPT [C]
│       │       └── Handling: defaults immutable once registered (lint compares with lineage)
│       └── Proto default (proto2 explicit defaults / proto3 zero values) changes → semantic changes for unset fields [C]
│
├── 3. NESTED RECORD / MESSAGE STRUCTURE
│   │
│   ├── 3.1 ADD NESTED RECORD FIELD
│   │   ├── Avro: default for record-typed field must be a complete object or null (union) [C]
│   │   │   ├── Null default (union null-first) → MINOR ✓
│   │   │   └── Full-object default → every historical event gains a fully populated sub-record — fabricated — reject
│   │   ├── Proto: new message field → absent in old data (has-presence) ✓ [C]
│   │   └── Edge cases
│   │       ├── E3.1a Nested record added with required inner fields — consumers construct defaults when absent
│   │       │         → fake "empty" objects treated as real — CORRUPT [C]
│   │       │         └── Handling: absent parent = UNKNOWN; consumers never instantiate defaults for missing records
│   │       └── E3.1b Nested record added inside array elements — mixed old/new elements in same event impossible,
│   │                 but mixed across events → per-event handling
│   │
│   ├── 3.2 MOVE FIELD BETWEEN LEVELS (flatten / nest / re-parent)
│   │   ├── Example: "city" moves from top-level to "shipping.address.city"
│   │   ├── All formats → breaking (different path) [C]
│   │   ├── Decision → EXPAND-CONTRACT: write both locations from one source value; readers migrate; remove old
│   │   ├── Upcaster: path mapping old → new (pure, lossless)
│   │   └── Edge cases
│   │       ├── E3.2a Move into an array element ("city" → "addresses[0].city") — cardinality AND path change —
│   │       │         which element is "primary"? — semantic ambiguity — CORRUPT [C]
│   │       │         └── Handling: explicit role field (address.type = SHIPPING); never positional meaning
│   │       └── E3.2b Field moved into optional parent — absence now ambiguous (field absent vs parent absent) [C]
│   │                 └── Handling: consumers distinguish parent-null vs field-null explicitly
│   │
│   ├── 3.3 SPLIT / MERGE STRUCTURES
│   │   ├── Split one record into two siblings (customer → customer + contact) → EXPAND-CONTRACT
│   │   ├── Merge two siblings into one record → EXPAND-CONTRACT
│   │   └── E3.3a Split changes ownership semantics (contact now separately updatable) — updates that touch only
│   │             the new sub-record don't bump parent-level version used by consumers — CORRUPT [C]
│   │             └── Handling: version semantics documented per sub-structure (§8.4)
│   │
│   ├── 3.4 RENAME TYPE / NAMESPACE / PACKAGE
│   │   ├── Avro: reader matches named types (record/enum/fixed) by FULL NAME (namespace + name); needs aliases [C]
│   │   ├── Proto: message rename affects Any type URLs, registry artifact naming, generated code; wire unaffected [C]
│   │   ├── Registry: artifact id derived from record name strategy → rename = NEW artifact lineage (compat not checked!) [C]
│   │   └── E3.4a Namespace "cleanup" refactor — new artifact lineage created silently — compatibility checks skipped —
│   │             consumers fail to resolve — OUTAGE [C]
│   │             └── Handling: CI flags any change of fully qualified name as STREAM-level review
│   │
│   ├── 3.5 OPTIONAL PARENT CHAINS (a?.b?.c)
│   │   ├── Each level's presence must be explicit (null vs absent vs empty object)
│   │   ├── E3.5a Producer switches from "omit parent" to "send empty parent" — consumers treat empty object as
│   │   │         real data (e.g., empty address overwrites existing address in view) — CORRUPT [C]
│   │   │         └── Handling: contract states presence semantics per level; contract tests on null/empty/absent
│   │   └── E3.5b Partial-state events (patch) vs full-state events at nested level — see §8.3
│   │
│   └── 3.6 RECURSIVE TYPES (trees: category → subcategories, org → children)
│       ├── Evolution rules apply at every recursion level simultaneously
│       ├── E3.6a Recursion depth exceeds parser limits (protobuf default recursion limit ~100) — decode failure — OUTAGE [C]
│       │         └── Handling: producer-side depth cap; consumers configured consistently; prefer adjacency lists
│       ├── E3.6b Codegen/tools mishandle recursive Avro/JSON schemas (some languages) — build or runtime failures [L]
│       └── E3.6c Adding a field to recursive record with non-null default — default applied at every level — size explosion [C]
│
├── 4. COLLECTIONS
│   │
│   ├── 4.1 ARRAYS / REPEATED — ELEMENT EVOLUTION
│   │   ├── Element scalar type change → promotion rules apply to items (Avro) [C]; proto truncation per element [C]
│   │   ├── Element record evolves → all §2/§3 rules recursively per element
│   │   ├── Check: what if ONE element fails decoding/validation (unknown enum, bad value)?
│   │   │   ├── Policy A: whole event rejected → quarantine + key hold (consistency preserved) [default]
│   │   │   └── Policy B: drop element → NEVER for business data (silent partial event) — CORRUPT [C]
│   │   └── Edge cases
│   │       ├── E4.1a Element identity missing (line items without stable ids) — consumers diff by position —
│   │       │         reordering looks like changes — CORRUPT [C]
│   │       │         └── Handling: stable element ids; adding ids later requires backfill semantics for old events
│   │       ├── E4.1b Order significance changed (list → set semantics) without schema change — SEMANTIC [C]
│   │       ├── E4.1c Duplicate elements allowed vs not — consumers dedupe differently — CORRUPT [C]
│   │       └── E4.1d Nullable elements (array of nullable records) introduced — consumers iterate and crash — OUTAGE [C]
│   │
│   ├── 4.2 NULL vs EMPTY vs ABSENT COLLECTION
│   │   ├── Three meanings: "not provided" vs "known empty" vs "unknown"
│   │   ├── E4.2a Producer changes "omit field" to "send []" — view overwrites existing items with empty list — LOSS [C]
│   │   └── Handling: contract declares semantics; full-state vs patch semantics explicit (§8.3)
│   │
│   ├── 4.3 SIZE GROWTH
│   │   ├── Unbounded arrays grow with business (order with 10k lines, audit trails)
│   │   ├── E4.3a Event exceeds message size limit — poison at producer or relay — OUTAGE [C]
│   │   │         └── Handling: max items per event in contract; claim-check above size; split into multiple events
│   │   └── E4.3b Changing to paginated/split events later — consumers assume single complete event — SEMANTIC/STREAM [C]
│   │
│   ├── 4.4 MAPS
│   │   ├── Key types
│   │   │   ├── Avro: keys always strings [C]
│   │   │   ├── Proto: keys integral or string only; values cannot be maps; maps cannot be repeated [C]
│   │   │   └── JSON: keys strings [C]
│   │   ├── Map ↔ repeated key/value message (proto) → wire-equivalent [C]; JSON mapping and code differ — EXPAND-CONTRACT
│   │   ├── Array ↔ map (keyed by element id) → breaking (Avro/JSON); EXPAND-CONTRACT
│   │   └── Edge cases
│   │       ├── E4.4a Key normalization change (case, trimming, locale) — duplicate logical keys — CORRUPT [C]
│   │       ├── E4.4b Map iteration order assumed stable — hashes/diffs differ across languages — false drift [C]
│   │       │         └── Handling: canonical ordering in hashing (monitoring tree F17.4)
│   │       ├── E4.4c User-controlled map keys indexed in search — mapping explosion — OUTAGE [C]
│   │       └── E4.4d Map value type evolves (record) — rules recursively per value
│   │
│   └── 4.5 NESTED COLLECTIONS (array of maps of arrays…)
│       ├── Every level adds a presence/cardinality/identity question — document per level
│       └── E4.5a Upcaster mutates nested collection in place across shared references — aliasing bugs in some languages [L]
│                 └── Handling: upcasters build new structures (immutable transformation)
│
├── 5. UNIONS, ONEOF, POLYMORPHISM
│   │
│   ├── 5.1 AVRO UNIONS
│   │   ├── Add branch → old readers fail on data using the new branch [C] → EXPAND-CONTRACT / new field
│   │   ├── Remove branch → new readers fail on old data using it [C]
│   │   ├── Reorder branches → resolution by type/name, but default must match FIRST branch [C]
│   │   │   └── E5.1a Moving null from first position — null default becomes invalid — registration or read failure [C]
│   │   ├── Union of records → resolved by full name (§3.4) [C]
│   │   └── E5.1b Two records in union with similar shapes — wrong branch chosen after rename without alias — CORRUPT [C]
│   │
│   ├── 5.2 PROTOBUF ONEOF
│   │   ├── Add new member → old readers see unknown field, oneof appears unset [C]
│   │   │   └── E5.2a Old consumer treats "oneof unset" as a valid state (e.g., no payment method) — CORRUPT [C]
│   │   │       └── Handling: explicit UNKNOWN-variant handling distinct from "not set"
│   │   ├── Remove member → reserve number and name [C]
│   │   ├── Move existing field into/out of oneof → unsafe (data loss/merge semantics) [L]
│   │   └── E5.2b Two members of the same oneof set across mixed-version writers — last one wins — LOSS [C]
│   │
│   ├── 5.3 DISCRIMINATOR POLYMORPHISM (type field + variant payload)
│   │   ├── New variant → old readers receive unknown discriminator
│   │   │   ├── Policy per handler: hold (if variant affects correctness) | skip + record (if irrelevant)
│   │   │   └── Never default to a known variant
│   │   ├── Variant schema evolves independently → each variant has own lineage/compatibility
│   │   └── E5.3a Discriminator value renamed — all old events become "unknown variant" — OUTAGE/CORRUPT [C]
│   │
│   └── 5.4 NULLABLE UNION OF EVOLVING RECORD
│       └── E5.4a Record inside nullable union gains required field — old readers fail only when non-null — intermittent failures [C]
│
├── 6. ENUMS AT ANY DEPTH
│   ├── Add symbol
│   │   ├── Avro: needs enum default on reader (every nested enum, not just top-level) [C]
│   │   ├── Proto3: open enums keep unknown integer values [C]; proto2 closed enums treat unknown as unknown field [C]
│   │   └── E6a New symbol inside array elements — old consumer handles each element's UNKNOWN per policy (§4.1)
│   ├── Remove symbol → old data with symbol breaks new readers (Avro) [C] → never remove; deprecate
│   ├── Rename symbol → breaking (Avro by name; proto JSON by name) [C]
│   ├── Renumber (proto) → CORRUPT (values remapped) [C] → forbidden
│   ├── Change meaning of a symbol → SEMANTIC (frozen semantics per value)
│   └── E6b Enum used as map key or partition/filter attribute — new symbol creates unseen partitions/buckets downstream [C]
│
├── 7. SPECIAL TYPES (at any depth)
│   ├── Decimal: precision/scale change → breaking (scale reinterpretation) [C]; money as minor units preferred
│   ├── Timestamps: unit (millis ↔ micros) → breaking ×1000 errors [C]; local vs UTC → SEMANTIC
│   ├── Dates vs timestamps: conversion → SEMANTIC (timezone boundary)
│   ├── Fixed (Avro): size change → breaking [C]
│   ├── UUID: string vs bytes representation → breaking across languages [C]
│   ├── Large integers in JSON (> 2^53) → precision loss in many parsers [C] → strings
│   └── E7a Nested timestamp unit changed in one variant only — partial ×1000 errors hard to spot — CORRUPT [C]
│
├── 8. SEMANTIC EDGE CASES IN NESTED DATA  (registry-invisible)
│   ├── 8.1 Meaning changes at nested paths (unit, currency, inclusive/exclusive) — same rules as top-level
│   ├── 8.2 Parent meaning change (e.g., "address" now billing, was shipping) changes meaning of all children [C]
│   ├── 8.3 Full-state vs patch semantics per nested structure
│   │   ├── Full-state: nested object replaces previous value entirely
│   │   ├── Patch: only present fields change; absence = unchanged; explicit clear needs a marker
│   │   ├── FieldMask/path-based patches reference field NAMES → renames break patches (E2.3a) [C]
│   │   └── E8.3a Producer switches nested structure from patch to full-state (or reverse) — views wipe or
│   │             retain stale data — CORRUPT [C] → SEMANTIC change, new event type or explicit mode field
│   ├── 8.4 Versioning granularity
│   │   ├── Aggregate version bumps on any nested change (default)
│   │   └── E8.4a Sub-entities (line items) edited independently with their own versions — consumers assume
│   │             aggregate version covers them — stale sub-entities overwrite newer — CORRUPT [C]
│   ├── 8.5 Derived totals vs nested items (total must equal sum of lines)
│   │   └── E8.5a Item amount semantics change (tax-inclusive) without total change — invariant breaks — CORRUPT [C]
│   │       └── Handling: business invariant checks (monitoring tree §20) run on new schema in canary
│   └── 8.6 OPAQUE CONTENT (JSON in string/bytes, JSONB columns via CDC)
│       ├── Registry can't validate → separate JSON Schema contract for the blob, validated at producer
│       ├── E8.6a Teams evolve blob content freely — consumers parse ad hoc — silent breakage — CORRUPT [C]
│       └── Handling: forbid opaque blobs in T0 contracts; or govern blob schema like any artifact
│
├── 9. COUPLING TO DOWNSTREAM STRUCTURES  (nested paths used beyond deserialization)
│   ├── Check: is the changed path used in…
│   │   ├── Content hashes / idempotency fingerprints → hash definition update + dual-hash window [C]
│   │   ├── View columns / denormalized fields → views tree §10 (expand + versioned backfill)
│   │   ├── Search mappings (nested/object types) → type conflicts need reindex [C]
│   │   ├── Field masks / patches → §8.3
│   │   ├── Partition keys / routing / filters → partitioning tree (stream change)
│   │   ├── Encryption tags (PII at nested path) → encryption path must move with field (multi-tenant tree §9)
│   │   └── Warehouse nested columns (struct/array types) → type evolution limits per warehouse [L]
│   └── E9a Field moved to new nested path but PII tag left at old path — new location stored unencrypted — COMPLIANCE [C]
│
├── 10. ROLLOUT & RUNTIME EDGE CASES
│   ├── 10.1 Mixed versions on the stream (normal under FULL) — consumers handle any supported version per event
│   ├── 10.2 Upcasters for nested changes
│   │   ├── Traverse every element of every collection at the path (not just the first)
│   │   ├── Handle parent-null at each level
│   │   ├── Pure and total (property tests generate nested/edge structures: empty, null, max depth, max size)
│   │   └── E10.2a Upcaster handles top-level but misses the same shared type inside array elements — partial upcast —
│   │             CORRUPT [C] → Handling: upcasters generated/registered per (type, path) from 1.2 path expansion
│   ├── 10.3 Re-serialization by intermediaries
│   │   ├── E10.3a Forwarder decodes with old reader and re-encodes — unknown nested fields dropped — LOSS [C]
│   │   │   (proto preserves unknown fields in many runtimes [L]; Avro and JSON-to-struct mappings generally do not)
│   │   └── Handling: intermediaries forward raw bytes; never decode-modify-encode events they don't own
│   ├── 10.4 Partial upgrades across languages
│   │   └── E10.4a Go consumer upgraded, Kotlin consumer not — different handling of new nested variant — divergent views [C]
│   │       └── Handling: C-CRG readiness per consumer (v2 §6.2), language-agnostic golden tests
│   └── 10.5 Rollback with nested changes
│       └── E10.5a Rollback of producer after nested additions — old events keep new nested data; consumers that
│                 started using it must handle its disappearance in newer events (absent again) [C]
│
├── 11. CLASSIFICATION SUMMARY (output of §1 for a PR)
│   ├── MINOR: nullable additions at any depth with no semantic impact, no downstream coupling hits
│   ├── EXPAND-CONTRACT: renames, moves, cardinality, optionality, type changes, map/array conversions, union/oneof additions
│   ├── SEMANTIC: units/scale/timezone/parent meaning/patch-vs-full/list-vs-set/default changes
│   └── STREAM: key/partitioning changes, type full-name lineage changes needing new artifacts, aggregate re-modeling,
│               format changes
│
└── 12. NESTED-STRUCTURE TEST MATRIX
    ├── Path expansion: change in shared Money → CI evaluates every (artifact, path), incl. transitive referrers
    ├── Add nullable field inside array element record → old and new readers decode mixed events
    ├── Add nested record with full-object default → lint rejects
    ├── Remove nested field used in content hash → CI blocks
    ├── Rename nested proto field used in FieldMask → contract test fails
    ├── int32 → int64 inside repeated amounts → classified SEMANTIC, not MINOR
    ├── Proto singular → repeated → classified EXPAND-CONTRACT (last-value-wins test demonstrates loss)
    ├── Move field into optional parent → consumers distinguish parent-null vs field-null
    ├── Producer sends empty parent instead of omitting → contract test catches overwrite behavior
    ├── Recursive structure at max depth → decodes in every language within limits
    ├── New enum symbol inside array element → whole-event policy applied, no element dropped
    ├── Avro union: null moved from first branch → registration rejected
    ├── New oneof member → old consumer takes UNKNOWN path, not "unset"
    ├── Discriminator rename → blocked as STREAM-level review
    ├── Map key normalization change → duplicate logical keys detected in contract tests
    ├── Array grows past max items → producer splits/claim-checks; no oversize poison
    ├── Upcaster over nested arrays with nulls/empties/max sizes → property tests pass, identity fields unchanged
    ├── Intermediary forwarder → raw bytes preserved, unknown nested fields intact
    ├── PII field moved to nested path → encryption follows the field
    └── Go and Kotlin decode golden nested samples → identical canonical values
```
# Multi-Tenant Event Isolation — Business Logic Decision Tree

```
MULTI-TENANT EVENT ISOLATION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-TRG  Tenant registry — tenant id, tier, region/residency, retention class, keys, quotas, lifecycle state
│   │   ├── C-TCX  Tenant context — derived from authenticated identity at the API; immutable per request
│   │   ├── C-OBX  Producer outbox — stamps tenant id server-side; validates consistency before insert
│   │   ├── C-TOP  Topology manager — pooled / dedicated / silo placement per tenant; provisioning via IaC
│   │   ├── C-KMS  Key hierarchy — tenant KEK (in KMS/HSM) → data-subject / period DEKs (wrapped)
│   │   ├── C-CRY  Crypto service/library — field-level encryption, DEK cache, shred-aware decrypt
│   │   ├── C-RT   Consumer runtime — per-record tenant scope, consistency checks, tenant-fair lanes
│   │   ├── C-QOS  Tenant quota & fairness — app-level rate limits, lane shares, held/DLQ budgets
│   │   ├── C-RES  Residency controller — region pinning, replication policy per tenant/topic
│   │   ├── C-ERA  Erasure orchestrator — subject erasure, tenant offboarding, plaintext inventory, evidence
│   │   └── C-MON  Monitoring — per-tenant SLOs, leak detectors, audit trail
│   │
│   ├── 0.2 Tenancy models
│   │   ├── M-POOL     Shared topics, shared consumers, tenant in key/header/payload (default)
│   │   ├── M-DEDICATED Per-tenant topics (per aggregate stream), shared or dedicated consumer groups
│   │   └── M-SILO     Per-tenant cluster / region / database (residency, regulatory, very large tenants)
│   │
│   └── 0.3 Ownership
│       ├── Platform: registry, topology, keys, crypto library, quotas, residency, erasure orchestration
│       ├── Domain teams: tenant-correct handlers, projections with tenant scoping, effect tenant routing
│       ├── Security: leak detection, SIEM, key policies, access reviews
│       └── Legal/compliance: residency rules, retention classes, erasure SLAs, evidence requirements
│
├── 1. INVARIANTS  (with enforcement)
│   ├── MT-01 Tenant identity is server-derived and immutable per event [C]
│   │   └── Enforced by: outbox stamps tenant from C-TCX / owning aggregate row; client-supplied values ignored
│   ├── MT-02 Tenant identity is consistent across key, header, payload, and owning aggregate [C]
│   │   └── Enforced by: producer check before outbox insert (fails business tx); consumer check on ingest
│   ├── MT-03 Every event has exactly one owning tenant [C]
│   │   └── Enforced by: schema requires tenant field; cross-tenant interactions modeled as separate events per tenant
│   ├── MT-04 No silent drops [C]
│   │   └── Enforced by: mismatches → integrity quarantine + security alert; drop path does not exist in code
│   ├── MT-05 Per-record tenant scope; no ambient context reuse [C]
│   │   └── Enforced by: scope object created per record, passed explicitly; lint bans thread/goroutine-local tenant state
│   ├── MT-06 Every derived store is tenant-scoped [C]
│   │   └── Enforced by: tenant column + row-level security; tenant-prefixed cache keys; tenant-filtered search aliases
│   ├── MT-07 Isolation is defense in depth [C]
│   │   └── Enforced by: at least two independent controls on every read path (e.g., query scoping + RLS)
│   ├── MT-08 Residency is a placement constraint [C]
│   │   └── Enforced by: C-RES refuses placement/replication violating tenant region policy
│   ├── MT-09 Personal data is encrypted per data subject; identifiers in keys/headers are non-personal [C]
│   │   └── Enforced by: schema PII tags → field-level encryption; lint forbids PII in keys/headers/ids
│   ├── MT-10 Erasure covers every plaintext copy [C]
│   │   └── Enforced by: plaintext inventory per tenant/subject; erasure job checklist; evidence record
│   ├── MT-11 Fairness: no tenant can consume more than its share of shared capacity [C]
│   │   └── Enforced by: app-level quotas at edge and producer; tenant-fair lanes; per-tenant budgets
│   ├── MT-12 Tenant ids are never reused [C]
│   │   └── Enforced by: registry tombstones retired ids permanently
│   └── MT-13 Tier/region moves are stream migrations [C]
│       └── Enforced by: migration runbook with version-based cutover (versioning tree §11)
│
├── 2. TENANCY MODEL SELECTION
│   ├── 2.1 Decision
│   │   ├── Residency or regulatory separation required? → M-SILO
│   │   ├── Contractual isolation (dedicated capacity, custom retention, independent replay) → M-DEDICATED
│   │   ├── Tenant throughput > [G: X% of pooled topic capacity] sustained → M-DEDICATED
│   │   └── Otherwise → M-POOL
│   │
│   ├── 2.2 Topic naming (aggregate streams; never per event type)
│   │   ├── M-POOL: "<domain>.<aggregate>.v<major>"
│   │   ├── M-DEDICATED: "t.<tenant_id>.<domain>.<aggregate>.v<major>"
│   │   └── M-SILO: same as pooled, in the tenant's dedicated cluster/region
│   │
│   ├── 2.3 Capacity reality
│   │   ├── Topic and partition counts grow with dedicated tenants; cluster metadata limits apply [L]
│   │   ├── Consumer groups: shared groups subscribing to many dedicated topics vs per-tenant groups (cost)
│   │   └── Budget: max dedicated tenants per cluster [G]
│   │
│   ├── F2.1 Per-event-type topics per tenant — ordering broken + topic explosion [C]
│   │   └── Handling: aggregate-stream topics only
│   │
│   ├── F2.2 Tenant tier changes treated as config flip — reorder/duplicates during move [C]
│   │   └── Handling: MT-13; §12.3 migration
│   │
│   └── F2.3 Regex subscriptions to "t.*" pick up topics of tenants not meant for this consumer [C]
│       └── Handling: explicit subscription sets driven by registry; ACLs
│
├── 3. TENANT IDENTITY & PROPAGATION
│   ├── 3.1 Source of truth
│   │   ├── Commands: tenant from authenticated principal (token claims validated server-side)
│   │   ├── Aggregate: tenant column on aggregate row (immutable after creation)
│   │   └── Outbox stamping: tenant read from aggregate row in the same transaction
│   │
│   ├── 3.2 Consistency rule at producer (MT-02)
│   │   └── request tenant = aggregate tenant = key prefix = header = payload field → else fail tx
│   │
│   ├── 3.3 Async chain propagation
│   │   ├── Consumer emitting downstream events copies tenant from the SOURCE EVENT, not from runtime context
│   │   ├── Correlation/causation ids carry across; tenant never changes along a causation chain
│   │   └── Scheduled jobs and batch processors: tenant explicit per unit of work
│   │
│   ├── 3.4 Cross-tenant interactions (marketplaces, B2B, transfers between tenants)
│   │   ├── Model as separate events per tenant (each owned, encrypted, retained by its tenant)
│   │   ├── Shared reference only by opaque ids (no other tenant's personal data in payload)
│   │   └── Platform/system events (no tenant): explicit "platform" pseudo-tenant with its own policies
│   │
│   ├── F3.1 Client-supplied tenant header trusted — tenant spoofing — SECURITY [C]
│   │   └── Handling: MT-01
│   │
│   ├── F3.2 Ambient context bleed across lanes (reused worker context) — events processed under wrong tenant [C]
│   │   ├── Impact: writes into other tenant's projection; effects sent to wrong tenant's customers
│   │   ├── Handling: MT-05; per-record scope; tests that interleave tenants on one lane
│   │   └── Detection: consumer consistency check (record tenant vs scope tenant) at write time
│   │
│   ├── F3.3 Admin/support tools act "as tenant" without audit — SECURITY [C]
│   │   └── Handling: impersonation is an explicit, audited mode with reason and expiry
│   │
│   ├── F3.4 Aggregate moved between tenants (account transfer) — tenant mutability [C]
│   │   └── Handling: forbidden in place; model as close-in-A + create-in-B with lineage reference
│   │
│   └── F3.5 Events missing tenant from legacy producers — rejected at outbox, business tx fails [C]
│       └── Handling: backfill at source; never default to a tenant
│
├── 4. PARTITIONING & ORDERING PER TENANT
│   ├── 4.1 Key = tenant_id + separator + aggregate_id (pooled) — ordering per aggregate, tenant-aware keys
│   ├── 4.2 Never key by tenant alone (whole tenant in one partition → hot partition) [C]
│   ├── 4.3 Large tenants in pooled topics spread across partitions by aggregate; per-tenant lanes provide fairness
│   │
│   ├── F4.1 Key format inconsistent between producers (separator, case, encoding) — split ordering [C]
│   │   └── Handling: one key builder library; partition-affinity tests (ordering tree F5.3)
│   │
│   ├── F4.2 Hot tenant with a hot aggregate — partition saturation — DEGRADE others on that partition [C]
│   │   └── Handling: tenant/aggregate quotas; move tenant to M-DEDICATED; aggregate re-model
│   │
│   └── F4.3 Personal data in key (email as aggregate id) — survives shredding — COMPLIANCE [C]
│       └── Handling: MT-09; opaque ids only
│
├── 5. ACCESS CONTROL LAYERS  (what each layer can and cannot enforce)
│   ├── 5.1 Kafka ACLs
│   │   ├── Can: restrict principals by topic name/prefix and consumer group [C]
│   │   ├── Cannot: restrict records within a topic by tenant [C]
│   │   └── Use: M-DEDICATED/M-SILO isolation; service-level least privilege on pooled topics
│   │
│   ├── 5.2 Service authorization
│   │   └── Services processing pooled topics are trusted multi-tenant components (higher review bar)
│   │
│   ├── 5.3 Projections (PostgreSQL)
│   │   ├── Tenant column on every row; composite keys lead with tenant
│   │   ├── Row-level security policies bound to session tenant setting (second control, MT-07)
│   │   └── Read APIs set tenant from authenticated context per request/transaction
│   │
│   ├── 5.4 Search (OpenSearch)
│   │   ├── Shared index: mandatory tenant filter via filtered aliases or query rewriting layer
│   │   ├── Large/dedicated tenants: per-tenant index/alias
│   │   └── Aggregations and suggestions also tenant-filtered (common leak path)
│   │
│   ├── 5.5 Caches
│   │   └── Tenant-prefixed keys; cache wrapper rejects unprefixed keys
│   │
│   ├── F5.1 Relying on Kafka ACLs for pooled isolation — false assurance — SECURITY [C]
│   │
│   ├── F5.2 RLS bypassed by privileged connection role (superuser / owner bypass) — SECURITY [C]
│   │   └── Handling: application roles not owners; force RLS on tables; tests as application role
│   │
│   ├── F5.3 Search aggregation/autocomplete without tenant filter — cross-tenant leak — SECURITY [C]
│   │   └── Handling: query layer enforces filter for all query types; leak tests
│   │
│   ├── F5.4 Cache key collision across tenants — wrong tenant's data served — SECURITY [C]
│   │   └── Handling: 5.5; cache wrapper; leak canaries (§15)
│   │
│   └── F5.5 Connection pool reuses session with previous tenant setting — RLS context leak — SECURITY [C]
│       └── Handling: tenant set per transaction (transaction-scoped setting), reset on return
│
├── 6. CONSUMER-SIDE ENFORCEMENT
│   ├── 6.1 Per record
│   │   ├── Extract tenant from header, key prefix, payload; verify all equal (MT-02)
│   │   ├── Verify tenant exists and lifecycle state allows processing (ACTIVE / SUSPENDED rules §12.2)
│   │   ├── Verify residency: record's tenant allowed in this region/cluster (MT-08)
│   │   ├── Build per-record tenant scope → handler → writes carry tenant explicitly
│   │   └── Write-time guard: every DB write's tenant = scope tenant (library-enforced)
│   │
│   ├── 6.2 Mismatch handling (MT-04)
│   │   ├── Quarantine (DLQ tree) with class INTEGRITY-TENANT; key held
│   │   ├── Security alert to SIEM with event id, producer, offsets (no payload plaintext)
│   │   └── Producer incident opened; never auto-redriven
│   │
│   ├── F6.1 Silent drop on mismatch — LOSS + evidence destroyed [C]
│   ├── F6.2 Unknown tenant (not in registry yet — onboarding race) — misclassified as attack [C]
│   │   └── Handling: registry replication before tenant activation; short causal wait; then quarantine
│   └── F6.3 Event for offboarded tenant arrives (late, replay) — processing resurrects data — COMPLIANCE [C]
│       └── Handling: OFFBOARDED state → skip with record (data deliberately not processed); counted
│
├── 7. NOISY NEIGHBOR & FAIRNESS
│   ├── 7.1 Quotas (MT-11)
│   │   ├── Edge: per-tenant request rate and concurrency
│   │   ├── Producer: per-tenant outbox emission rate (bulk imports throttled at source)
│   │   ├── Kafka quotas apply per user/client id, not per tenant inside shared producers [C]
│   │   │   → tenant fairness must be application-level in pooled model
│   │   └── Consumer: per-tenant concurrency share across lanes (weighted fair scheduling)
│   │
│   ├── 7.2 Per-tenant budgets
│   │   ├── Held keys / parked events / quarantine items per tenant
│   │   ├── Retry traffic per tenant
│   │   └── Budget breach → tenant-level throttling/halt, others unaffected
│   │
│   ├── 7.3 Priority
│   │   ├── VIP/SLA tenants: dedicated lanes or M-DEDICATED with dedicated consumer capacity
│   │   └── Priority cannot reorder within a partition (ordering fixed)
│   │
│   ├── F7.1 One tenant's bulk import saturates shared partitions — DEGRADE all [C]
│   │   └── Handling: source throttling + bulk lane/topic
│   │
│   ├── F7.2 Tenant-level halt implemented as partition pause — halts other tenants on same partitions [C]
│   │   └── Handling: tenant halt = hold that tenant's keys (held store), partitions keep flowing
│   │
│   └── F7.3 Per-tenant metrics cardinality explodes monitoring — observability OUTAGE [C]
│       └── Handling: per-tenant metrics only for top-N + VIP; long tail aggregated; per-tenant detail in logs/traces
│
├── 8. DATA RESIDENCY & SOVEREIGNTY
│   ├── 8.1 Registry fields: home region, allowed regions, replication allowed (yes/no/where)
│   ├── 8.2 Placement
│   │   ├── Residency-bound tenants → M-SILO or regional pooled topics in their region
│   │   ├── DR replication only to allowed regions
│   │   └── Processing services for those tenants run in-region
│   │
│   ├── 8.3 Derived data
│   │   └── Analytics/warehouse/ML pipelines respect residency (common violation path)
│   │
│   ├── F8.1 Pooled topic mirrored cross-region for DR — residency violation — COMPLIANCE [C]
│   ├── F8.2 Logs/traces with payload snippets shipped to central region — COMPLIANCE [C]
│   │   └── Handling: no payload data in logs; regional log stores for residency tenants
│   ├── F8.3 Third-party processors (email/SMS providers) outside region — COMPLIANCE [L]
│   │   └── Handling: provider routing per tenant region
│   └── F8.4 Support/engineering access from other jurisdictions — COMPLIANCE [L]
│       └── Handling: access policies by region; audited break-glass
│
├── 9. ENCRYPTION & KEY MANAGEMENT
│   ├── 9.1 Key hierarchy
│   │   ├── Tenant KEK — in KMS/HSM; never leaves it
│   │   ├── Data-subject DEK — per end-user (or per subject + period); wrapped by tenant KEK
│   │   ├── Tenant-level DEK — for tenant business data not tied to a person (optional)
│   │   └── Wrapped DEKs stored in a key table (not in events); event carries DEK reference id
│   │
│   ├── 9.2 Field-level encryption
│   │   ├── Only PII-tagged fields encrypted (schema tags; versioning tree §18)
│   │   ├── Envelope stays clear: event id, tenant id, aggregate id (opaque), versions, event type, schema id
│   │   └── Schema registry sees encrypted fields as bytes with metadata (compatibility still checkable)
│   │
│   ├── 9.3 Decryption
│   │   ├── Consumers call crypto library: unwrap DEK via KMS (permissioned), cache plaintext DEK briefly
│   │   ├── Consumers never hold KEKs; KMS permission per service and per tenant scope
│   │   └── Shredded DEK → field returns "ERASED" marker (not an exception)
│   │
│   ├── 9.4 Caching vs erasure latency
│   │   ├── DEK cache TTL [L: ≤ 5–15 min] bounds how long shredded data stays decryptable
│   │   └── Shred broadcast invalidates caches immediately (best effort; TTL is the guarantee)
│   │
│   ├── 9.5 Rotation
│   │   ├── Rotate KEKs (re-wrap DEKs; data untouched)
│   │   ├── New DEKs for new periods; old DEKs retained until data they protect is expired or erased
│   │   └── Immutable Kafka data is never re-encrypted in place
│   │
│   ├── 9.6 Availability
│   │   ├── KMS outage → decrypt fails → ENVIRONMENTAL (pause; never quarantine) (retry tree §6)
│   │   ├── KMS rate limits → DEK caching + batching; capacity planned for replay peaks
│   │   └── Multi-region keys only where residency allows
│   │
│   ├── F9.1 Encrypting whole payload with tenant master key — no subject-level erasure, key sprawl [C]
│   │   └── Handling: 9.1 hierarchy
│   │
│   ├── F9.2 Consumers fetch and hold master keys — compromise of any consumer exposes all tenant history — SECURITY [C]
│   │   └── Handling: 9.3
│   │
│   ├── F9.3 Accidental KEK/DEK deletion — mass irreversible LOSS [C]
│   │   ├── Handling: deletion only via C-ERA workflow with approvals; KMS deletion protection/waiting period;
│   │   │             canary decrypt checks; key inventory reconciliation
│   │   └── Detection: decrypt failures spike for a tenant outside an erasure workflow → page
│   │
│   ├── F9.4 Per-event KMS calls — latency, cost, throttling — DEGRADE [C]
│   │   └── Handling: DEK caching; one DEK per subject/period, not per event
│   │
│   ├── F9.5 Encrypted fields break upcasting/parity logic — CORRUPT [C]
│   │   └── Handling: upcasters operate on clear envelope + decrypted fields via library; ERASED marker handled
│   │
│   └── F9.6 Key-store backups restore deleted keys — erasure undone — COMPLIANCE [C]
│       └── Handling: key-store backup policy aligned with erasure (expiry window documented; restores re-apply erasure log)
│
├── 10. ERASURE
│   ├── 10.1 Two distinct operations
│   │   ├── E-SUBJECT: one person's data within a tenant (right to erasure)
│   │   └── E-TENANT: tenant offboarding (contract end, deletion request)
│   │
│   ├── 10.2 E-SUBJECT procedure
│   │   ├── Step 1 Verify request, legal holds, retention obligations (some data may be retained by law)
│   │   ├── Step 2 Shred subject DEK(s) (KMS key material deletion per policy; record time)
│   │   ├── Step 3 Delete/anonymize plaintext in every inventory location (§10.4) for that subject
│   │   ├── Step 4 Invalidate caches; reindex search docs for subject
│   │   ├── Step 5 Notify processors/third parties holding subject data
│   │   └── Step 6 Evidence record: what, where, when, by whom; residual copies with expiry dates (backups)
│   │
│   ├── 10.3 E-TENANT procedure
│   │   ├── Step 1 Suspend writes; drain in-flight processing; final export if contractually required
│   │   ├── Step 2 Mark tenant OFFBOARDING → consumers skip tenant events (record counts)
│   │   ├── Step 3 Delete plaintext in all inventory locations (§10.4)
│   │   ├── Step 4 Delete dedicated topics, groups, positions, indexes, schemas references (keep shared schemas)
│   │   ├── Step 5 Shred tenant KEK (all wrapped DEKs become useless) after pending-deletion window
│   │   ├── Step 6 Registry: OFFBOARDED (id tombstoned forever, MT-12)
│   │   └── Step 7 Evidence record; backup expiry schedule
│   │
│   ├── 10.4 Plaintext inventory (must be maintained per service; MT-10)
│   │   ├── Projections (PostgreSQL), search indexes, caches
│   │   ├── Quarantine/held stores (diagnostics may contain decrypted values)
│   │   ├── Consumer outbox rows and downstream services' derived stores
│   │   ├── Analytics warehouse, data lake, ML features
│   │   ├── Logs, traces, error trackers
│   │   ├── Emails/SMS/notification provider logs (third parties)
│   │   ├── Backups and snapshots of all of the above
│   │   └── Replay shadow stores and forensic targets
│   │
│   ├── F10.1 "Deleting the key = erasure" — plaintext copies remain — COMPLIANCE [C]
│   ├── F10.2 Subject request handled with tenant key — wrong scope (all or nothing) [C]
│   ├── F10.3 Identifiers in keys/headers survive — COMPLIANCE [C]
│   │   └── Handling: MT-09; if legacy data has them, topic-level rewrite or retention expiry is the only cure
│   ├── F10.4 KMS pending-deletion window ignored in SLA — "instant" claims false [C]
│   │   └── Handling: SLA states effective time; schedule-deletion + immediate disable (decrypt denied) at once
│   ├── F10.5 Replay after erasure resurrects data — COMPLIANCE [C]
│   │   └── Handling: ERASED marker propagation; erasure registry check in replay (replay tree RP-11)
│   └── F10.6 Erasure job partially fails — some locations still hold data — COMPLIANCE [C]
│       └── Handling: per-location status with retries; incomplete erasures escalate before SLA
│
├── 11. RETENTION & LEGAL HOLD PER TENANT
│   ├── 11.1 Topic retention is per topic → pooled topics use the platform retention class
│   ├── 11.2 Tenants with longer retention needs → archive with per-tenant retention (archive is tenant-partitioned)
│   ├── 11.3 Tenants with shorter retention needs → M-DEDICATED topics or rely on crypto-shredding by period DEKs
│   ├── 11.4 Legal hold → blocks erasure for scoped data; recorded with scope and expiry
│   └── F11.1 Conflicting retention in pooled topic (one tenant needs 7 years, another 30 days) — COMPLIANCE [C]
│       └── Handling: retention via archive + period DEKs, not via shared topic retention
│
├── 12. TENANT LIFECYCLE
│   ├── 12.1 Onboarding
│   │   ├── Registry entry (tier, region, retention class, quotas)
│   │   ├── Keys (KEK) created in tenant's region
│   │   ├── Placement provisioned (dedicated topics/ACLs/consumer config via IaC)
│   │   ├── Registry replicated to all consumers before ACTIVE (avoids F6.2)
│   │   └── Canary event per tenant validates end-to-end isolation (§15)
│   │
│   ├── 12.2 Suspension (non-payment, abuse, security)
│   │   ├── Policy per flow: block new commands; continue processing in-flight events (consistency)
│   │   └── Never drop events for suspended tenants (they may be reactivated)
│   │
│   ├── 12.3 Tier / region migration (MT-13)
│   │   ├── Dual-publish from outbox to old and new placement (same tx)
│   │   ├── New consumers start in shadow; version-based per-aggregate cutover
│   │   ├── Residency migrations: data copied only to allowed destination; old region erased after cutover
│   │   └── Reconciliation before decommissioning old placement
│   │
│   ├── 12.4 Merge / split (acquisitions, reorganizations)
│   │   ├── New tenant ids; aggregates re-created with lineage (MT-12, F3.4)
│   │   └── Keys: data re-encrypted only via new events (history stays under old keys)
│   │
│   └── 12.5 Offboarding → §10.3
│
├── 13. INTERACTIONS WITH OTHER TREES
│   ├── Outbox: tenant stamping + consistency in the business tx; invalid → tx fails
│   ├── Idempotency: dedupe keys include handler + event_id (tenant implicit via event); version guard per tenant aggregate
│   ├── DLQ: per-tenant budgets; INTEGRITY-TENANT class; masked triage views per tenant
│   ├── Retry/backpressure: tenant-fair lanes; tenant halt = key holds, not partition pause
│   ├── Ordering: key includes tenant; tier moves = stream migrations
│   ├── Versioning: PII tags drive encryption; encrypted fields in schema evolution
│   ├── Replay: tenant-scoped replays via archive (tenant-partitioned) or dedicated topics; erasure honored
│   └── Offsets/consumer groups: dedicated tenants may have dedicated groups; positions deleted at offboarding
│
├── 14. OBSERVABILITY
│   ├── Per-tenant (top-N + VIP): throughput, time lag, held keys, quarantine, retry, quota throttling
│   ├── Isolation signals: tenant-consistency failures (must be 0), RLS denials, cache wrapper rejections,
│   │   search filter enforcement hits
│   ├── Crypto: decrypt failures by tenant (outside erasure = page), KMS latency/throttling, DEK cache hit rate
│   ├── Residency: placement/replication violations (must be 0)
│   ├── Erasure: requests by state, SLA, per-location completion
│   ├── Lifecycle: tenants by state; onboarding/offboarding durations
│   └── Absence alerts on all of the above
│
├── 15. ISOLATION TESTING & VERIFICATION
│   ├── Leak canaries: synthetic tenants with unique marker data; continuous probes from other tenants'
│   │   contexts through every read path (API, search, cache, exports) → any hit = SECURITY incident
│   ├── Interleaving tests: lanes processing alternating tenants (context bleed, F3.2)
│   ├── RLS tests executed as application role (F5.2)
│   ├── Fuzzing tenant fields at producer (spoofing, mismatches) → tx fails / quarantine
│   ├── Erasure verification: post-erasure scans across inventory for subject markers
│   ├── Residency audits: replication topology vs registry, monthly
│   └── Access reviews: principals with pooled-topic access reviewed quarterly
│
├── 16. RUNBOOKS
│   ├── RB-01 Tenant mismatch detected → quarantine + SIEM → producer investigation → fix → disposition
│   ├── RB-02 Leak canary hit → SECURITY incident → isolate read path → scope impact → notify per policy
│   ├── RB-03 Noisy tenant → source throttle → tenant key holds if needed → consider M-DEDICATED
│   ├── RB-04 Subject erasure → §10.2 → evidence
│   ├── RB-05 Tenant offboarding → §10.3 → evidence
│   ├── RB-06 Tenant tier/region migration → §12.3 → reconciliation → decommission
│   ├── RB-07 KMS outage → pause decrypting consumers → monitor retention headroom → resume
│   └── RB-08 Unexpected decrypt failures → freeze key deletions → key inventory audit → restore if accidental (within window)
│
└── 17. GO-LIVE TEST MATRIX
    ├── Client sends forged tenant header → server stamps true tenant; mismatch → tx fails
    ├── Producer bug emits key/header/payload mismatch → consumer quarantines + SIEM alert; no drop
    ├── Alternating-tenant records on one lane → zero cross-tenant writes
    ├── RLS with pooled connections → no context leakage across transactions
    ├── Search aggregation from tenant A context → zero tenant B data
    ├── Cache key collision attempt → wrapper rejects
    ├── Bulk import by one tenant → others' time lag within SLO
    ├── Tenant halt → only that tenant's keys held; others flow
    ├── Subject erasure → DEK shredded, all inventory locations cleaned, replay shows ERASED
    ├── Tenant offboarding → topics/positions/indexes removed, KEK scheduled, events skipped thereafter
    ├── Accidental key deletion attempt outside workflow → blocked; decrypt canary alerts
    ├── Residency tenant with DR mirroring → placement controller refuses
    ├── KMS outage → consumers pause, no quarantines, resume cleanly
    ├── Tier migration pooled → dedicated → zero reorder, zero loss, reconciliation clean
    └── Metrics pipeline down → absence alerts fire
```

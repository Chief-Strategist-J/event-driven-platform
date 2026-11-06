# Audit Trail — Business Logic Decision Tree

```
AUDIT TRAIL — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (verify per regulation/vendor/version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-SDK   Audit SDK + framework middleware — mandatory capture points (commands, admin APIs, auth, data access)
│   ├── C-AOB   Audit outbox — rows written in the business transaction (Class A/C with state change)
│   ├── C-SPL   Durable local spool — for events without a business tx (auth, reads) when sink unavailable
│   ├── C-ING   Audit ingestion — dedupe, canonicalize, assign shard + sequence, write to ledger
│   ├── C-LED   Audit ledger store (PostgreSQL, append-only, time-partitioned, per-shard sequences)
│   ├── C-INT   Integrity service — per-shard hash chains / Merkle trees, signed checkpoints, verification
│   ├── C-ANC   Anchor store — checkpoints in WORM storage + optional external timestamping
│   ├── C-ARC   Compliance archive — object storage with Object Lock; encrypted; partition exports + proofs
│   ├── C-BUS   Audit stream (Kafka) — feeds SIEM/analytics; NOT the system of record
│   ├── C-SIEM  SIEM / log analytics — detection and correlation; lossy by design
│   ├── C-PSE   Pseudonymization & key service — subject tokens, per-subject DEKs, erasure execution
│   ├── C-QRY   Query & forensics service — role-gated, masked, query-audited; archive retrieval
│   ├── C-EXP   Export service — signed bundles with inclusion/consistency proofs, chain of custody
│   ├── C-COV   Coverage verifier — business mutations ↔ audit records reconciliation
│   └── C-MON   Monitoring — capture rate, spool depth, verification status, anchor freshness
│
├── 1. INVARIANTS  (with enforcement)
│   ├── AU-01 Every auditable action produces exactly one logical audit record [C]
│   │   └── Enforced by: C-SDK middleware; C-AOB in business tx; idempotent ingestion by audit_id; C-COV reconciliation
│   ├── AU-02 Audit and business state commit atomically where a transaction exists [C]
│   │   └── Enforced by: audit outbox in same tx; no post-commit "fire and forget" audit for Class A
│   ├── AU-03 Denied, failed, and successful attempts are all recorded [C]
│   │   └── Enforced by: middleware emits on every outcome; outcome field mandatory
│   ├── AU-04 Integrity is externally anchored [C]
│   │   └── Enforced by: signed checkpoints published to WORM anchor store on schedule; signing key in HSM/KMS
│   ├── AU-05 Hashes are computed over canonical bytes that are stored verbatim [C]
│   │   └── Enforced by: canonical serialization; canonical bytes stored as bytes (not re-normalized types)
│   ├── AU-06 No global serialization point [C]
│   │   └── Enforced by: sharded chains (per shard sequence), Merkle aggregation per checkpoint
│   ├── AU-07 Truncation and rollback are detectable [C]
│   │   └── Enforced by: verification against latest anchored checkpoint (count + root + last sequence per shard)
│   ├── AU-08 Retention deletes only what is archived and verified [C]
│   │   └── Enforced by: partition drop gated on archive object present, checksum verified, checkpoint covering it
│   ├── AU-09 Personal data is pseudonymized or per-subject encrypted before reaching immutable tiers [C]
│   │   └── Enforced by: C-PSE in ingestion; lint on audit schemas (PII tags); secret scanners
│   ├── AU-10 Secrets never enter audit records [C]
│   │   └── Enforced by: deny-list fields, value scanners at ingestion; reject + alert
│   ├── AU-11 Access to audit data is itself audited and separately authorized [C]
│   │   └── Enforced by: C-QRY only path; query events written to a separate audit shard
│   ├── AU-12 Changes to the audit system are audited outside it [C]
│   │   └── Enforced by: config/role/retention/object-lock changes recorded in independent account + alerts
│   ├── AU-13 Security events are never dropped; volume is handled by aggregation, not sampling [C]
│   │   └── Enforced by: aggregation records with counts and first/last seen; raw retained in spool limits
│   └── AU-14 Readability for the full retention period [C]
│       └── Enforced by: schema versioning, archived schemas, canonical format stable for retention horizon
│
├── 2. CAPTURE — WHAT HAPPENED AND HOW IS IT RECORDED?
│   └── Trigger: Auditable occurrence
│       │
│       ├── Check 2.1: Occurrence type
│       │   ├── T-MUTATION  Business state change (ledger transfer, refund, order cancel, fee adjustment)
│       │   │   └── Capture: C-AOB row in the SAME transaction as the mutation (AU-02)
│       │   │       ├── Tx commits → audit row commits → relay to C-ING
│       │   │       └── Tx rolls back → no audit row → (if attempt matters) T-DENIED path records the attempt
│       │   ├── T-ADMIN     Administrative/infra action (role grant, topic change, DLQ purge, offset reset, config change)
│       │   │   └── Capture: C-OPS/control-plane tooling writes audit before executing (intent) + after (outcome)
│       │   ├── T-AUTH      Authentication/session (login success/failure, MFA, token issue/revoke, key creation)
│       │   │   └── Capture: identity provider hooks → C-SDK → C-ING (no business tx) → C-SPL if sink down
│       │   ├── T-ACCESS    Read access to sensitive data (PII, cardholder data, exports, audit queries themselves)
│       │   │   └── Capture: query-layer middleware records subject, scope, purpose, row/field counts (not values)
│       │   ├── T-DENIED    Authorization failure / validation rejection of privileged action
│       │   │   └── Capture: middleware on every deny decision (AU-03)
│       │   └── T-SYSTEM    Automated jobs acting on business data (batch corrections, retention purges, reconciliations)
│       │       └── Capture: job identity + triggering ticket/approval + scope
│       │
│       ├── Check 2.2: Failure policy if audit cannot be recorded (by class)
│       │   ├── Class A (financial) → FAIL CLOSED: action does not complete without audit (outbox makes this automatic)
│       │   ├── Class B (security/auth)
│       │   │   ├── Login success/failure → durable spool (C-SPL); never block authentication on audit sink
│       │   │   └── Privilege escalation, key creation → FAIL CLOSED
│       │   ├── Class C (admin/infra) → FAIL CLOSED (control plane refuses action if audit write fails)
│       │   └── Class D (data access) → spool; if spool full → FAIL CLOSED for bulk exports, spool-pressure alert otherwise
│       │
│       └── Edge cases
│           ├── E2.1 Audit emitted after commit (post-commit hook) — crash between commit and emit — missing audit — LOSS [C]
│           │   └── Handling: AU-02 outbox; C-COV detects
│           ├── E2.2 Audit emitted before commit, tx later rolls back — phantom audit of action that never happened — CORRUPT [C]
│           │   └── Handling: intent records labelled INTENT; outcome record required; unmatched intents flagged
│           ├── E2.3 Bulk operation (1M rows corrected by a job) — one audit per row floods pipeline vs one record hides detail
│           │   └── Handling: one parent audit record (scope, query, approval) + per-row change manifest stored as
│           │                 archived artifact referenced by hash
│           ├── E2.4 Direct SQL by engineer (break-glass) — no middleware — invisible — SECURITY [C]
│           │   └── Handling: database-level statement auditing for human roles shipped off-host; break-glass sessions recorded
│           ├── E2.5 Action via internal service-to-service call bypassing gateway — no capture point — LOSS [C]
│           │   └── Handling: capture in the owning service's command handler, not only at the edge
│           ├── E2.6 Spool disk fills during long sink outage — events lost or service blocked [C]
│           │   └── Handling: spool sized for max outage [G]; spool-pressure alerts; fail-closed thresholds per class
│           └── E2.7 Retried command (idempotent replay) emits a second audit record — duplicate audit — CORRUPT [C]
│               └── Handling: audit_id derived from idempotency key + action; ingestion dedupes by audit_id
│
├── 3. RECORD CONTENT
│   │
│   ├── 3.1 Actor resolution
│   │   ├── Check: who is acting?
│   │   │   ├── Human user directly → actor = user principal + session id + auth method (incl. MFA state)
│   │   │   ├── Human impersonating (support "act as") → actor = support agent; on_behalf_of = user;
│   │   │   │                                             impersonation session id + reason + approval
│   │   │   ├── Service acting for a user (delegation chain) → actor = service; on_behalf_of chain (user → svc A → svc B)
│   │   │   ├── Service acting autonomously (scheduled job) → actor = job identity + run id + triggering config/ticket
│   │   │   └── API key / client credentials → actor = key id (never key value) + owning principal
│   │   └── Edge cases
│   │       ├── E3.1a Impersonation recorded as the end user — support actions attributed to customers — COMPLIANCE [C]
│   │       ├── E3.1b Delegation chain truncated to last hop — true initiator lost — forensics impossible [C]
│   │       └── E3.1c Shared service account used by humans — non-attributable actions — SECURITY [C]
│   │           └── Handling: forbid; break-glass identities are personal and time-bound
│   │
│   ├── 3.2 Source & context
│   │   ├── Client IP from trusted proxy chain only (forwarded headers from untrusted hops ignored) [C]
│   │   ├── Request id, correlation id, trace id, tenant id, region, service version
│   │   └── E3.2a Forwarded-for header taken at face value — attacker spoofs source IP in audit — SECURITY [C]
│   │
│   ├── 3.3 Action & outcome
│   │   ├── Action taxonomy: "<domain>.<resource>.<verb-past>" from a registry (no free text)
│   │   ├── Outcome: SUCCEEDED | DENIED | FAILED | INTENT (with reason codes)
│   │   └── E3.3a Free-text action names drift across services — queries miss events — LOSS for forensics [C]
│   │
│   ├── 3.4 Target
│   │   ├── type + id (+ tenant); multiple targets allowed for multi-entity actions
│   │   └── E3.4a Target id is personal data (email as id) — immutable PII in archive — COMPLIANCE [C]
│   │       └── Handling: opaque ids only (partitioning tree PT-08)
│   │
│   ├── 3.5 Change detail (diff policy by class)
│   │   ├── Class A: before/after of monetary fields, account ids, amounts in minor units — exact, not scrubbed
│   │   ├── Class B: role/permission before/after; key metadata (never secret material)
│   │   ├── Class C: config before/after (secrets redacted to fingerprints)
│   │   ├── Class D: what was accessed (fields, counts, purpose), never the values read
│   │   ├── Personal fields in diffs → pseudonymized/encrypted per subject (AU-09)
│   │   ├── Large diffs → stored as archived artifact referenced by hash
│   │   └── Edge cases
│   │       ├── E3.5a Scrubbing removes the values auditors need (amounts) — evidence useless — COMPLIANCE [C]
│   │       ├── E3.5b Secret (password, token, card number) captured in diff of a config/user object — permanent in WORM — SECURITY [C]
│   │       │   └── Handling: AU-10 scanners reject; fields typed as secret never serialized; incident if found
│   │       └── E3.5c Nested object diffs computed differently per service — inconsistent evidence [C]
│   │           └── Handling: shared diff library; canonical paths (schema evolution tree §1)
│   │
│   └── 3.6 Time
│       ├── occurred_at (source clock) + ingested_at (ingestion clock) + shard sequence (ordering)
│       ├── Clock offset monitoring on sources; skew beyond bound flagged on record
│       └── E3.6a UUIDv7 or timestamps treated as total order across hosts — misleading forensic timelines — CORRUPT [C]
│           └── Handling: per-shard sequence for order; timestamps for time; both shown in forensics
│
├── 4. ORDERING, IDENTITY & SHARDING
│   ├── audit_id: deterministic from (source, idempotency key/action instance) where possible; else UUID at source
│   ├── Shard selection: by tenant or by source service (stable), not by random
│   ├── Per-shard monotonic sequence assigned at ingestion (single writer per shard)
│   └── Edge cases
│       ├── E4.1 Two ingestion instances write same shard — duplicate sequences / fork — CORRUPT [C]
│       │   └── Handling: shard ownership with epoch fencing (consumer groups tree CG-01)
│       └── E4.2 Out-of-order arrival (spooled events delivered hours late) — sequence ≠ occurred order [C]
│           └── Handling: accepted by design; forensics sorts by occurred_at with sequence tiebreak and shows late flag
│
├── 5. INTEGRITY
│   ├── 5.1 Canonicalization (AU-05)
│   │   ├── Deterministic serialization (field order, number formats, no insignificant whitespace, UTF-8 normalized)
│   │   ├── Canonical bytes stored verbatim (bytea) alongside queryable columns
│   │   └── E5.1a Hashing JSON then storing in jsonb — key order/whitespace normalized — every record fails verification [C]
│   │
│   ├── 5.2 Per-shard structure
│   │   ├── Option A: hash chain per shard — record_hash = H(prev_hash_in_shard || canonical_bytes) (keyed: HMAC)
│   │   ├── Option B: Merkle tree per shard per interval — supports inclusion proofs for exports
│   │   └── E5.2a Unkeyed "salt" stored next to data — no protection against recomputation [C]
│   │       └── Handling: keyed hashing or signatures with key outside the database
│   │
│   ├── 5.3 Checkpoints & anchoring (AU-04, AU-07)
│   │   ├── Every interval [L: 5–15 min] per shard: (shard, last sequence, record count, chain head / Merkle root)
│   │   ├── Signed with HSM/KMS key; written to C-ANC (WORM) and optionally external timestamping service
│   │   ├── Checkpoint of checkpoints (global root across shards) for whole-system attestations
│   │   └── Edge cases
│   │       ├── E5.3a Anchor publication fails silently — unanchored window grows — truncation undetectable [C]
│   │       │   └── Detection: anchor freshness alert per shard
│   │       └── E5.3b Signing key compromised or rotated without record — verification ambiguity [C]
│   │           └── Handling: key rotation recorded in anchor store; old public keys retained for retention period
│   │
│   ├── 5.4 Verification types
│   │   ├── Consistency: recompute chain/roots for shard ranges; compare with anchored checkpoints
│   │   ├── Truncation/rollback: latest anchored (last sequence, count) vs current store
│   │   ├── Archive parity: archived partitions' roots vs anchored roots
│   │   ├── Inclusion: specific record proof for exports
│   │   └── Schedule: continuous incremental (recent), daily full for hot tier, monthly sampled archive restores
│   │
│   ├── 5.5 Results
│   │   ├── INTACT → certified window
│   │   ├── MISMATCH → SECURITY incident; freeze writes to affected shard? (no — keep writing; preserve evidence)
│   │   └── UNKNOWN (store unavailable, archive restore pending) → never reported as INTACT
│   │
│   └── Edge cases
│       ├── E5.5a Hot-tier retention pruned the chain start — "verify from offset 0" impossible — false failures or skipped checks [C]
│       │   └── Handling: verification starts from the oldest retained anchored checkpoint
│       ├── E5.5b Backup restore of hot store — earlier state, valid chain — rollback undetected [C]
│       │   └── Handling: post-restore verification vs anchors mandatory; gap re-ingested from archive/spool/bus
│       └── E5.5c Legit schema migration rewrites rows — hashes no longer match — false incident or verification disabled [C]
│           └── Handling: never rewrite audit rows; new columns derived elsewhere; canonical bytes immutable
│
├── 6. STORAGE TIERS & LIFECYCLE
│   ├── 6.1 Hot ledger (PostgreSQL)
│   │   ├── Table owner = non-login role; ingestion role INSERT-only; no UPDATE/DELETE/TRUNCATE grants
│   │   ├── Defense in depth: triggers rejecting UPDATE/DELETE; DDL event auditing; superuser use alerted
│   │   ├── Time partitions; detach/drop executed only by lifecycle job role after AU-08 checks
│   │   ├── Replicated (sync for Class A shards); backups encrypted; restores audited
│   │   └── Edge cases
│   │       ├── E6.1a Partition dropped before archive object confirmed — LOSS of evidence [C]
│   │       ├── E6.1b Superuser/owner session modifies rows — triggers bypassable by owner — SECURITY [C]
│   │       │   └── Detection: anchored verification (§5.4) + DB-level statement audit shipped off-host
│   │       └── E6.1c Hot store contains PII beyond its own retention needs — COMPLIANCE [C]
│   │
│   ├── 6.2 Audit stream (C-BUS)
│   │   ├── Durability config as T0 topics; keyed by shard; at-least-once → SIEM dedupes by audit_id
│   │   ├── Not system of record (SIEM retention, sampling, vendor outages)
│   │   └── E6.2a SIEM ingestion fails silently for days — detection blind — SECURITY [C]
│   │       └── Detection: SIEM-side canary events with freshness alerts
│   │
│   ├── 6.3 Compliance archive (C-ARC)
│   │   ├── Partition exports: canonical bytes + per-partition Merkle root + manifest + checkpoint references
│   │   ├── Object Lock
│   │   │   ├── COMPLIANCE mode: nobody can delete or shorten retention, including root [C]
│   │   │   ├── GOVERNANCE mode: privileged principals can override [C]
│   │   │   └── Decision: validate layout, encryption, retention values in GOVERNANCE first; switch to COMPLIANCE
│   │   │       only after verification (mistakes become permanent)
│   │   ├── Retention per class and jurisdiction [G: legal input; 7 years is not universal]
│   │   ├── Legal hold supported per object for litigation
│   │   ├── Encryption: KMS keys with retention ≥ object retention; per-subject keys for personal fields (AU-09)
│   │   ├── Cold storage retrieval takes hours [C] → forensic SLA accounts for it; recent years in faster tier
│   │   └── Edge cases
│   │       ├── E6.3a Wrong retention value in COMPLIANCE mode (e.g., 70 years) — irreversible cost/legal problem [C]
│   │       ├── E6.3b Secret or unlawful PII written to COMPLIANCE-locked object — undeletable — SECURITY/COMPLIANCE [C]
│   │       │   └── Handling: AU-09/AU-10 before archive; if it happens: rotate secret, crypto-shred if field-encrypted, legal review
│   │       ├── E6.3c Archive KMS key scheduled for deletion — entire archive unreadable — LOSS of evidence [C]
│   │       │   └── Handling: key deletion protection; alerts on key state changes (AU-12)
│   │       └── E6.3d Archive written by at-least-once job — duplicate or missing partitions [C]
│   │           └── Handling: deterministic object names per partition; manifest reconciliation with ledger
│   │
│   └── 6.4 Retention conflicts
│       ├── Legal minimum (keep) vs privacy maximum (delete) → resolved per field via pseudonymization/encryption
│       └── E6.4a Single retention for all audit classes — over-retention of personal data or under-retention of financial evidence [C]
│
├── 7. PRIVACY VS IMMUTABILITY
│   ├── 7.1 At ingestion
│   │   ├── Subject identifiers → stable pseudonymous tokens (mapping in deletable C-PSE store)
│   │   ├── Personal fields (IP, user agent, names in diffs) → encrypted with per-subject DEK
│   │   └── Non-personal evidence (amounts, account ids, actions) → clear
│   ├── 7.2 Erasure request
│   │   ├── Check: legal hold or regulatory retention obligation on this subject's audit data?
│   │   │   ├── YES → retain; record denial/deferral reason; restrict access
│   │   │   └── NO → destroy subject DEK + token mapping → archived ciphertext becomes unreadable; integrity unaffected
│   │   │                                                    (hashes cover ciphertext)
│   │   └── E7.2a Personal data hashed in clear (not encrypted) — "pseudonymized" but re-identifiable by brute force — COMPLIANCE [C]
│   ├── 7.3 Residency: audit data stored and processed in tenant's region (multi-tenant tree §8); SIEM vendor region checked
│   └── 7.4 E7.4 Audit shipped to third-party SIEM with personal data in clear — residency/processor violation — COMPLIANCE [C]
│
├── 8. COMPLETENESS & COVERAGE  (AU-01)
│   ├── 8.1 Framework enforcement: every command handler, admin endpoint, auth flow, sensitive query path has capture
│   │   └── Static checks: handlers without audit annotation fail CI (allow-list for non-auditable)
│   ├── 8.2 Coverage tests: integration tests assert audit record per auditable endpoint
│   ├── 8.3 Reconciliation (C-COV)
│   │   ├── Class A: every ledger posting / refund / adjustment ↔ audit record by (target, version)
│   │   ├── Class C: control-plane change logs ↔ audit records
│   │   └── Class B: identity provider logs ↔ audit records (counts per window)
│   └── Edge cases
│       ├── E8.1 New endpoint shipped without capture — silent gap — LOSS [C]
│       ├── E8.2 Async worker mutates state outside handlers (consumer projections, sagas) — no audit [C]
│       │   └── Handling: classify which projections are auditable (financial); capture in their transactions
│       └── E8.3 Reconciliation itself sampled — rare gaps unseen — LOSS [C]
│           └── Handling: Class A reconciliation is full, not sampled
│
├── 9. ACCESS, QUERY & FORENSICS
│   ├── 9.1 Roles
│   │   ├── Auditor (read masked), Investigator (unmask per case, approved), Export (four-eyes), Admin (no data access)
│   │   └── Tenant-scoped audit views for customers (their own tenant only)
│   ├── 9.2 Query path
│   │   ├── Only via C-QRY (no direct DB access for humans)
│   │   ├── Every query recorded as T-ACCESS in a separate shard (AU-11)
│   │   ├── Rate limits and result-size caps; bulk requires export workflow
│   │   └── Archive queries: restore requests tracked; results expire
│   ├── 9.3 Exports
│   │   ├── Bundle: records (canonical bytes), inclusion proofs, checkpoint signatures, public keys, manifest
│   │   ├── Signed by export key (HSM); chain of custody recorded (requester, approver, recipient, purpose)
│   │   └── Verifiable offline by third parties
│   └── Edge cases
│       ├── E9.1 Investigator queries own actions or colleagues' without case — SECURITY [C]
│       │   └── Handling: case-bound access; conflict-of-interest rules; alerts on self-queries
│       ├── E9.2 Export without proofs — evidence challengeable — COMPLIANCE [C]
│       └── E9.3 Forensic timeline built from occurred_at only — misleading under clock skew [C]
│
├── 10. WATCHING THE WATCHERS  (AU-12)
│   ├── Changes to: audit pipeline config, retention, object lock settings, KMS keys, roles on audit stores,
│   │   SIEM forwarding rules, capture allow-lists → recorded in independent account/system + paged
│   ├── Separation of duties: audit system operators ≠ business system admins
│   ├── Deployment of audit components requires security approval
│   └── E10.1 Audit capture disabled by a config flag during an incident and never re-enabled — LOSS [C]
│       └── Handling: capture cannot be disabled for Class A/C; for others, disable expires automatically + pages
│
├── 11. AVAILABILITY, VOLUME & ABUSE
│   ├── 11.1 Backpressure: ingestion slow → outbox/spool absorbs; Class A unaffected (outbox in business DB)
│   ├── 11.2 Flood handling (credential stuffing, scanners)
│   │   ├── Aggregate repetitive identical events per (actor/source, action, window) with counts + first/last seen
│   │   ├── Keep first N raw events per window [L]
│   │   └── Never drop security events entirely (AU-13)
│   ├── 11.3 Capacity: records/s per class, bytes per record, retention per tier → storage and verification cost [G]
│   └── E11.1 Attacker floods audit to exhaust spool and hide real actions — SECURITY [C]
│       └── Handling: per-source quotas with aggregation; priority lanes for Class A/C
│
├── 12. LONG-TERM READABILITY  (AU-14)
│   ├── Audit record schema versioned; schemas archived with partitions
│   ├── Canonical encoding stable for the retention period (changes create new version, old remain verifiable)
│   ├── Verification tooling kept runnable for archived formats (container images archived)
│   └── E12.1 Verification tool deprecated — 5-year-old partitions can't be verified — COMPLIANCE [C]
│
├── 13. DR & MULTI-REGION
│   ├── Shards owned per region; anchors replicated to independent region/account
│   ├── Failover: shard ownership moves with epoch bump; checkpoint continuity verified
│   └── E13.1 Region failover with async replication — last minutes of audit lost — LOSS [C]
│       └── Handling: Class A shards synchronous; spool/outbox re-delivery after failover; post-failover verification
│
├── 14. OBSERVABILITY
│   ├── Capture: records/s by class and outcome; denied-action rate; T-ACCESS volume
│   ├── Pipeline: outbox depth, spool depth/pressure, ingestion lag, dedupe rate
│   ├── Integrity: last verified window per shard, anchor freshness, MISMATCH (must be 0), UNKNOWN windows
│   ├── Coverage: reconciliation gaps (must be 0 for Class A)
│   ├── Archive: partitions pending export, archive parity results, object lock config drift
│   ├── Security: secret-scanner rejections, superuser sessions on audit DB, audit-system config changes
│   └── Absence alerts on all of the above (external dead-man's switch — monitoring tree EC-15)
│
├── 15. RUNBOOKS
│   ├── RB-01 Integrity MISMATCH → security incident → preserve evidence → identify range → compare with archive/anchors
│   ├── RB-02 Anchor publication failing → restore publication → close unanchored window with catch-up checkpoint
│   ├── RB-03 Coverage gap (Class A) → identify code path → hotfix capture → backfill audit from business data
│   │         (marked BACKFILLED with evidence) → incident review
│   ├── RB-04 Secret found in audit → rotate secret → crypto-shred if encrypted → legal/security review → scanner fix
│   ├── RB-05 Erasure request → §7.2 → evidence record
│   ├── RB-06 Spool pressure → restore sink → drain at controlled rate → verify no drops
│   ├── RB-07 Hot store restored from backup → verify against anchors → re-ingest gap → certify
│   └── RB-08 Forensic export request → case approval → export with proofs → chain-of-custody record
│
└── 16. TEST MATRIX
    ├── Refund committed → exactly one audit record; refund rolled back → none (or INTENT + FAILED pair)
    ├── Denied privileged action → audit record with DENIED outcome
    ├── Concurrent ingestion on many shards at peak → no chain forks; throughput meets target
    ├── Stored bytes re-hash → match (no jsonb normalization)
    ├── Delete newest 100 rows as superuser (staging) → truncation detected vs anchor
    ├── Restore hot store from yesterday's backup → rollback detected
    ├── Partition drop attempted before archive verification → blocked
    ├── Secret in config diff → rejected at ingestion; alert
    ├── IP and user agent → encrypted per subject; erasure → unreadable, integrity verification still passes
    ├── Object Lock retention typo in GOVERNANCE test → caught before COMPLIANCE enablement
    ├── New endpoint without audit annotation → CI fails
    ├── Support impersonation → actor = agent, on_behalf_of = customer
    ├── Spoofed forwarded IP header → ignored; trusted proxy IP recorded
    ├── Credential-stuffing flood → aggregated, Class A latency unaffected
    ├── SIEM outage → canary freshness alert; ledger unaffected
    ├── Export bundle → verified offline with published public keys
    ├── 5-year-old archived partition → restored and verified with archived tooling
    └── Audit pipeline config change → recorded externally and paged
```

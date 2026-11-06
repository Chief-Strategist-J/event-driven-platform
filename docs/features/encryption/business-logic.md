# Encryption — Business Logic Decision Tree

```
ENCRYPTION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (verify per provider/version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-KMS   KMS/HSM — root and key-encryption keys (KEKs); never exports key material
│   ├── C-KEYS  Key service + key table — subject/tenant/field-class DEKs stored WRAPPED; key references; states
│   ├── C-CRY   Crypto library (one per language, same behavior) — AEAD, AAD construction, envelope format, caching
│   ├── C-DEC   Decryption policy layer — entitlements per consumer × field class; audit of decrypt decisions
│   ├── C-ERA   Erasure registry — authoritative "subject erased at time T" (multi-tenant tree §10)
│   ├── C-BIX   Blind index service — keyed hashes for equality search on encrypted fields
│   ├── C-PKI   Certificate infrastructure — CA, SPIFFE/SPIRE, cert-manager, rotation, expiry monitoring
│   ├── C-SEC   Secret store — TLS keys, client credentials, HMAC keys (never in config files/images)
│   └── C-MON   Monitoring — cert expiry, KMS usage/throttling, decrypt outcomes, plaintext leak scanners
│
├── 1. INVARIANTS  (with enforcement)
│   ├── EN-01 Erasure granularity is decided before data is written [C]
│   │   └── Enforced by: PII fields encrypted under per-subject (or per-subject-per-period) DEKs in C-KEYS; never per-event
│   ├── EN-02 Ciphertext is bound to its context [C]
│   │   └── Enforced by: AAD = tenant + subject + aggregate + field path + schema version + key ref; library-constructed
│   ├── EN-03 Encryption metadata is part of the typed payload, not headers [C]
│   │   └── Enforced by: schema type "EncryptedValue" for every PII-tagged field; lint
│   ├── EN-04 Consumers fail closed on unexpected plaintext [C]
│   │   └── Enforced by: PII-tagged field arriving as plaintext → quarantine + security alert
│   ├── EN-05 Decrypt outcomes are distinguished; only ERASED maps to the erased marker [C]
│   │   └── Enforced by: library returns DECRYPTED | ERASED | UNAUTHORIZED | UNAVAILABLE | TAMPERED | MALFORMED
│   ├── EN-06 Nonces never repeat under a key [C]
│   │   └── Enforced by: random 96-bit nonces + per-key usage limits → DEK rotation per period; no shared counters
│   ├── EN-07 KMS calls are bounded [C]
│   │   └── Enforced by: DEK caching with TTL; wrapped DEKs fetched once per subject/period; quota budget per service
│   ├── EN-08 No plaintext PII outside entitled boundaries [C]
│   │   └── Enforced by: encryption before outbox insert; log/trace redaction; dump policies; leak scanners
│   ├── EN-09 Every network path is encrypted and authenticated [C]
│   │   └── Enforced by: connection inventory with TLS/mTLS status; plaintext listeners disabled; scans
│   ├── EN-10 Certificate expiry is never an outage [C]
│   │   └── Enforced by: automated rotation, overlap windows, expiry alerts at multiple horizons, renewal canaries
│   ├── EN-11 Key deletion is governed [C]
│   │   └── Enforced by: deletion only via erasure/offboarding workflows; KMS deletion protections; alerts on key state
│   └── EN-12 Algorithms and key schemes are versioned [C]
│       └── Enforced by: version field in EncryptedValue; readers support all versions within readability horizon
│
├── 2. THREAT MODEL — WHAT EACH LAYER ACTUALLY PROTECTS
│   ├── Check 2.1: Which threat?
│   │   ├── Stolen disk / decommissioned hardware → at-rest storage encryption ✓
│   │   ├── Network eavesdropping / MITM → TLS + certificate verification ✓
│   │   ├── Service impersonation → mTLS workload identity ✓
│   │   ├── Compromised DB credentials / backup access / snapshot sharing → at-rest disk encryption ✗ → field-level ✓
│   │   ├── Broad Kafka read access (any consumer reads PII) → field-level + entitlements ✓
│   │   ├── Erasure obligations on immutable logs/archives → per-subject crypto-shredding ✓
│   │   └── Compromised service with decrypt rights → limited by entitlements, caching TTL, anomaly detection (partial)
│   └── E2.1 "Encrypted at rest" presented as protection against insiders/DB users — false assurance — SECURITY [C]
│
├── 3. ENCRYPTION IN TRANSIT
│   ├── 3.1 Connection inventory (every path, EN-09)
│   │   ├── Client → gateway; gateway → services; service ↔ service
│   │   ├── Producers/consumers/relays → Kafka brokers; broker ↔ broker; controller quorum traffic
│   │   ├── Kafka Connect workers ↔ brokers; Connect REST; schema registry clients ↔ registry
│   │   ├── Services → PostgreSQL; PostgreSQL replication (physical and logical/CDC)
│   │   ├── Services → OpenSearch (HTTP) and node ↔ node transport; Redis client and replication
│   │   ├── Services → KMS / Vault / object storage; observability agents → backends
│   │   └── Admin tools, bastions, CI runners → all of the above
│   │
│   ├── 3.2 Configuration rules
│   │   ├── TLS 1.2 minimum (1.3 preferred) with modern suites; hostname/SAN verification ON everywhere
│   │   ├── No plaintext listeners on production networks (check ports serving plain HTTP alongside TLS)
│   │   ├── mTLS identities (SPIFFE IDs) mapped to authorization policies, not just "valid cert"
│   │   └── Kafka: TLS/SASL listeners only; inter-broker on TLS
│   │
│   ├── 3.3 Certificate lifecycle (EN-10)
│   │   ├── Short-lived workload certs (SPIRE) — rotation is continuous; agent/server health critical
│   │   ├── Long-lived certs (ingress, partner mTLS) — expiry alerts at 30/14/7/1 days
│   │   ├── CA rotation with overlap (old + new CA trusted during transition)
│   │   └── Edge cases
│   │       ├── E3.3a Ingress certificate expires — total external outage — OUTAGE [C]
│   │       ├── E3.3b SPIRE server/agent down longer than SVID TTL — every mTLS call fails — mesh OUTAGE [C]
│   │       │   └── Handling: TTL ≥ several × worst-case recovery time; SPIRE HA; alert on SVID age
│   │       ├── E3.3c CA rotated without overlap — half the fleet distrusts the other — partial OUTAGE [C]
│   │       ├── E3.3d Java/Go trust stores not reloaded on rotation — long-running processes fail after rotation [C]
│   │       │   └── Handling: hot reload support verified per runtime; rolling restart fallback
│   │       └── E3.3e Partner mTLS cert renewed but partner not updated — partner integration OUTAGE [C]
│   │
│   └── Edge cases
│       ├── E3.1 Hostname verification disabled "temporarily" in a client — MITM possible — SECURITY [C]
│       ├── E3.2 Replication/CDC connections left unencrypted (internal network assumption) — PII on the wire — SECURITY [C]
│       └── E3.3 TLS terminates at load balancer, plaintext to backend across zones — SECURITY [C]
│
├── 4. ENCRYPTION AT REST
│   ├── 4.1 Store-by-store (storage encryption with customer-managed keys where required)
│   │   ├── PostgreSQL: data files, WAL, temp files, replicas, backups, snapshots, logical dumps, PITR archives
│   │   ├── Kafka: broker volumes; tiered storage objects; no native per-record encryption [C]
│   │   ├── OpenSearch: data volumes; snapshot repositories (separate object storage encryption) [L]
│   │   ├── Redis: persistence files (RDB/AOF), replicas
│   │   ├── Object storage: SSE with KMS keys; bucket policies deny unencrypted puts
│   │   └── Observability: log/trace/metrics backends (often hold the most PII leaks)
│   │
│   ├── 4.2 Key ownership
│   │   ├── Provider-managed vs customer-managed keys per store per compliance class
│   │   └── Mixed cloud providers → one key management model documented per provider [G: your topology]
│   │
│   └── Edge cases
│       ├── E4.1 Backups/snapshots copied to another account/region with weaker keys — SECURITY [C]
│       ├── E4.2 Snapshot restore into lower environment — production PII in dev — COMPLIANCE [C]
│       ├── E4.3 Core dumps / heap dumps written to disk or uploaded to crash tooling — plaintext keys/PII — SECURITY [C]
│       │   └── Handling: disable core dumps in production; heap dumps only to encrypted, access-controlled, auto-expiring storage
│       └── E4.4 Customer-managed key disabled accidentally — store unreadable — OUTAGE [C]
│           └── Handling: key state alerts; deletion protection (EN-11)
│
├── 5. FIELD-LEVEL ENCRYPTION — DESIGN DECISIONS
│   ├── 5.1 Which fields? (schema PII tags drive everything; schema evolution tree §9)
│   │   ├── Direct identifiers (name, email, phone, national id, address) → encrypt
│   │   ├── Payment data (card numbers) → tokenize via payment provider; never store PAN in events
│   │   ├── Quasi-identifiers (birth date, precise location, IP) → encrypt
│   │   ├── Business data needed for routing/ordering (ids, versions, tenant, event type) → clear (opaque ids only)
│   │   └── Free-text fields (notes, descriptions) → encrypt (PII hides in free text)
│   │
│   ├── 5.2 Key granularity (EN-01)
│   │   ├── Check: what must be erasable independently?
│   │   │   ├── Individual data subjects → per-subject DEK (+ per-period for long-lived subjects)
│   │   │   ├── Whole tenant → tenant KEK wrapping its subjects' DEKs
│   │   │   └── Field classes with different entitlements → separate DEKs per (subject, field class)
│   │   ├── Key table row: key ref, subject, tenant, field class, period, wrapped DEK, KEK id, state, created/destroyed time
│   │   └── E5.2a Per-event DEK wrapped by shared key — subject erasure impossible — COMPLIANCE [C]
│   │
│   ├── 5.3 EncryptedValue structure (EN-03, EN-12) — typed in schema
│   │   ├── scheme version, key ref (opaque), nonce, ciphertext+tag
│   │   ├── Raw bytes in binary formats (no Base64 inflation in Avro/Protobuf)
│   │   └── Present for every PII-tagged field at every nesting level (schema evolution tree §1 paths)
│   │
│   ├── 5.4 AAD (EN-02)
│   │   ├── Includes: tenant, subject, aggregate id, field path (canonical), schema version, key ref
│   │   ├── Constructed only by library from typed context (no handwritten AAD)
│   │   └── Edge cases
│   │       ├── E5.4a No AAD — ciphertext moved between subjects/fields decrypts successfully — CORRUPT/SECURITY [C]
│   │       ├── E5.4b AAD includes event_id — field can't be copied legitimately into views/derived events
│   │       │         → re-encryption needed downstream; decide AAD scope deliberately [C]
│   │       └── E5.4c Field path changes in schema evolution (rename/move) — AAD mismatch on old data — decrypt failures [C]
│   │           └── Handling: AAD uses stable field identity (tag/number), versioned mapping for moves
│   │
│   ├── 5.5 Algorithm & nonce rules (EN-06)
│   │   ├── AES-256-GCM with random 96-bit nonces; NIST guidance limits random-nonce usage per key (~2^32 messages) [C]
│   │   ├── Long-lived per-subject DEKs at high volume → rotate per period well below limit
│   │   ├── Nonce reuse under GCM leaks plaintext XOR and enables forgery [C] → never counters shared across instances
│   │   └── Consider misuse-resistant schemes for very high-volume keys [L]
│   │
│   ├── 5.6 Size & compression
│   │   ├── Overhead per field (nonce + tag + key ref); message size budgets updated
│   │   └── Encrypted fields don't compress; Kafka batch compression ratio drops → capacity planning [C]
│   │
│   └── 5.7 Deterministic encryption / searchable needs → §8 (blind indexes) — never deterministic GCM
│
├── 6. PRODUCER PATH
│   ├── 6.1 Steps
│   │   ├── Resolve subject + field class → key ref (create DEK if first use; wrapped DEK stored in C-KEYS)
│   │   ├── Obtain plaintext DEK (cache hit, or unwrap via KMS once per TTL)
│   │   ├── Encrypt each PII field with AAD (library) BEFORE the event is built for the outbox
│   │   ├── Outbox row contains only ciphertext for PII fields (EN-08)
│   │   └── Erasure check: subject erased? → refuse to create new data under destroyed key; business decision
│   │
│   ├── 6.2 Caching (EN-07)
│   │   ├── Plaintext DEK cache TTL [L: 5–15 min]; bounded size; per-process memory only
│   │   ├── Erasure broadcast invalidates cache (best effort; TTL is the guarantee)
│   │   └── Key material held in byte arrays, not immutable strings; zeroing is best-effort in managed runtimes [C]
│   │
│   ├── 6.3 KMS / key service unavailable
│   │   ├── Cache hit → continue
│   │   ├── Cache miss → ENVIRONMENTAL failure → command fails (fail closed) or queued (business policy)
│   │   └── Never fall back to plaintext
│   │
│   └── Edge cases
│       ├── E6.1 Encryption at relay/CDC instead of application — outbox table and WAL hold plaintext — SECURITY [C]
│       ├── E6.2 Logging the event or request before encryption — plaintext PII in log pipeline — COMPLIANCE [C]
│       │   └── Handling: structured logging with PII-tag-aware redaction; leak scanners on log streams
│       ├── E6.3 Two instances create DEK for same new subject concurrently — two keys, split data — erasure misses one [C]
│       │   └── Handling: unique (subject, field class, period) constraint; loser reloads winner's key
│       ├── E6.4 KMS throttling during bulk import — commands fail at scale — OUTAGE [C]
│       │   └── Handling: quota-aware batching; pre-warm keys; dedicated import path with rate limits
│       └── E6.5 Producer encrypts with wrong subject (bug) — data erasable only under wrong subject — COMPLIANCE [C]
│           └── Handling: subject derived from aggregate ownership, not request input; tests
│
├── 7. CONSUMER DECRYPTION — OUTCOME TREE  (EN-05)
│   └── For each EncryptedValue
│       ├── Check 7.1: Is the field PII-tagged in the schema?
│       │   ├── YES and value is EncryptedValue → continue
│       │   ├── YES and value is plaintext → PLAINTEXT-UNEXPECTED → quarantine + security alert (EN-04)
│       │   └── NO and value is EncryptedValue → MALFORMED (schema/contract defect) → quarantine
│       │
│       ├── Check 7.2: Is this consumer entitled to this field class? (C-DEC)
│       │   ├── NO → UNAUTHORIZED → consumer schema must not require the field; field left encrypted/absent
│       │   │        (never NULL-overwrite stored values; never treat as erased)
│       │   └── YES → continue
│       │
│       ├── Check 7.3: Is the subject/key erased? (C-ERA / key state DESTROYED)
│       │   ├── YES → ERASED → write erased marker (distinct from null/unknown); remove plaintext copies downstream
│       │   └── NO → continue
│       │
│       ├── Check 7.4: Obtain DEK (cache or unwrap)
│       │   ├── Success → continue
│       │   ├── Throttled / timeout / network / KMS unavailable → UNAVAILABLE → ENVIRONMENTAL pause (retry tree §6)
│       │   ├── Access denied unexpectedly (policy drift) → UNAVAILABLE + security alert (not ERASED)
│       │   └── Key not found but NOT in erasure registry → INTEGRITY/SECURITY incident (possible accidental deletion)
│       │
│       ├── Check 7.5: AEAD decrypt with reconstructed AAD
│       │   ├── Tag valid → DECRYPTED
│       │   └── Tag invalid → TAMPERED (or AAD mismatch from schema change) → quarantine + security alert
│       │
│       └── Edge cases
│           ├── E7.1 Any decrypt error mapped to NULL — KMS outage wipes PII in views — CORRUPT/LOSS [C]
│           ├── E7.2 Header-stripped event treated as plaintext — ciphertext stored as customer data — CORRUPT [C]
│           ├── E7.3 Per-record KMS decrypt at replay speed — quota exhaustion — replay OUTAGE + live impact [C]
│           │   └── Handling: per-subject DEK caching; replay rate limits (replay tree §9.4)
│           ├── E7.4 Erasure registry lag behind key destruction — key-not-found classified as incident falsely [C]
│           │   └── Handling: key destruction and registry entry written in the same workflow step order (registry first)
│           └── E7.5 Erased marker written into delta views (sums/counts) — arithmetic errors — CORRUPT [C]
│               └── Handling: PII never used in arithmetic; erased marker only on PII fields
│
├── 8. DERIVED DATA & SEARCHABILITY
│   ├── 8.1 Plaintext proliferation
│   │   ├── Every decrypting consumer that stores plaintext becomes part of the erasure inventory (multi-tenant tree §10.4)
│   │   └── Prefer storing EncryptedValue in views and decrypting at read time for low-volume PII fields
│   ├── 8.2 Equality search on encrypted fields (find customer by email)
│   │   ├── Blind index: keyed hash (HMAC) of normalized value, separate key per field class/tenant (C-BIX)
│   │   ├── Leakage: equality and frequency are revealed — acceptable per field decision [C]
│   │   ├── Normalization rules versioned (case, whitespace, phone formats)
│   │   └── Erasure: blind index entries deleted with subject; HMAC key rotation requires re-indexing
│   ├── 8.3 Range/full-text search on PII → generally not possible on ciphertext → design alternative (tokenized subsets) [C]
│   └── Edge cases
│       ├── E8.1 Unkeyed hash used as "index" for emails/phones — dictionary attack re-identifies — SECURITY [C]
│       ├── E8.2 Search index stores decrypted PII — erasure misses it — COMPLIANCE [C]
│       └── E8.3 Normalization change without re-index — lookups miss existing records — CORRUPT [C]
│
├── 9. KEY LIFECYCLE
│   ├── 9.1 Creation: DEK generated by KMS or library CSPRNG; stored wrapped; KEK in KMS/HSM
│   ├── 9.2 Rotation
│   │   ├── KEK rotation: re-wrap DEKs (cheap); provider automatic rotation keeps old versions for decrypt [L]
│   │   ├── DEK rotation: new period keys for new data (EN-06); old DEKs kept until data expires or erased
│   │   └── Immutable stores (Kafka, archives) are never re-encrypted in place
│   ├── 9.3 Compromise response
│   │   ├── DEK compromise → that subject/period's ciphertext exposed; cannot re-encrypt history → retention/rewrite decision
│   │   ├── KEK compromise → re-wrap all DEKs under new KEK; revoke old; investigate unwrap logs
│   │   └── Library/nonce bug → identify affected keys/time range; rotate; assess exposure
│   ├── 9.4 Destruction (erasure)
│   │   ├── Order: erasure registry entry → key state DESTROYED (wrapped DEK deleted) → cache invalidation broadcast
│   │   ├── Key table backups: retention aligned with erasure SLA; restored backups re-apply erasure log (multi-tenant F9.6)
│   │   ├── Provider KMS key deletion has pending windows (e.g., 7–30 days on AWS) [C] → disable immediately + schedule
│   │   └── Evidence: what was destroyed, when, by which workflow
│   ├── 9.5 Residency & DR
│   │   ├── Keys in the tenant's region; DR region needs key access within residency rules
│   │   └── Multi-region keys only where residency permits
│   └── Edge cases
│       ├── E9.1 Restore of key table from backup resurrects destroyed DEKs — erasure undone — COMPLIANCE [C]
│       ├── E9.2 DR region lacks KMS access — failover succeeds but nothing decrypts — OUTAGE [C]
│       ├── E9.3 Provider key deleted (not subject DEK) — all data under it lost — LOSS [C]
│       └── E9.4 Old KEK version disabled after rotation — historical wrapped DEKs can't be unwrapped — LOSS [C]
│
├── 10. ACCESS CONTROL & DETECTION
│   ├── 10.1 Entitlements per (service, field class) enforced by C-DEC / KMS key policies
│   ├── 10.2 Key usage logged (provider audit + library decision logs) → audit trail tree (T-ACCESS class)
│   ├── 10.3 Anomaly detection: decrypt rate per service vs baseline; unusual subjects; off-hours bulk decrypts
│   └── Edge cases
│       ├── E10.1 One shared KMS key for all services — any compromised service decrypts everything — SECURITY [C]
│       ├── E10.2 Decrypt audit logs too voluminous → disabled — exfiltration undetectable — SECURITY [C]
│       │   └── Handling: aggregate per service/subject/window; full logs for bulk patterns
│       └── E10.3 Developer access to production key service for debugging — SECURITY [C]
│           └── Handling: break-glass only, time-bound, audited
│
├── 11. INTERACTIONS WITH OTHER TREES
│   ├── Outbox/CDC: ciphertext only in outbox rows and raw CDC topics carrying PII columns
│   ├── DLQ: quarantine stores ciphertext; diagnostics never include decrypted values
│   ├── Replay: decryption caching + erasure honored (replay tree RP-11)
│   ├── Schema evolution: EncryptedValue type, AAD stable field identity, PII tags at nested paths
│   ├── Audit trail: decrypt/access events; per-subject encryption for audit personal fields
│   ├── Multi-tenant: tenant KEKs, subject DEKs, residency
│   └── Monitoring: ciphertext compared as bytes or via hashes; never decrypt for consistency checks
│
├── 12. ALGORITHM AGILITY  (EN-12)
│   ├── Scheme version in every EncryptedValue
│   ├── Readers support all versions within readability horizon; writers use current
│   ├── Migration of scheme = new writes only; old data readable until expired/erased
│   └── E12.1 Library upgrade changes AAD construction or encoding silently — old data fails to decrypt — OUTAGE [C]
│       └── Handling: cross-version golden ciphertexts in CI for every supported scheme
│
├── 13. OBSERVABILITY
│   ├── Certificates: days to expiry per cert; SVID age; rotation failures
│   ├── TLS: plaintext connection detection; handshake failures by peer
│   ├── KMS: requests/s per service, throttling, latency, errors by type, quota headroom
│   ├── Key service: DEK cache hit rate, keys created/destroyed, state changes (alerts on unexpected destroy/disable)
│   ├── Decrypt outcomes: DECRYPTED/ERASED/UNAUTHORIZED/UNAVAILABLE/TAMPERED/MALFORMED/PLAINTEXT-UNEXPECTED
│   │   (TAMPERED and PLAINTEXT-UNEXPECTED must be 0 → page)
│   ├── Leak scanners: PII patterns in logs, traces, DLQ diagnostics, dumps
│   └── Absence alerts on all of the above
│
├── 14. RUNBOOKS
│   ├── RB-01 Certificate near expiry → renew → verify trust store reload → canary connections
│   ├── RB-02 SPIRE/PKI degradation → restore servers → extend SVID TTL if needed → monitor mesh errors
│   ├── RB-03 KMS throttling → raise quotas / increase cache TTL within policy → pause bulk paths → resume
│   ├── RB-04 TAMPERED spike → security incident → isolate producer/path → compare with source
│   ├── RB-05 Plaintext PII detected in logs/topics → stop source → purge where possible → incident → scanner rule
│   ├── RB-06 Subject erasure → registry → destroy DEKs → invalidate caches → purge derived plaintext → evidence
│   ├── RB-07 Key compromise → rotate/re-wrap → scope exposure → notify per policy
│   └── RB-08 DR failover → verify KMS/key service access in DR → decrypt canary → resume
│
└── 15. TEST MATRIX
    ├── Erase one subject → only that subject's fields unreadable; others intact; integrity checks pass
    ├── KMS throttled for 10 min → consumers pause; zero NULL overwrites; resume cleanly
    ├── KMS access policy removed for a consumer → UNAVAILABLE + alert, not ERASED
    ├── Ciphertext copied from subject A into subject B's event → TAMPERED (AAD) → quarantined
    ├── Encryption header stripped / plaintext in PII field → PLAINTEXT-UNEXPECTED → quarantined
    ├── Replay of 50M events → KMS calls bounded by subjects × periods, within quota
    ├── Concurrent first writes for new subject → single DEK
    ├── Field renamed in schema → AAD by stable field identity → old data decrypts
    ├── Key table restored from backup → erasure log re-applied → destroyed keys stay destroyed
    ├── Ingress cert expiry simulation → alerts at all horizons; renewal automation succeeds
    ├── SPIRE outage for 2× SVID TTL → behavior matches design (degraded but bounded)
    ├── Core dump attempt in production → disabled
    ├── Log line containing an email → redacted; scanner alert if missed
    ├── Blind index lookup by normalized email → found; erasure removes index entry
    ├── DR failover → decrypt canary passes in DR region
    └── Cross-language golden ciphertexts (Go, Kotlin) → identical decrypt results for all scheme versions
```

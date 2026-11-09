# ADR: Cryptographically Verified, Tamper-Evident Sharded Audit Trail

## Status
Accepted

## Context
Enterprise distributed systems require immutable, non-repudiable audit logging across financial transactions, authorization checks, administrative actions, and data access (AU-01 to AU-14). Previous architectures often rely on standard `jsonb` log tables or downstream SIEM event streams (e.g. Kafka topics) as the source of truth. However:
1. SIEM streams are lossy by design and do not provide deterministic atomicity with local database state mutations (AU-02).
2. Storing canonical payloads in PostgreSQL `jsonb` columns strips insignificant whitespace and alters key ordering. When re-computing SHA-256 or HMAC signatures, 100% of cryptographic hashes fail verification (Failure E5.1a).
3. Monolithic single-chain ledgers create a global write bottleneck and lock contention across tenants.
4. Database superusers or malicious actors could mutate or truncate historical audit rows without detection if integrity is not anchored externally into WORM storage (AU-04, AU-07).

## Decision
1. **Verbatim Canonical Bytes (`bytea`) Storage (AU-05)**:
   - Payloads are normalized through deterministic JSON serialization (lexicographical key sorting, strict numeric formats, UTF-8 normalization) and stored verbatim in PostgreSQL as raw `BYTEA` (`canonical_payload`).
   - Hash calculations are strictly computed against verbatim stored bytes.

2. **Per-Shard Monotonic Hash Chains (AU-06)**:
   - Partition audit streams by shard (`shard_id`, e.g., tenant or system domain).
   - Each shard maintains an independent, strictly monotonic sequence number and cryptographic hash chain:
     $$\text{record\_hash}_k = \text{HMAC-SHA256}(\text{record\_hash}_{k-1} \,\|\, \text{canonical\_payload}_k)$$
   - Eliminates cross-shard lock contention and enables high-throughput concurrent ingestion.

3. **Signed Checkpoints & External Anchoring (AU-04, AU-07)**:
   - Background sealers periodically checkpoint each shard interval: recording `start_sequence`, `end_sequence`, `record_count`, and `checkpoint_root_hash`.
   - Checkpoints are cryptographically signed using KMS/HSM keys and archived to immutable WORM anchor stores.
   - Truncation, deletion, and rollback attacks are immediately detectable by comparing the current head against anchored checkpoints.

4. **Database-Level Immutability & Audit Triggers (AU-02, AU-11)**:
   - PostgreSQL triggers reject any `UPDATE` or `DELETE` statement executed against `audit_trail_ledger`.
   - Forensics and query accesses into the audit trail are logged into a separate `audit_trail_access_log` table to prevent covert audit browsing.

5. **Pseudonymization and Secret Scrubbing (AU-09, AU-10)**:
   - Input payloads are scanned for forbidden secret patterns (tokens, passwords, private keys). Offending payloads are rejected at ingress.
   - PII fields are mapped to deterministic subject tokens before immutable storage, permitting crypto-shredding on GDPR erasure without breaking cryptographic hash chains.

## Consequences
- **Positive**: Cryptographic non-repudiation, tamper detection, compliance with SOC2/PCI-DSS/GDPR, independent sharded throughput, zero JSON re-serialization verification bugs.
- **Negative**: Increased storage requirements for verbatim raw bytes, strict ingest-time secret linting, operational overhead of periodic checkpoint signing.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Conventional application logs (stdout, text files, Elastic/Splunk) are volatile, non-atomic with database writes, and trivially editable by database administrators or compromised root accounts. Without a cryptographically verifiable, append-only ledger, an enterprise cannot legally prove non-repudiation during a financial audit or forensic investigation.

### 2. Internal Algorithmic Mechanics & State Invariants

Each shard maintains an independent, strictly monotonic sequence number ($S_k = S_{k-1} + 1$). The engine normalizes incoming payloads into deterministic, byte-exact JSON and stores them as raw BYTEA. An HMAC-SHA256 digest is computed across the previous digest, sequence number, and canonical bytes: record_hash_k = HMAC(record_hash_{k-1} || sequence_k || canonical_payload_k). Database triggers reject UPDATE and DELETE, while background sealers periodically sign checkpoint roots using KMS/HSM and archive them to external WORM storage.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: The Epistemology of Cryptographic Hash Chaining
* **Design Philosophy & Epistemology**:
  Human institutions operate on trust; computational infrastructure must operate on mathematical proof. Application logs are mutable strings written to volatile disks, making them legally and forensically useless under adversarial conditions. The philosophy is Absolute Historical Lineage: every recorded action must contain the irreversible mathematical fingerprint of the entire preceding timeline. You cannot alter a single comma of the past without visibly shattering all subsequent reality.
* **Threat Vector Defeated**: Unauthorized ledger alteration, historical timestamp forgery, silent row deletion, and record tampering by rogue database administrators, compromised credentials, or ransomware.
* **Storage & State Invariant**:
```text
record_hash[k] = HMAC_SHA256(shard_key, record_hash[k-1] || sequence[k] || timestamp[k] || canonical_payload[k])
where sequence[k] == sequence[k-1] + 1 and timestamp[k] >= timestamp[k-1]
```
* **Implementation Mechanism**:
  Ingestion acquires a row-level lock on the shard head table (`SELECT last_sequence, last_hash FROM audit_trail_shards WHERE shard_id = $1 FOR UPDATE`). The service computes the HMAC binding previous hash, sequence increment, and payload bytes, appends the row into `audit_trail_ledger`, and advances the shard head within the exact same database transaction.
* **Non-Negotiable Trade-Off**: Shard serialization bottleneck; ingestion throughput per shard is bounded by the round-trip latency of lock acquisition and HMAC calculation. High-throughput multi-tenant systems must horizontally partition across explicit shard IDs.

---

#### Principle 2: Canonical Byte Immutability (BYTEA vs Serialized JSONB)
* **Design Philosophy & Epistemology**:
  Software serializers are non-deterministic interpreters, not objective record-keepers. Storing audit payloads in parsed formats like PostgreSQL `jsonb` or allowing language-specific JSON formatters to normalize keys and whitespace silently corrupts signatures over time. The philosophy is Verbatim Byte Supremacy: truth exists only at the raw binary layer. Hashes must be computed over verbatim ingress bytes, not interpreted object models.
* **Threat Vector Defeated**: Serializer-induced signature divergence. PostgreSQL's native `jsonb` type strips insignificant whitespace, reorders object keys lexicographically, and normalizes numeric representations (e.g., `1.00` becomes `1`), causing 100% false-positive cryptographic verification failures.
* **Storage & State Invariant**:
```text
Hash(IngressBytes) == Hash(StoredBytes) == Hash(AuditedBytes)
```
* **Implementation Mechanism**:
  Payloads are canonicalized at the ingress gateway according to RFC 8785 (JSON Canonicalization Scheme - JCS) and persisted verbatim in PostgreSQL as raw `BYTEA`. Verification engines stream the stored binary bytes directly from disk into cryptographic hashers with zero intermediate JSON unmarshaling.
* **Non-Negotiable Trade-Off**: Higher disk footprint than compressed JSONB, and querying deeply nested audit fields requires explicit generated columns or separate projection tables.

---

#### Principle 3: Kernel-Enforced Database Mutation Prohibition
* **Design Philosophy & Epistemology**:
  API-layer permission checks are fragile illusions easily bypassed by direct database connections, ORM bugs, migration scripts, or SQL injection attacks. The philosophy is Defense at the Lowest Storage Layer: immutability must be enforced by the database storage engine itself. Once written, a record is physically incapable of undergoing UPDATE, DELETE, or TRUNCATE operations.
* **Threat Vector Defeated**: Accidental or malicious UPDATE or DELETE operations executed by application bugs, rogue ORM batch migrations, or compromised database credentials.
* **Storage & State Invariant**:
```text
For all t1 < t2: LedgerState(t1) is a strict subset of LedgerState(t2) (Monotonic Growth Only)
```
* **Implementation Mechanism**:
  PostgreSQL BEFORE UPDATE OR DELETE triggers reject mutations with SQLSTATE 23505 exceptions. Dedicated application database roles explicitly have UPDATE, DELETE, and TRUNCATE privileges revoked (`REVOKE ALL ON audit_trail_ledger FROM app_user; GRANT SELECT, INSERT`). Human errors are addressed strictly via compensating ledger transactions.
* **Non-Negotiable Trade-Off**: Operational permanence. Mistakes committed to the audit ledger cannot be erased or corrected in-place; compensating audit transactions consume permanent storage.

---

#### Principle 4: External Non-Repudiation Anchoring (Hardware KMS + WORM)
* **Design Philosophy & Epistemology**:
  A ledger whose keys and data live inside the same security boundary is vulnerable to full infrastructure compromise. If an attacker gains root access to the database host, they can recalculate the hash chain from scratch. The philosophy is External Multi-Party Witnessing: periodically summarize historical reality into a Merkle root and anchor it into an immutable, third-party cryptographic vault outside the database's trust zone.
* **Threat Vector Defeated**: Total infrastructure destruction, database truncation, or rogue administrator database restoration where an attacker restores a 7-day-old database snapshot to wipe out audit evidence.
* **Storage & State Invariant**:
```text
MerkleRoot = CalculateMerkleRoot(RecordHashes[start...end])
Signature = KMS.Sign(HSM_PrivateKey, MerkleRoot || start_seq || end_seq || timestamp)
```
* **Implementation Mechanism**:
  A background sealer service builds a binary Merkle tree over unsealed ledger records (e.g., every 10,000 rows), signs the root using an AWS KMS / Cloud HSM asymmetric private key inaccessible to DBAs, and exports the signed manifest to AWS S3 Object Lock storage in Compliance Mode (WORM - Write Once, Read Many).
* **Non-Negotiable Trade-Off**: Cloud KMS API latency and operational complexity of asynchronous reconciliation between relational tables and object storage.

---

#### Principle 5: PII Tokenization & Cryptographic Shredding
* **Design Philosophy & Epistemology**:
  Modern systems face an existential legal conflict: financial regulations (SOX, PCI-DSS) demand that audit records remain immutable for 7 years, while privacy regulations (GDPR Article 17) demand the 'Right to Erasure'. The philosophy is Cryptographic Forgetfulness: store identity keys separately from domain action ledgers. When identity must be forgotten, destroy the key rather than mutating the ledger.
* **Threat Vector Defeated**: Regulatory non-compliance penalties resulting from the inability to delete personal data within an immutable audit trail without invalidating the cryptographic hash chain.
* **Storage & State Invariant**:
```text
Payload = Encrypt(PII, SubjectKey) || AnonymousDomainData
Erasure == Destroy(SubjectKey) ==> Probability(Decrypt(PII)) == 0
```
* **Implementation Mechanism**:
  Ingress scanners replace personal identifiers with deterministic surrogate tokens (UUIDv7). Identity mappings are stored in an isolated key-management store encrypted with per-subject Data Encryption Keys (DEKs). Upon GDPR erasure, deleting the subject's DEK renders the personal data unrecoverable while leaving the audit ledger's hash chain 100% mathematically intact.
* **Non-Negotiable Trade-Off**: Cryptographic key management complexity and added CPU overhead during audit payload tokenization and retrieval.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is an append-only database table without cryptographic chaining legally and architecturally insufficient for compliance?**
   * *Answer*: An append-only table without cryptographic chaining allows any privileged user or compromised DB connection to modify rows or delete entries without leaving a trace. Cryptographic chaining mathematically links every row to its predecessor, making any modification immediately detectable across all subsequent records.
2. **What happens to hash chain verification when a shard experiences high-concurrency ingestion across multiple instances?**
   * *Answer*: High-concurrency ingestion without sharding creates database lock contention on the shard head. The architecture resolves this by horizontally partitioning audit streams into independent shard IDs, each maintaining its own monotonic sequence and hash chain.
3. **How does cryptographic shredding satisfy GDPR Article 17 without breaking Merkle tree anchors in WORM storage?**
   * *Answer*: The audit ledger and external WORM manifests store ciphertext and tokenized surrogates. When the subject's encryption key is destroyed from the HSM, the ciphertext becomes irreversibly unrecoverable entropy. The hash of the ciphertext remains unchanged, keeping the Merkle chain perfectly valid while personal data is permanently erased.

---
## Authoritative References & Production Citations

* **[RFC 3161: Internet X.509 Public Key Infrastructure Time-Stamp Protocol (TSP)](https://www.rfc-editor.org/rfc/rfc3161)**
  - *Architectural Rationale & Implementation Grounding*: Establishes authoritative standards for cryptographically proving data existed at a given timestamp without subsequent alteration.
* **[NIST Special Publication 800-92: Guide to Computer Security Log Management](https://csrc.nist.gov/publications/detail/sp/800-92/final)**
  - *Architectural Rationale & Implementation Grounding*: Federal standard detailing tamper-evident logging architecture, integrity verification models, and long-term secure archival.
* **[Bela Gipp et al.: Decentralized Trusted Timestamping using Merkle Trees](https://www.gipp.com/wp-content/papercite-cache/pdf/gipp2015a.pdf)**
  - *Architectural Rationale & Implementation Grounding*: Seminal research paper on verifiable integrity trees, tamper-evident data structures, and continuous checkpoint anchoring.
* **[PCI-DSS v4.0 Requirement 10: Log and Monitor All Access to System Components and Cardholder Data](https://www.pcisecuritystandards.org/)**
  - *Architectural Rationale & Implementation Grounding*: Mandates non-repudiation, time synchronization, and tamper-resistance for all financial audit trails.

## Amendment 2026-11-07: Database error mapping and typed handler errors

### Problem observed
Audit ingest, checkpoint, integrity-verification and query failures returned 400 with the raw error text, which for Postgres failures included `pq:` driver messages and constraint names.

### Root cause
- RC2: `audit-trail.postgres.repository.adapter.go` returned raw driver errors at nine call sites.
- RC1: every handler error was reported as 400 `VALIDATION_FAILED` with `err.Error()`, whatever the cause.

### Decision / fix
- All nine adapter error returns now go through `database.MapDatabaseError`, which yields typed `ApplicationError`s (class 23 becomes 409 with the generic message "request conflicts with existing data or violates a data integrity rule", without the constraint name).
- New `AuditTrailHandler.writeServiceError` uses `errors.As` so those typed statuses reach the client; untyped errors remain 400 with the error text.
- The shared mapper fallback in `src/infra/database/error_mapper.go` no longer includes the constraint name.

### API-visible behavior change
Integrity violations on audit routes change from 400 (raw text) to 409 (generic message). Other typed database errors surface with their mapped status. Not breaking for valid requests.

### Verification
Mapping sites verified by code inspection and the shared mapper change re-tested live (generic class-23 message). No audit-trail-specific unit or HTTP test was added.

### Remaining gaps
- Unverified: a previously claimed "list without tenant_id returns 400" does not hold for `GET /api/v1/audits/query` in the current code; the handler defaults a missing `tenant_id` to `default` and requires `shard_id` instead.
- Untyped (non-database) errors on audit routes still map to 400 with raw text.


## Amendment 2026-11-07 (second pass): tenant scope required on query

`GET /audits/query` requires `tenant_id` (400) in addition to `shard_id`; the silent `default` tenant fallback was removed. This closes the gap recorded in the first amendment.

Verified live on the compose-deployed image (`chiefj/event-driven-platform:v2`) and by unit tests; see ADR-0002 "Closed in a second pass".

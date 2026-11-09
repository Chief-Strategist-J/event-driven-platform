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

### 3. Core Architectural Principles

* **Cryptographic Hash Chaining & Merkle Linkage**: Every audit entry is cryptographically linked to its predecessor. Mutating, deleting, or reordering any row invalidates all subsequent hashes across the shard.
* **Canonical Byte Immutability**: Hashes are computed strictly over verbatim stored byte arrays, eliminating JSON serializer whitespace and key-order nondeterminism that causes false-positive verification failures.
* **Database-Level Mutation Prohibition**: PostgreSQL triggers and role permissions enforce append-only semantics at the storage engine layer; corrections are strictly recorded as compensating entries.
* **External Checkpoint Anchoring**: Periodic shard hash checkpoints are signed by KMS hardware security modules and exported to immutable S3 Object Lock (WORM) storage, bounding truncation blast radiuses.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **If an attacker gains superuser access to the PostgreSQL database today, can they alter historical audit records without detection? (Our design ensures any alteration breaks the subsequent cryptographic hash chain and fails external WORM checkpoint verification).**
2. **How do we comply with GDPR 'Right to be Forgotten' when our audit ledger is cryptographically immutable? (We tokenize PII into surrogate keys; GDPR erasure shreds the encryption key for that surrogate without altering stored raw bytes or breaking the hash chain).**
3. **Why not use an external blockchain or distributed ledger? (Blockchains introduce severe write latency (seconds to minutes) and external dependency overhead; a sharded, KMS-signed local hash chain provides identical tamper-evidence at sub-millisecond local database speeds).**

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

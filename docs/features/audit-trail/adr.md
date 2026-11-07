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

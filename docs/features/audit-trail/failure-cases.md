# Audit Trail Failure Catalog

| Failure ID | Scenario | Severity | Impact | Prevention / Mitigation |
|------------|----------|----------|--------|-------------------------|
| E2.1 | Audit emitted after transaction commit via post-commit hook | LOSS | Process crashes before emitting; business mutation exists without audit record | AU-02 transactional outbox / ledger write inside same DB tx |
| E2.2 | Audit emitted before commit, transaction subsequently rolls back | CORRUPT | Phantom audit record of action that never occurred | Use INTENT outcome or write audit inside same DB tx |
| E2.3 | Bulk job mutates 1M rows in a single batch | DEGRADE | Flooding pipeline with 1M events vs losing granularity | Parent batch record with hash-referenced manifest artifact |
| E2.4 | Break-glass direct SQL executed on database by engineer | SECURITY | Untracked mutations bypassing service application layer | Database statement-level audit triggers shipped to off-host WORM |
| E2.5 | Internal microservice call bypasses API gateway | LOSS | No edge capture point for internal service action | Capture in service domain command handlers, not edge only |
| E2.6 | Local spool disk fills during prolonged audit sink outage | OUTAGE / LOSS | Spool exhausts storage; service crashes or drops audit records | Sized spool limits, backpressure alerts, fail-closed for Class A |
| E2.7 | Idempotent retried command re-emits identical audit event | CORRUPT | Duplicate audit rows for single logical mutation | Idempotent ingestion key derived from idempotency key + action (AU-01) |
| E3.1a | Impersonation ("act-as") recorded as end customer | COMPLIANCE | Support staff action erroneously attributed to customer | Explicit actor vs on_behalf_of schema fields with session id |
| E3.1b | Delegation chain truncated to last hop | FORENSIC | Original user identity lost in multi-service call chain | Full delegation chain propagated in correlation metadata |
| E3.1c | Shared service account used by human engineers | SECURITY | Non-attributable destructive mutations | Strict IAM prohibition; personal time-bound break-glass keys |
| E3.2a | Untrusted `X-Forwarded-For` header spoofed by caller | SECURITY | Attacker falsifies client IP in audit logs | Client IP parsed only from verified trusted edge reverse proxy |
| E3.3a | Uncontrolled free-text action strings drift across services | LOSS | Forensic queries miss events due to naming inconsistency | Central dot-delimited action taxonomy registry (`domain.resource.verb`) |
| E3.4a | Target ID contains unmasked email or PII | COMPLIANCE | Immutable PII stored in permanent WORM compliance tier | Opaque IDs only (PT-08); PII mapped through pseudonymization tokens |
| E3.5a | Over-aggressive redaction scrubs monetary values | COMPLIANCE | Financial auditors cannot verify exact balance movements | Class A exact amount retention in minor units (cents) |
| E3.5b | Password or API token captured in configuration diff | SECURITY | Secret leaked permanently into immutable WORM storage | Ingress secret scanners (AU-10) reject payloads with secret patterns |
| E3.5c | Inconsistent JSON serialization across polyglot services | CORRUPT | Struct diffs computed differently across languages | Verbatim canonical byte storage (AU-05) and canonical JSON serializer |
| E3.6a | Timestamps or UUIDv7 treated as total global ordering | CORRUPT | Clock skew creates interleaving and misleading forensic timeline | Per-shard monotonic sequence number for total order |
| E4.1 | Concurrent ingestion workers write to the same shard | CORRUPT | Sequence collision, split-brain, or forked hash chain | Single writer per shard with database row-level locking or fencing |
| E4.2 | Spooled events arrive hours late due to network partition | DEGRADE | Out-of-order sequence vs occurred_at timestamp | Forensics sorts by occurred_at with sequence tiebreak and late flag |
| E5.1a | Hashing JSON string then storing in PostgreSQL `jsonb` | CORRUPT | Whitespace/key-order normalization alters bytes; verification fails | Store verbatim canonical bytes in `BYTEA` column |
| E5.2a | Unkeyed hash or salt stored directly beside record data | SECURITY | Attacker with DB access recomputes hash chain undetected | HMAC with external secret key or HSM-anchored signatures |
| E5.3a | Checkpoint anchor publishing to WORM storage fails | COMPLIANCE | Unanchored window grows; silent truncation becomes possible | Anchor freshness alerts per shard; fail-closed alerting |
| E5.3b | Checkpoint signing key rotated without audit history | SECURITY | Inability to verify historical checkpoints signed by old key | Public key rotation registry with retention matching audit horizon |
| E5.5a | Retention prunes earliest rows from hot database tier | DEGRADE | Verification from sequence 0 fails | Verification starts from oldest retained anchored checkpoint |
| E5.5b | Database restored from yesterday's backup | SECURITY | Rollback attack replaces latest state with old valid chain | Verification against external WORM anchor detects sequence rollback |
| E5.5c | Schema migration alters table columns | CORRUPT | Canonical payload altered; cryptographic verification fails | `canonical_payload` bytea column remains completely untouched |
| E6.1a | Database partition dropped before archive verification | LOSS | Permanent loss of audit evidence | Partition drop gated on archive presence and checksum verification (AU-08) |
| E6.1b | Database superuser updates rows directly | SECURITY | Bypasses application logic | DB immutability triggers + off-host statement audit + anchor check |
| E6.3a | Incorrect retention period set on Object Lock | COMPLIANCE | Millions in storage costs or premature deletion of records | Staged validation in GOVERNANCE mode before COMPLIANCE lock |
| E7.2a | Personal data hashed without salt or key | COMPLIANCE | Pseudonymized identifiers subject to rainbow table attack | Per-subject DEK encryption; crypto-shredding key destruction on erasure |
| E9.1 | Investigator searches audit logs of self or peers without case | SECURITY | Unauthorized covert snooping on colleagues | Query path restricted; every query audited in `audit_trail_access_log` |
| E10.1 | Audit capture flag disabled during production incident | LOSS | Critical operational events go unrecorded | Class A/C audit capture cannot be disabled via configuration flags |

| E11.1 | Database error leaks driver text or constraint name via audit API (Amendment 2026-11-07) | SECURITY | Schema details exposed to clients; integrity violations misreported as 400 | Adapter errors pass through `MapDatabaseError`; handler `writeServiceError` uses `errors.As`; class-23 violations return 409 with a generic message. Operator: inspect server logs for the underlying error. |
| E11.2 | `GET /audits/query` without `tenant_id` | COMPLIANCE | Query silently scoped to tenant `default` | **Open**: handler defaults tenant to `default`; `shard_id` is required (400 if missing). Operator: always pass `tenant_id`. |

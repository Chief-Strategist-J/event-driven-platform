# Failure Cases & Forensic Remediation Matrix: CQRS & Token Resolution (WP-15)

This failure catalog documents all failure modes across the Command and Query responsibility segregation lifecycle, covering concurrency collisions, TOCTOU races, token forgery, cascading primary read collapses, and authorization leaks per the governing specification.

---

## 1. Failure Taxonomy Matrix

| Failure ID | Failure Mode | Severity | Invariant Violated | Detection Mechanism | Automated / Operational Remediation |
|---|---|---|---|---|---|
| **F2.1** | Side-Effecting HTTP GET Mutation | CORRUPT | CQ-06 | HTTP method router analyzer in CI | Reject side-effecting GET requests at API Gateway; map mutations strictly to POST commands with idempotency keys. |
| **F2.2** | Synchronous Audit Write on Query Path | OUTAGE | CQ-06 | Query latency breakdown probe | Decouple telemetry and access logging from query path; emit non-blocking async events to telemetry pipeline. |
| **F2.3** | Command Handler Returning Stale Read View | CORRUPT | CQ-04 | Response contract schema validator | Command handlers return minimal mutation metadata + signed consistency token; clients query read views or token resolver. |
| **F3.1** | Unkeyed Command Submission Under Retry | CORRUPT | CQ-01 | Gateway header filter (`Idempotency-Key`) | Gateway rejects mutation requests lacking `Idempotency-Key` header with HTTP `400 Bad Request`. |
| **F3.2** | Out-of-Transaction Idempotency Storage | CORRUPT | CQ-01 | Transaction boundary code scanner | Enforce persistence of aggregate mutation, outbox event, and idempotency record within a single atomic database transaction. |
| **F3.3** | Idempotency Key Reused with Mutated Payload | CORRUPT | CQ-01 | SHA-256 request payload fingerprint check | Compare incoming request fingerprint with stored hash; if hashes diverge, reject with `422 UNPROCESSABLE_ENTITY (IDEMPOTENCY_PAYLOAD_MISMATCH)`. |
| **F3.4** | TOCTOU Invariant Validation Before Lock | CORRUPT | CQ-02 | Concurrency stress integration test | Acquire row lock or optimistic version guard before reading decision state; validate business invariants under the lock. |
| **F3.5** | Lost Update on Concurrent User Edit | CORRUPT | CQ-03 | Expected version check (`expected_version == version`) | Reject mutations where version has advanced with HTTP `409 Conflict`; return current version to prompt client refresh. |
| **F3.6** | Hot Aggregate Lock Contention Storm | DEGRADE | CQ-02 | Lock wait duration metric (`pg_stat_activity`) | Implement exponential backoff with jitter for commutative operations; shard hot counters via delta aggregates. |
| **F3.7** | Premature Idempotency Record Expiry | CORRUPT | CQ-01 | Idempotency TTL scanner | Retain idempotency records for minimum 7 days to cover extended mobile offline and asynchronous retry horizons. |
| **F3.8** | Long-Running Command Premature Completion | CORRUPT | CQ-12 | Saga completion lifecycle monitor | Return HTTP `202 ACCEPTED` with saga tracking status URI; forbid returning `COMPLETED` before domain transitions finalize. |
| **F4.1** | Command Handler Reading Materialized View | CORRUPT | CQ-04 | Static analysis import gate & DB credentials | Restrict command service database credentials to write-model schemas; zero read grants on projection tables or search indices. |
| **F4.2** | TOCTOU Uniqueness Check via Query | CORRUPT | CQ-05 | Database unique constraint violation handler | Enforce cross-aggregate uniqueness exclusively via PostgreSQL `UNIQUE` constraints or dedicated reservation tables. |
| **F4.3** | External Synchronous Service Dependency Outage | OUTAGE | CQ-05 | Circuit breaker state on outbound RPC | Apply circuit breakers with bounded timeouts; queue non-critical cross-service commands or return `ACCEPTED` for asynchronous processing. |
| **F5.1** | Watermark Disagreement with Document State | CORRUPT | CQ-08 | Watermark reconciliation auditor | Store monotonic version inside the view document itself; token resolver evaluates document-embedded version. |
| **F5.2** | Watermark Advanced Past Held Event Gaps | DEGRADE | CQ-08 | Held event queue scanner | Exclude partitions with held gap quarantines from advancing view watermark; resolver checks held status for requested aggregate. |
| **F6.1** | Raw Version Header Injection / Token Forgery | SECURITY | CQ-07 | HMAC-SHA256 token verification routine | Reject raw version headers; verify server cryptographic signature on all consistency tokens; forged tokens default to `EVENTUAL`. |
| **F6.2** | Consistency Token Leak Across Principals | SECURITY | CQ-07 | Principal context claim validator | Bind signed tokens to `principal_id` and `tenant_id`; reject tokens presented by unauthorized caller contexts. |
| **F7.1** | Tight Polling During Projection Catchup | OUTAGE | CQ-09 | Query rate spike detector on read views | Enforce asynchronous event notification or sleep-bounded watermark check (single wait, 300-800ms); forbid client tight polling loops. |
| **F7.2** | Thread Exhaustion Under Query Wait Budget | OUTAGE | CQ-09 | Goroutine / Thread pool saturation monitor | Implement non-blocking channels and global waiter concurrency semaphore; shed excess waiters immediately with `PENDING`. |
| **F7.3** | Premature 404 on Just-Created Aggregate | CORRUPT | CQ-07 | Token presence check on empty query result | If query returns 0 rows but valid signed token is present, return HTTP `200` with `status: PENDING`, never HTTP `404 Not Found`. |
| **F7.4** | Shared CDN Serving Stale Content with Token | CORRUPT | CQ-07 | Cache-Control header validator | Request containing `x-consistency-token` automatically sets `Cache-Control: no-cache, private` to bypass intermediate caches. |
| **F7.5** | Unconditional Waiting Under Prolonged Lag | DEGRADE | CQ-09 | View watermark lag threshold evaluator | If current projection lag exceeds 5,000ms, skip bounded wait entirely and immediately return `PENDING` (fast-fail). |
| **F8.1** | Cascading Primary Read Outage Under Lag | OUTAGE | CQ-09 | Write model CPU and active query gauge | Strictly forbid generic read fallback to primary write model; return `PENDING` or degraded view read. |
| **F8.2** | Inconsistent Schema on Authoritative Fallback | CORRUPT | CQ-11 | Contract serialization diff tester | Authoritative-Own read API (`C-AUTHR`) uses identical serialization projection pipeline as the background projector. |
| **F8.3** | Fallback Bypassing Tenant & Resource Filters | SECURITY | CQ-10 | Query authorization interceptor | Enforce identical read-time authorization filters (`C-AUTH`) across both view queries and Authoritative-Own reads. |
| **F10.1** | Stale Permissions Cached in Materialized Views | SECURITY | CQ-10 | Permission cache invalidation subscriber | Do not project authorization grants into view rows; evaluate permissions dynamically at query execution time via `C-AUTH`. |
| **F10.3** | Token Exploited After Access Revocation | SECURITY | CQ-10 | Pre-resolution authorization gate | Query handler validates caller authorization *before* attempting consistency token verification or view retrieval. |

---

## 2. Forensic Scenarios & Execution Blueprints

### Scenario 1: Preventing Write-Model Collapse During Consumer Lag (F8.1, F7.5)
1. Kafka projection consumer experiences an uncommitted offset lag spike of 45 seconds.
2. A client submits command `UpdateProfile` $\rightarrow$ Command succeeds, returning version $v=8$ and signed token `c_tok_...`.
3. Client immediately calls `GET /api/v1/profiles/{id}` with `x-consistency-token: c_tok_...`.
4. Read view store currently reflects version $v=7$.
5. **CQRS Token Resolver Execution**:
   - Inspects current view lag ($45\text{s} > 5\text{s}$ threshold).
   - Fast-fails without waiting: returns view record $v=7$ with `consistency_status = "PENDING"`, `expected_version = 8`.
   - **Zero queries dispatched to primary PostgreSQL write model**.
   - Primary database CPU remains at normal levels ($< 15\%$), preventing platform cascade.

### Scenario 2: Concurrency TOCTOU & Idempotency Duplicate Rejection (F3.3, F3.4)
1. Client A and Client B submit concurrent commands to debit account `acc_55` with initial balance \$100.
2. Client A supplies `Idempotency-Key: "idemp_alpha"`, `expected_version: 1`, `amount: 80`.
3. Client B supplies `Idempotency-Key: "idemp_beta"`, `expected_version: 1`, `amount: 50`.
4. Command handler A acquires row lock on `acc_55` via `SELECT FOR UPDATE`.
5. Command handler A checks balance ($100 \ge 80$), updates balance to \$20, advances version to 2, inserts outbox event, and marks `idemp_alpha` as `COMPLETED` in the same transaction. Commit succeeds.
6. Command handler B acquires row lock on `acc_55`.
7. Handler B evaluates `expected_version == 1`. Loaded version is now 2.
8. Handler B rejects mutation immediately with HTTP `409 Conflict` (`CONFLICT` outcome).
9. Account balance is never overdrawn; invariants are strictly maintained.


---

## 3. Amendment 2026-11-07: Token and Key Handling

| Failure ID | Failure Mode | Severity | Invariant Violated | Detection Mechanism | Automated / Operational Remediation |
|---|---|---|---|---|---|
| **F7.1** | Invalid consistency token silently treated as IMMEDIATE | CORRUPT | CQ-04 (read-your-writes) | Client receives 200 without the version it expected | `ResolveQuery` returns `ErrInvalidConsistencyToken`; HTTP 400. Malformed, expired and bad-signature tokens are intentionally indistinguishable. Operator: have the client re-mint a token from the command response, or omit the token to accept IMMEDIATE. |
| **F7.2** | Signing key secret echoed in API response | SECURITY | Secret write-only | Response contract review | `signingKeyResponse` omits `raw_secret_value`. Operator: rotate any key whose response was logged before this fix. |
| **F7.3** | `secret_key_hash` still returned | SECURITY | Minimise key-derived material | Response contract review | **Open**: still echoed. Operator: treat the hash as sensitive in logs. |
| **F7.4** | No active tenant signing key at resolve time | SECURITY | Per-tenant keys | Code review (default secret in `ResolveQuery`) | **Open**: falls back to a built-in default secret. Operator: register a signing key for every tenant. |

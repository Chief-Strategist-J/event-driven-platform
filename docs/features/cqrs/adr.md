# Architectural Decision Record: Command Query Responsibility Segregation (CQRS) & Read-Your-Own-Writes Token Resolution (WP-15)

## 1. Title & Metadata
* **Status**: PROPOSED
* **Date**: 2026-11-06
* **Decision Makers**: Platform Architecture Guild, Core Distributed Systems Group, Chief Data Architect
* **Governing Specification**: [CQRS — Production System Design Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/cqrs/business-logic.md)

---

## 2. Context & Problem Statement
In conventional CRUD architectures, the same database schema and data models serve both command mutations and query retrievals. In a high-throughput event-driven platform, coupling write operations with read views introduces catastrophic bottlenecks:
1. **Contention & Locking Collisions**: Complex multi-table joins and reporting aggregations acquire shared read locks or consume high CPU/IOPS on the primary transactional database, degrading latency for latency-sensitive OLTP command writes.
2. **Stale Decision Making (F4.1, CORRUPT)**: If command handlers read from eventually consistent read models or cached materialized views to evaluate domain business rules (e.g., checking available balance or inventory before executing a debit), concurrent operations cause silent over-allocation and negative balance corruption.
3. **Cascading Primary Outages Under Projection Lag (F8.1, OUTAGE)**: When downstream projection consumers lag behind Kafka queues, impatient API clients re-query. If read endpoints implement a generic fallback to query the primary database, read traffic floods the write database, turning a transient queue lag into a total database outage.
4. **Side-Effecting GET Queries (F2.1, CORRUPT)**: Read endpoints that synchronously modify database state (such as updating "last seen" timestamps or view counters) turn HTTP GET requests into mutations, causing data corruption during HTTP crawler prefetching and proxy retries.
5. **Consistency Token Forgery (F6.1, SECURITY)**: If API clients are permitted to pass arbitrary version integers in request headers to demand strong consistency, malicious actors can pass `version = 9999999` to force infinite polling loops and primary database exhaustion.

To achieve linear horizontal scalability, strict data integrity, and deterministic read-your-own-writes (RYW) guarantees, the platform mandates strict architectural segregation of Commands and Queries.

---

## 3. Decision Drivers & Architecture Invariants (CQ-01 to CQ-12)

* **CQ-01: Commands are Idempotent by Key [Certain]**:
  Every command requires an idempotency key passed via header or payload. Idempotency state (`IN_PROGRESS`, `COMPLETED`, `REJECTED`, `CONFLICT`) and execution outcomes are persisted in the **same database transaction** as the aggregate mutation and transactional outbox event.
* **CQ-02: Decisions Use State Read Under Concurrency Checks [Certain]**:
  Command handlers must load aggregate state and execute invariant validation within a single transaction guarded by row-level locks (`SELECT FOR UPDATE`) or optimistic concurrency version checks (`WHERE version = loaded_version`). Pre-lock validation is forbidden (eliminating TOCTOU vulnerabilities, F3.4).
* **CQ-03: User-Edit Commands Carry Expected Version [Certain]**:
  All user-initiated edit commands must supply `expected_version`. Version mismatches immediately return HTTP `409 Conflict` (`CONFLICT` outcome), preventing lost updates on concurrent browser sessions (F3.5).
* **CQ-04: Commands Never Read the Read Model for Decisions [Certain]**:
  Command execution logic and business invariant checkers have zero read credentials for projection tables, Valkey/Redis views, or OpenSearch indices. All reference data required for decisions is loaded from authoritative write-side tables.
* **CQ-05: Cross-Aggregate Invariants Enforced by Write-Side Mechanisms [Certain]**:
  Cross-aggregate constraints (such as unique email/username or inventory reservation limits) are enforced exclusively by database unique constraints, reservation tables, or orchestrated sagas—never by querying read views.
* **CQ-06: Queries Have No Side Effects [Certain]**:
  Query handlers execute with read-only database credentials. Side-effecting operations (such as audit logging or view counter incrementing) must be published as asynchronous fire-and-forget events, never blocking the query path (F2.2).
* **CQ-07: Consistency Tokens are Server-Issued, Signed, Bounded, and Expiring [Certain]**:
  Consistency tokens (`c_tok_...`) issued upon command completion are cryptographically signed (HMAC-SHA256) and carry expiration TTLs ($5 \le TTL \le 15\text{ minutes}$). Unsigned, forged, expired, or out-of-scope tokens are treated strictly as `EVENTUAL` and increment security anomaly metrics.
* **CQ-08: Version Lives Inside the View Record [Certain]**:
  Materialized read views store the aggregate version within the record payload (`version` column). The query token resolver inspects the record's own version to evaluate satisfaction.
* **CQ-09: Lag Never Cascades into the Write Model [Certain]**:
  Query timeouts on lagging views must **never** fall back to querying the primary transactional database. If a view cannot satisfy a token within its bounded wait budget ($300\text{--}800\text{ms}$), the query returns the existing record flagged with `consistency_status = "PENDING"` or fast-fails without touching the write model.
* **CQ-10: Authorization is Evaluated at Read Time [Certain]**:
  Read models carry resource ownership and tenant attributes, but never hardcoded permission grants. Query handlers authorize caller access dynamically via the authorization engine (`C-AUTH`) with short-lived permission caching and real-time revocation push ($\text{SLO} \le 60\text{s}$, F10.1).
* **CQ-11: Response Shape is Stable Regardless of Resolution Path [Certain]**:
  In the narrow instances where an Authoritative-Own read (`C-AUTHR`) is permitted for an aggregate owner, the write-model record is projected using the **exact same serialization library** as the projection pipeline, guaranteeing identical JSON response schemas.
* **CQ-12: Ambiguous Command Outcomes are Resolvable [Certain]**:
  If a network timeout occurs during command submission, retrying with the same idempotency key returns the deterministic cached outcome. In addition, an explicit command status query API is exposed.

---

## 4. Considered Architectural Options

### Option A: Unified Monolithic Active-Record Architecture
* **Description**: Single domain models perform both validation and querying against the same PostgreSQL tables with read-replicas for load relief.
* **Drawbacks**: Severe lock contention during long reports; read replicas suffer replication lag without consistency tokens, causing broken read-your-own-writes user experience; command mutations accidentally rely on unindexed read views.
* **Verdict**: REJECTED.

### Option B: Asymmetric CQRS with Unbounded Read Fallback
* **Description**: Separate write and read models, but permit read endpoints to automatically redirect queries to the primary database when read views are stale.
* **Drawbacks**: Under consumer partition stalls or traffic spikes, 100% of read queries fall back to the primary database, causing catastrophic resource exhaustion and total system outage (F8.1).
* **Verdict**: REJECTED.

### Option C: Strict Hexagonal CQRS with Signed Token-Bounded Resolution (Chosen Architecture)
* **Description**: Complete decoupling of Command and Query execution paths. Commands execute against the write model, emitting outbox events and returning signed cryptographic consistency tokens. Queries read from optimized read views, resolving tokens via bounded non-polling asynchronous waits ($300\text{--}800\text{ms}$) with fast-fail `PENDING` statuses and zero fallback to the primary store.
* **Verdict**: APPROVED.

---

## 5. System Architecture & Flow Model

```
+-----------------------------------------------------------------------------------+
|                                  API GATEWAY (C-GW)                               |
|  - Validates API Tokens & Tenant Context                                          |
|  - Routes POST /api/v1/commands/* -> Command Bus (C-CMD)                          |
|  - Routes GET  /api/v1/queries/*  -> Query Bus   (C-QRY)                          |
+-----------------------------------------------------------------------------------+
         |                                                           |
         | [COMMAND PATH]                                            | [QUERY PATH]
         v                                                           v
+-------------------------------+                           +-------------------------------+
|     Command Handler (C-CMD)   |                           |      Query Handler (C-QRY)    |
| - Verify Idempotency Key      |                           | - Extract x-consistency-token |
| - Authorize Mutation          |                           | - Authorize at Read Time      |
| - Load Aggregate (Row Lock)   |                           +---------------+---------------+
| - Validate Invariants         |                                           |
| - Mutate & Write Outbox       |                                           v
| - Sign Consistency Token      |                           +-------------------------------+
+---------------+---------------+                           |   Token Resolver (C-TOK)      |
                | (Atomic Commit)                           | - Validate HMAC Signature     |
                v                                           | - Check View Version >= Token |
+-------------------------------+                           | - Bounded Wait (300-800ms)    |
|   Write Model (PostgreSQL)    |                           | - Fast-Fail to PENDING        |
| - Aggregates & Ledger         |                           +---------------+---------------+
| - Transactional Outbox        |                                           |
| - Command Idempotency Ledger  |                                           v
+---------------+---------------+                           +-------------------------------+
                |                                           |      Read View Stores         |
                v (CDC / Relay)                             | - Authoritative Postgres Views|
+-------------------------------+                           | - Redis KV Fast Caches        |
|   Kafka Projection Pipeline   | ------------------------> | - OpenSearch Search Indices   |
+-------------------------------+                           +-------------------------------+
```

---

## 6. Consistency Modes & Token Resolution Protocol

| Mode | Token Required | Resolution Behavior | Failure State | Fallback to Write Model? |
| :--- | :--- | :--- | :--- | :--- |
| `EVENTUAL` | No | Immediate read from view store; staleness bounded by view SLO. | Empty / 404 if not yet created. | **STRICTLY FORBIDDEN** |
| `TOKEN` | Yes (`x-consistency-token`) | Validates HMAC signature; checks record `version >= token.min_version`. If behind, performs bounded wait. | Returns view record with `consistency_status = "PENDING"`. | **STRICTLY FORBIDDEN** |
| `AUTHORITATIVE-OWN` | Yes (`x-consistency-token` + Owner Match) | Strongly consistent read on write model for specific high-value flows (e.g. checkout receipt). | Rate-limited (10 req/s); circuit-broken on DB latency $> 100\text{ms}$. | Permitted (Only via dedicated `C-AUTHR` endpoint) |

### 6.1 Token Cryptographic Structure
Tokens are opaque URL-safe base64 strings containing an HMAC-SHA256 signature:
$$\text{Signature} = \text{HMAC-SHA256}_{K_{\text{sign}}} (\text{TenantID} \parallel \text{PrincipalID} \parallel \text{AggregateID} \parallel \text{MinVersion} \parallel \text{ExpiresAt})$$

---

## 7. PostgreSQL Persistence Model (Migration `0019`)
The database schema introduces three relational tables supporting CQRS infrastructure:

1. **`cqrs_command_idempotency_ledger`**:
   Tracks command keys, request payload fingerprints (SHA-256), execution state (`IN_PROGRESS`, `COMPLETED`, `REJECTED`, `CONFLICT`), cached outcome payloads, and signed tokens.
2. **`cqrs_aggregate_root_ledger`**:
   Maintains core aggregate identities, current sequence versions, and optimistic lock concurrency states for transactional command mutations.
3. **`cqrs_token_signing_key_registry`**:
   Manages active and rotated HMAC signing keys with cryptographic key versioning.

---

## 8. Consequences & Trade-offs

### Positive
- **Independent Elastic Scaling**: Read query handlers scale horizontally on read views without adding lock contention or memory pressure to the transactional write database.
- **Deterministic Read-Your-Own-Writes**: Users never experience UI rollbacks or phantom state resets after submitting mutations.
- **Resilience Against Thundering Herds**: Bounded non-polling waits and explicit `PENDING` returns ensure lagging projections never crash the write model.

### Negative & Mitigations
- **Increased Architectural Complexity**: Maintaining separate command and query DTOs requires disciplined interface contracts.
  - *Mitigation*: Enforce hexagonal code organization and shared validation normalizers.
- **Eventual Consistency Latency**: Non-tokened queries may reflect slight staleness ($< 200\text{ms}$).
  - *Mitigation*: Freshness SLOs are continuously monitored and alerted via WP-14.


## Amendment 2026-11-07: Consistency-token error semantics and signing-key response

### Problem observed
- `GET /api/v1/queries/resolve` with a garbage `x-consistency-token` returned 200 with `consistency_status: IMMEDIATE`, silently ignoring the token and giving the caller no read-your-writes guarantee.
- `POST /api/v1/commands/keys` echoed `raw_secret_value` in the 201 response.

### Root cause
- RC4-style silent fallback: `CQRSService.ResolveQuery` returned a synthetic IMMEDIATE result whenever `rules.VerifyConsistencyToken` returned `valid == false`, so an invalid token was indistinguishable from "no token".
- `HandleRegisterSigningKey` serialised the stored record struct directly, including the write-only secret.

### Decision / fix
- `ResolveQuery` returns the new sentinel `contracts.ErrInvalidConsistencyToken`; the handler maps it to 400 (the resolve handler maps service errors to 400 `VALIDATION_FAILED` with the error text). A request with no token still returns IMMEDIATE.
- Token error semantics: `VerifyConsistencyToken` returns only `(nil, false)` for a missing `c_tok_` prefix, bad base64, bad JSON, expiry, and signature mismatch. The API therefore cannot, and deliberately does not, say which of these occurred; the message is "malformed, expired, or has an invalid signature". This avoids giving an attacker an oracle for forging tokens and for probing expiry, at the cost of making client debugging harder (a client must re-mint a fresh token).
- Signing-key registration returns a dedicated `signingKeyResponse` that omits `raw_secret_value`.

### API-visible behavior change
- Invalid token: 200 IMMEDIATE becomes 400. **Breaking** for clients that sent stale/invalid tokens and relied on the silent fallback; they must refresh the token or omit it.
- `POST /commands/keys`: `raw_secret_value` no longer in the response. **Breaking** for any client that read the secret back; the caller already supplied it.
- `secret_key_hash` is still returned (see gaps).

### Verification
Re-tested live: garbage token returns 400; key registration response has no `raw_secret_value`. Existing token unit tests (`cqrs_unit_test.go`: mint/verify, tampered signature, expired) cover the verifier; no new unit test was added for the service-level 400 or the response shape.

### Remaining gaps
- `secret_key_hash` is still echoed in the response.
- If no active signing key exists for the tenant, `ResolveQuery` falls back to a hard-coded default secret (`default_system_cqrs_secret_key_32` in `cqrs.service.go`); this was not changed and is a security concern worth a separate decision.
- The handler maps all service errors from resolve to 400, including non-validation failures.

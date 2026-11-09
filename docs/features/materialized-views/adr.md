# Architectural Decision Record: Materialized Views, Projection Monotonicity & Zero-Downtime Rebuilds (WP-13)

## 1. Title & Metadata
* **Status**: APPROVED
* **Date**: 2026-11-06
* **Decision Makers**: Platform Architecture Guild, Data Infrastructure Board, Chief Distributed Systems Architect
* **Governing Specification**: [Materialized Views Production System Design Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/materialized-views/business-logic.md)

---

## 2. Context & Problem Statement
In event-driven architectures, read models and materialized views are decoupled from write models. Events stream from Kafka or transactional outbox logs into projection handlers that maintain denormalized read views. Without strict architectural controls, this decoupling introduces critical vulnerabilities:

1. **Out-of-Order / Backward State Overwrites (MV-01, CORRUPT)**: Events arriving out of order (due to transient network partitions, consumer group rebalances, or parallel retries) can cause an older event version (e.g., sequence 2) to overwrite a newer materialized projection (e.g., sequence 5), silently rolling back aggregate state.
2. **Silent Failure on Zero-Row Updates (MV-02, CORRUPT)**: Standard SQL UPDATE statements return success even when 0 rows match the conditional WHERE clause. Treating a zero-row update as a success masks concurrent race conditions and causes silent data drift.
3. **Dual-Write Inconsistencies Across Derived Stores (MV-04, MV-05, DRIFT)**: Materialized views often fan out to multiple query stores (PostgreSQL relational tables, Redis key-value caches, OpenSearch full-text indices). Dual-writing to secondary stores without an authoritative primary store leads to irreconcilable divergence when network hiccups occur.
4. **Catastrophic Rebuild Downtime (MV-10, OUTAGE)**: Truncating or dropping an active view table to rebuild it from historical event streams causes catastrophic query outages for API consumers during the entire duration of the backfill (which can take hours or days for large topics).
5. **Phantom / Unquarantined Event Gaps (F3.2, CORRUPT)**: If event sequence 3 is missed and event sequence 4 arrives, applying sequence 4 directly produces corrupt aggregations (such as balance calculations or delta rollups). Gaps must be detected and quarantined until missing sequences arrive or timeouts expire.
6. **GDPR / Tenant Erasure Leaks (MV-11, COMPLIANCE)**: If views store plaintext tenant data without cryptographic isolation or tenant-scoped lifecycle boundaries, crypto-shredding on the primary event store leaves zombie personal data in read models.

---

## 3. Decision Drivers & Architecture Invariants (MV-01 to MV-12)

* **MV-01: Every write is version-guarded by the store that holds the data [Certain]**:
  Every projection update evaluates aggregate sequence monotonicity: `new_version == current_version + 1`. Regression attempts (`new_version < current_version`) are rejected.
* **MV-02: Zero-row update results are failures, never silent success [Certain]**:
  Conditional atomic updates verify that exactly 1 row was updated. A 0-row return is mapped to an explicit concurrency conflict error and retried or quarantined.
* **MV-03: Dedupe, guard, mutation, and position are atomic per store [Certain]**:
  Within PostgreSQL, projection state mutation, version check, idempotency hash verification, and cursor advancement are executed in a single atomic transaction.
* **MV-04: One authoritative primary store per view [Certain]**:
  Every materialized view designates exactly one primary store (PostgreSQL) as the source of truth. Secondary query targets are derived.
* **MV-05: Derived stores (Redis/OpenSearch) are disposable and rebuilt from primary [Certain]**:
  Redis and OpenSearch are never treated as independent authorities. If a derived index drifts, it is wiped and re-projected from the primary view tables.
* **MV-06: Every view defines its consistency mode (Eventual, Token-Bounded, Strict-Serial) [Certain]**:
  API callers receive explicit consistency contracts. Token-bounded views support read-your-own-writes via monotonic version tokens (`cv_tok_{view}_{seq}`).
* **MV-07: Projections are strictly deterministic functions of event payloads [Certain]**:
  Projection handlers contain zero external I/O, zero random generation, and zero system clock dependencies. The same sequence of events produces byte-for-byte identical projections.
* **MV-08: Views never write back to write models [Certain]**:
  Unidirectional data flow is strictly enforced. Read models never emit domain command mutations back to transactional write tables.
* **MV-09: Freshness is explicit and measured against SLO [Certain]**:
  Each view registry record defines `freshness_slo_ms`. Materialization lag exceeding the SLO triggers metric alerts and degrades the view status.
* **MV-10: View rebuilds use zero-downtime blue/green swap [Certain]**:
  Rebuilding a view populates a shadow table (`{view_name}_shadow`) in the background via historical replay + stream catchup. Once caught up to target LSN, an atomic table rename or pointer swap switches live queries with zero query downtime.
* **MV-11: Views are tenant-scoped and cryptographic-erasure aware [Certain]**:
  All tables enforce composite tenant keys `(tenant_id, id)`. Cryptographic shredding invalidates projection keys immediately.
* **MV-12: Materialized view definitions are versioned code [Certain]**:
  Schema migrations and projection transformations are managed via versioned migration scripts and OpenAPI contracts.

---

## 4. Considered Alternatives

### Alternative A: Postgres Native Materialized Views (`REFRESH MATERIALIZED VIEW CONCURRENTLY`)
* **Pros**: Native database feature; simple SQL syntax.
* **Cons**: Blocks writes or locks tables during refresh; cannot handle high-throughput streaming events from Kafka; requires unique index on all columns; cannot fan out to Redis or OpenSearch; no gap quarantine capability.
* **Decision**: REJECTED.

### Alternative B: Direct Dual-Writing from Consumer Handlers to Postgres + Redis + OpenSearch
* **Pros**: No separate replication pipeline.
* **Cons**: Non-atomic dual writes create partial failure modes; network timeout to OpenSearch leaves Postgres updated and search out-of-sync; rebuilds require replaying to all three targets simultaneously.
* **Decision**: REJECTED in favor of Authoritative Primary Store with Outbox / Changefeed Replication (`MV-04`, `MV-05`).

### Alternative C: Hexagonal Application-Managed Materialized Views with Blue/Green State Machine
* **Pros**: Application enforces version monotonicity, deterministic hashing, gap quarantine, and zero-downtime shadow table promotion; primary store provides transactional safety; derived stores are reliably backfilled.
* **Cons**: Requires dedicated state tracking tables (`materialized_view_projection_state`, `materialized_view_rebuild_log`).
* **Decision**: ADOPTED.

---

## 5. Consequences

### Positive Consequences
* Absolute projection integrity: stale events are dropped, duplicates are no-ops, and gaps are quarantined.
* Zero downtime during view re-indexing and schema evolution via the 5-phase blue/green state machine.
* Clear read-your-own-writes guarantees via token-bounded consistency.
* Polyglot storage readiness (PostgreSQL, Redis, OpenSearch) without dual-write corruption.

### Negative Consequences / Operational Trade-Offs
* Additional storage overhead for projection version records and held event quarantine queues.
* Rebuild execution requires temporary 2x storage footprint for shadow tables until old tables are decommissioned.

---

## 6. Detailed Architectural Decision Models & Interaction Blueprints

### 6.1 Version Monotonicity & Quarantine Decision Logic (§5)
Every projection mutation must pass through the deterministic decision matrix prior to acquiring a database write lock:

```mermaid
flowchart TD
    START(["Receive Event (Seq: S, Hash: H)"]) --> FETCH["Fetch State (Current: V, LastHash: LH)"]
    FETCH --> COND1{"Is S <= V?"}

    COND1 -->|Yes| COND2{"Is S == V?"}
    COND2 -->|Yes| COND_HASH{"Is H == LH?"}
    COND_HASH -->|Yes| RES_DUP["Decision: DUPLICATE_NOOP<br/>Return Success (No DB Mutation)"]
    COND_HASH -->|No| RES_INTEG["Decision: INTEGRITY_CONFLICT<br/>Quarantine Aggregate + Raise Alert F5.1<br/>(Split-Brain / Payload Tampering)"]
    COND2 -->|No| RES_STALE["Decision: STALE_DROP<br/>Log Warning & Discard Safely"]

    COND1 -->|No| COND3{"Is S == V + 1?"}
    COND3 -->|Yes| RES_APPLY["Decision: APPLY<br/>Execute Atomic UPSERT<br/>Set current_version = S"]
    COND3 -->|No| RES_GAP["Decision: GAP_HOLD<br/>Insert into materialized_view_held_events<br/>Quarantine until gap resolves"]

    RES_APPLY --> CHECK_HELD{"Are there held events<br/>for Seq == S + 1?"}
    CHECK_HELD -->|Yes| PROCESS_HELD["Pop next held event<br/>Re-evaluate under Decision Loop"]
    CHECK_HELD -->|No| COMPLETE(["Pipeline Complete"])
    PROCESS_HELD --> COND3
```

### 6.2 Zero-Downtime Blue/Green Table Rename DDL Sequence (MV-10)
To avoid blocking concurrent queries during view schema migration or backfills, the table promotion step runs in an explicit short-lived transaction with a 2-second lock timeout:

```mermaid
sequenceDiagram
    autonumber
    participant Op as Rebuild Operator
    participant PG as PostgreSQL Engine
    participant Active as active_table (orders_view)
    participant Shadow as shadow_table (orders_view_shadow)
    participant Readers as API Readers (SELECT)

    Note over Op,PG: Pre-Condition: Stream Catchup Lag < 100 rows
    Readers->>Active: Concurrent Read Queries (AccessShareLock)
    Op->>PG: BEGIN TRANSACTION;
    Op->>PG: SET LOCAL lock_timeout = '2s';
    Op->>PG: LOCK TABLE orders_view IN ACCESS EXCLUSIVE MODE;
    Note over PG: Waits for active in-flight SELECTs to complete (<2s)
    Op->>PG: ALTER TABLE orders_view RENAME TO orders_view_old;
    Op->>PG: ALTER TABLE orders_view_shadow RENAME TO orders_view;
    Op->>PG: COMMIT;
    Note over PG: Lock Released. Total write blocking window < 15ms
    Readers->>Active: Subsequent SELECTs automatically route to NEW table
    Op->>PG: DROP TABLE orders_view_old;
    Note over Op: Rebuild Lifecycle Complete
```

### 6.3 Polyglot Storage Guarding Mechanics (MV-01, MV-04, MV-05)
Derived stores must enforce identical monotonicity without independent authority:

```mermaid
flowchart TD
    subgraph EventStream ["Incoming Event (Seq: 42)"]
        IN["Projection Event Payload"]
    end

    subgraph PostgresBranch ["PostgreSQL Primary Authority (MV-01)"]
        P_QUERY["UPDATE materialized_view_projection_state<br/>SET current_version = 42, payload_hash = $H<br/>WHERE aggregate_id = $ID AND current_version = 41"]
        P_CHECK{"Rows Affected == 1?"}
        P_SUCCESS["Commit Transaction<br/>Advance In-Database Position"]
        P_FAIL["Rollback Transaction<br/>Return 409 Conflict (Zero-Row Failure)"]

        P_QUERY --> P_CHECK
        P_CHECK -->|Yes| P_SUCCESS
        P_CHECK -->|No| P_FAIL
    end

    subgraph RedisBranch ["Valkey / Redis Atomic CAS (MV-01)"]
        R_LUA["EVAL script:<br/>local curr = redis.call('HGET', KEYS[1], 'v')<br/>if not curr or tonumber(curr) + 1 == tonumber(ARGV[1]) then<br/>  redis.call('HSET', KEYS[1], 'v', ARGV[1], 'data', ARGV[2])<br/>  return 1<br/>end<br/>return 0"]
        R_CHECK{"Script Return == 1?"}
        R_OK["Cache Updated"]
        R_REJECT["Reject Regression / Trigger Re-Sync"]

        R_LUA --> R_CHECK
        R_CHECK -->|Yes| R_OK
        R_CHECK -->|No| R_REJECT
    end

    subgraph OpenSearchBranch ["OpenSearch External Versioning (MV-01)"]
        OS_REQ["PUT /customer_search/_doc/cust-101?version=42&version_type=external_gte<br/>{ 'name': 'Alice', 'version': 42 }"]
        OS_CHECK{"HTTP Status?"}
        OS_OK["200 OK / 201 Created<br/>Document Indexed"]
        OS_CONFLICT["409 VersionConflictEngineException<br/>Stale Document Dropped Safely"]

        OS_REQ --> OS_CHECK
        OS_CHECK -->|200/201| OS_OK
        OS_CHECK -->|409| OS_CONFLICT
    end

    IN --> P_QUERY
    P_SUCCESS --> R_LUA
    P_SUCCESS --> OS_REQ
```

---

## 7. Operational Defenses & Verification

* **Full Diagram Inventory**: Complete 9-diagram operational suite is cataloged in [Materialized Views Diagrams](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/materialized-views/diagrams.md).
* **Failure Catalog**: Complete 18-scenario forensic catalog is maintained in [Materialized Views Failure Cases](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/materialized-views/failure-cases.md).
* **CI Verification Gate**: Every commit verifies version monotonicity guards, state machine transitions, and zero inline comments.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Executing heavy analytical queries or real-time user dashboard aggregations directly against normalized transactional write tables causes table locks, CPU spikes, and query timeouts. Materialized views solve this by pre-computing and persisting denormalized read models in real time as domain events stream past.

### 2. Internal Algorithmic Mechanics & State Invariants

An asynchronous projection worker listens to relevant domain event topics. Upon receiving an event, it extracts entity mutations, validates the monotonic version sequence, and executes an atomic UPSERT into the read-optimized projection table. The projection table is indexed specifically for target query patterns (e.g. JSONB GIN indexes, composite tenant keys). If the projection needs rebuilding, the worker replays events into a shadow table and executes an atomic rename.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Deterministic Stream Projection Replay
* **Design Philosophy & Epistemology**:
  A materialized view is an ephemeral, derivative cache of history, not an authoritative source of truth. If a database index corrupts or the view schema evolves, you must never patch it with manual SQL updates. The philosophy is Complete Derivability from History: any materialized view must be capable of being completely dropped and rebuilt from the raw event stream with zero loss of truth.
* **Threat Vector Defeated**: Permanent view corruption and data loss caused by operational accidents, unversioned schema migrations, or corrupted search indices.
* **Storage & State Invariant**:
```text
Replay(AllEvents(Stream)) ==> MaterializedView_State(Deterministic)
Direct SQL mutations on MaterializedView tables strictly prohibited
```
* **Implementation Mechanism**:
  View tables are updated exclusively by consumer projectors consuming Kafka event streams. Rebuild tooling spins up shadow tables (`view_shadow_v2`), streams historical events from offset 0, and executes an atomic atomic table rename (`ALTER TABLE ... RENAME`) once caught up.
* **Non-Negotiable Trade-Off**: Historical event storage requirements on Kafka or tiered object storage (S3) to enable full replays.

---

#### Principle 2: Monotonic Version Guarding (Stale-Write Rejection)
* **Design Philosophy & Epistemology**:
  In an asynchronous event network, duplicate and out-of-order events will arrive at the view projection. If an older event updates a newer view record, the query layer displays corrupt, backward-traveling state. The philosophy is Strict Version Monotonicity: a view row may only be updated if the incoming event's version strictly exceeds the row's currently recorded version.
* **Threat Vector Defeated**: View state corruption where stale event retries overwrite newer state (e.g., an OrderPending event overwrites an OrderShipped view).
* **Storage & State Invariant**:
```text
UpdateView(Row) permitted ONLY IF IncomingEvent.Version > Row.LastAppliedVersion
WHERE id = $1 AND last_applied_version < $2
```
* **Implementation Mechanism**:
  Every view table includes a `last_applied_version` column. Projection updates execute SQL with optimistic locking predicates: `UPDATE customer_summary SET balance = $1, last_applied_version = $2 WHERE customer_id = $3 AND last_applied_version < $2`. Stale events affect 0 rows and are safely skipped.
* **Non-Negotiable Trade-Off**: Requires every domain event to carry a strictly increasing monotonic aggregate version number.

---

#### Principle 3: Zero Write-Plane Contention Isolation
* **Design Philosophy & Epistemology**:
  Running heavy, multi-table analytical JOIN queries against transactional write databases creates lock contention, degrades user write latency, and triggers transaction rollbacks. The philosophy is Physical Read-Write Isolation: transactional writes execute against normalized tables; analytical queries execute against completely isolated, denormalized materialized view tables.
* **Threat Vector Defeated**: Transactional database lock exhaustion and user API latency spikes caused by heavy analytical queries locking operational tables.
* **Storage & State Invariant**:
```text
TransactionalWritePlane and MaterializedViewPlane reside on isolated database connections / clusters
Query locks on MaterializedViews have 0% impact on operational ACID transactions
```
* **Implementation Mechanism**:
  Projectors stream events from Kafka and write directly to dedicated read replica databases, Elasticsearch clusters, or Redis caches. Client read APIs query the materialized view cluster exclusively, completely bypassing the write database.
* **Non-Negotiable Trade-Off**: Read-after-write eventual consistency delay (typically 20ms - 200ms) between write transaction commit and read-view availability.

---

#### Principle 4: Blue-Green Shadow View Rebuilding
* **Design Philosophy & Epistemology**:
  Altering the schema of a 100-million-row materialized view in-place requires long-running `ALTER TABLE` locks that lock out queries and risk table corruption. The philosophy is Zero-Downtime Blue-Green Cutover: build the new schema in a parallel shadow table, catch up with live events, and switch pointers atomically.
* **Threat Vector Defeated**: Multi-hour query downtime and table lock timeouts during schema migrations on high-traffic view tables.
* **Storage & State Invariant**:
```text
ActiveView availability == 100% during migration
Cutover time == Atomic Metadata Rename (sub-millisecond)
```
* **Implementation Mechanism**:
  The migration engine creates `view_shadow`. A projection worker consumes historical events from the beginning of the stream to populate `view_shadow`. Once lag reaches zero, an atomic transaction renames `view_active` to `view_legacy` and `view_shadow` to `view_active`.
* **Non-Negotiable Trade-Off**: Temporary 2x storage overhead during the migration while both active and shadow tables coexist.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is storing pre-computed aggregations in a materialized view superior to querying normalized tables with SQL JOINs?**
   * *Answer*: Pre-computed materialized views shift the computational cost from read-time to write-time. Instead of 1,000 users executing expensive 10-table JOIN queries every second, the projection calculates the joined result once on event receipt, serving reads in sub-5ms O(1) lookups.
2. **What happens if a projection worker crashes mid-way through updating a materialized view?**
   * *Answer*: Because the update is guarded by monotonic version predicates and the Kafka offset is committed only after the database write succeeds, the recovered worker reprocesses the event, safely reapplies the idempotent update, and advances without corrupting the view.
3. **How do you detect if a materialized view has silently drifted from primary database state?**
   * *Answer*: A background drift detector periodically computes cryptographic checksums across random samples of primary aggregates and compares them with the corresponding materialized view records, alerting if drift is detected.

---
## Authoritative References & Production Citations

* **[Martin Kleppmann: Designing Data-Intensive Applications (Chapter 11: Stream Processing)](https://dataintensive.net/)**
  - *Architectural Rationale & Implementation Grounding*: Comprehensive coverage of derived data systems, materialized view maintenance, and stream-table duality.
* **[Jay Kreps: The Log: What every software engineer should know about real-time data's unifying abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)**
  - *Architectural Rationale & Implementation Grounding*: Seminal LinkedIn engineering blog establishing the log as the universal source of truth and views as derived caches.
* **[PostgreSQL Official Documentation: Materialized Views and Concurrent Refresh](https://www.postgresql.org/docs/current/rules-materializedviews.html)**
  - *Architectural Rationale & Implementation Grounding*: Technical guide on relational materialized views, indexing strategies, and concurrent atomic refresh mechanisms.
* **[Confluent: Real-Time Stream-Table Duality in Event-Driven Systems](https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streams-as-the-source-of-truth/)**
  - *Architectural Rationale & Implementation Grounding*: Deep architectural dive into duality between changelog streams and materialized state tables.

## Amendment 2026-11-07: Duplicate-view 409, unknown-view 404, and per-tenant rebuild uniqueness

### Problem observed
- Registering a duplicate view or starting a duplicate rebuild returned `400` carrying a raw constraint name.
- `POST /api/v1/materialized-views/query` for an unknown view returned `200` with `version 0`, indistinguishable from an empty-but-real view.

### Root cause
- RC2: unique violations went through the generic `database.MapDatabaseError`, which produced a class-23 message including the constraint name.
- RC3/RC4: `QueryViewWithConsistencyToken` never checked that the view exists.

### Decision / fix
- `mapUniqueViolation` in the Postgres adapter converts SQLSTATE 23505 into a conflict `ApplicationError` (409) with domain messages: "a materialized view with this view_name is already registered for the tenant" (`RegisterView`) and "a rebuild with this rebuild_id already exists" (`InitiateRebuild`). Other errors keep using `MapDatabaseError`.
- `QueryViewWithConsistencyToken` validates `tenant_id` and `view_name` non-empty (400), then calls `GetViewByName`, so an unknown view returns 404.
- Uniqueness facts: `materialized_view_registry.view_name` is already `UNIQUE (tenant_id, view_name)` (an earlier claim that it was global was wrong). `materialized_view_rebuild_log.rebuild_id` was globally unique (migration 0015), which let one tenant block another.
- **Migration 0028** (`database/migrations/sql/0028_scope_partitioning_and_rebuild_uniqueness_per_tenant.sql`, with `.rollback.sql`) changes it to `UNIQUE (tenant_id, rebuild_id)` (constraint `uk_materialized_view_rebuild_log_tenant_rebuild_id`, replacing `uk_materialized_view_rebuild_log_rebuild_id`); it also scopes two partitioning constraints (see partitioning-strategy). Reversible, lock risk LOW, schema only. **NOT applied to the live database** (`serve` does not run migrations; the live DB has no `schema_migrations` table) - apply through the migration tooling. Rollback re-adds global uniqueness and fails if tenants already share a rebuild_id.

### API-visible behavior change
Duplicate view/rebuild `400` -> `409`; unknown view query `200 version 0` -> `404` (**breaking** for clients that polled until the view appeared); missing tenant/view `400`.

### Verification
Live: duplicate view -> 409; unknown view query -> 404. Unit test `TestQueryViewUnknownViewIsNotFound` in `src/features/materialized-views/tests/materialized_views_query_unit_test.go`.

### Remaining gaps
The rebuild-duplicate 409 and the 0028 constraint change are unverified against live Postgres (migration not applied; no Postgres-backed CI test). Until 0028 is applied, a rebuild_id collision across tenants still conflicts.


## Amendment 2026-11-07 (second pass): migration 0028 applied

Migration 0028 was applied to the live dev database: `materialized_view_rebuild_log.rebuild_id` is now unique per `(tenant_id, rebuild_id)`. Verified for partitioning topics (two tenants registered the same `topic_name`).

Verified live on the compose-deployed image (`chiefj/event-driven-platform:v2`) and by unit tests; see ADR-0002 "Closed in a second pass".

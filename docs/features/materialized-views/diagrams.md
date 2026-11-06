# Materialized Views Architecture & Execution Diagrams (WP-13)

This document contains Mermaid diagrams illustrating the runtime architectures, decision engines, state machines, polyglot storage guards, and zero-downtime cutover mechanics of the Materialized Views system per the [Materialized Views Business Logic Specification](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/materialized-views/business-logic.md).

---

## 1. End-to-End View Projection Pipeline

```mermaid
flowchart TD
    subgraph Ingress ["Event Ingress Layer"]
        EV["Incoming Domain Event<br/>(Kafka / Outbox CDC)"]
        HD["API Handler / Stream Consumer<br/>(Extracts W3C Traceparent)"]
    end

    subgraph Guard ["Monotonicity Guard & ACL Layer"]
        VAL["Validation & Normalization<br/>(materialized-views.schema)"]
        STATE["Query Current Aggregate Version<br/>(materialized_view_projection_state)"]
        DEC["Evaluate Version & Payload Hash<br/>(materialized-views.rules)"]
    end

    subgraph Outcomes ["Projection Execution Branches"]
        DUP["Duplicate No-Op<br/>(Event dropped safely)"]
        STALE["Stale Sequence Drop<br/>(Logged & discarded)"]
        GAP["Gap Quarantine<br/>(Stored in held_events queue)"]
        INTEG["Integrity Conflict Hold<br/>(Same version, diverging hash)"]
        APPLY["Atomic Mutation & Position Advance<br/>(Primary PostgreSQL Table)"]
    end

    subgraph Fanout ["Derived Store Replication"]
        CDC["Postgres Logical Decoding / Outbox"]
        REDIS["Valkey / Redis KV Store"]
        OS["OpenSearch Vector & Text Index"]
    end

    EV --> HD
    HD --> VAL
    VAL --> STATE
    STATE --> DEC
    DEC -->|seq == current && hash match| DUP
    DEC -->|seq < current| STALE
    DEC -->|seq == current && hash mismatch| INTEG
    DEC -->|seq > current + 1| GAP
    DEC -->|seq == current + 1| APPLY
    APPLY --> CDC
    CDC --> REDIS
    CDC --> OS
```

---

## 2. Version Monotonicity & Gap Quarantine Decision Tree (§5)

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

---

## 3. Blue/Green View Rebuild State Machine (MV-10)

```mermaid
stateDiagram-v2
    [*] --> INITIATED: InitiateRebuild API Called
    INITIATED --> HISTORICAL_REPLAY: Provision Shadow Table & Snapshot LSN
    HISTORICAL_REPLAY --> STREAM_CATCHUP: Replay Finished, Begin Kafka Tail
    STREAM_CATCHUP --> PROMOTED: Catchup Lag < Threshold, Atomic Table Swap
    PROMOTED --> DECOMMISSIONED_OLD: Drop / Archive Deprecated Table
    DECOMMISSIONED_OLD --> [*]

    INITIATED --> DECOMMISSIONED_OLD: Operator Abort / Error
    HISTORICAL_REPLAY --> DECOMMISSIONED_OLD: Fatal Backfill Crash
    STREAM_CATCHUP --> DECOMMISSIONED_OLD: Timeout / Catchup Overflow
```

---

## 4. Atomic PostgreSQL Blue/Green Table Swap Execution Sequence

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

---

## 5. Polyglot Store Concurrency & External Versioning Mechanics

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

## 6. Consistency Tokens & Read-Your-Own-Writes Flow (§9)

```mermaid
sequenceDiagram
    autonumber
    actor Client as API Client / Frontend
    participant Gateway as API Gateway / Router
    participant WriteSvc as Write Model Service
    participant Outbox as Transactional Outbox
    participant ViewSvc as View Projection Service
    participant ReadReplica as Primary View Store

    Client->>WriteSvc: POST /api/v1/orders (Create Order)
    WriteSvc->>WriteSvc: Commit Order + Outbox Record (Seq: 104)
    WriteSvc-->>Client: 201 Created (Token: cv_tok_orders_104)

    par Async Projection
        Outbox->>ViewSvc: Consume OrderCreated (Seq: 104)
        ViewSvc->>ReadReplica: Apply Projection (current_version = 104)
    and Read-Your-Own-Writes Query
        Client->>Gateway: GET /api/v1/orders/123 (Header: x-consistency-token: cv_tok_orders_104)
        Gateway->>ReadReplica: Read Projection State
        alt Current Version >= 104
            ReadReplica-->>Client: 200 OK (Strongly Consistent Read)
        else Current Version < 104 (Lagging)
            Gateway->>Gateway: Wait up to Timeout Budget (50ms)
            Gateway->>ReadReplica: Poll Re-read
            ReadReplica-->>Client: 200 OK (Satisfied)
        end
    end
```

---

## 7. V-AGG Delta Rollup vs Contribution Table Reconciliation DAG (§7.2, RC-03)

```mermaid
flowchart TD
    subgraph IncrementalPath ["Streaming Incremental Path"]
        DELTA_EV["Incoming Metric/Delta Event<br/>(e.g., OrderTotal: +$50.00)"]
        INC_UPDATE["Atomic SQL Increment:<br/>UPDATE view_account_totals<br/>SET balance = balance + 50.00, version = version + 1"]
        REC_CONTRIB["Record Raw Event in Contribution Table:<br/>INSERT INTO agg_contributions (event_id, delta, seq)"]
    end

    subgraph PeriodicRecon ["Reconciliation Worker (RC-03)"]
        TRIGGER["Periodic Audit Cron / Divergence Alert"]
        SUM_CONTRIB["Execute Full Recompute:<br/>SELECT SUM(delta) FROM agg_contributions WHERE agg_id = $ID"]
        FETCH_VIEW["SELECT balance FROM view_account_totals WHERE agg_id = $ID"]
        COMPARE{"Is Incremental == Recompute?"}
        AUDIT_PASS["Audit OK: Zero Drift Recorded"]
        REPAIR["Drift Detected: Overwrite View Balance with SUM<br/>Emit Critical Drift Alarm (F12.1)"]
    end

    DELTA_EV --> INC_UPDATE
    INC_UPDATE --> REC_CONTRIB

    TRIGGER --> SUM_CONTRIB
    TRIGGER --> FETCH_VIEW
    SUM_CONTRIB --> COMPARE
    FETCH_VIEW --> COMPARE
    COMPARE -->|Match| AUDIT_PASS
    COMPARE -->|Mismatch| REPAIR
```

---

## 8. Multi-Tenant Cryptographic Shredding Cascade (MV-11, MT-09)

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Tenant Admin / Privacy Officer
    participant Auth as Identity & Access Control
    participant KMS as Key Management Service (AWS KMS / Vault)
    participant ViewRegistry as Materialized View Registry
    participant PrimaryDB as PostgreSQL Primary Store
    participant DerivedSearch as OpenSearch Service
    participant DerivedCache as Redis Cluster

    Admin->>Auth: DELETE /api/v1/tenants/tenant-omega/crypto-shred
    Auth->>KMS: Destroy Tenant Data Encryption Key (DEK-omega)
    Note over KMS: Cryptographic Erasure Complete (Ciphertext is now Unrecoverable)

    Auth->>ViewRegistry: Mark Tenant Views as DECOMMISSIONED
    par Cascade Purge from Primary Store
        ViewRegistry->>PrimaryDB: DELETE FROM materialized_view_projection_state WHERE tenant_id = 'tenant-omega'
        ViewRegistry->>PrimaryDB: DELETE FROM materialized_view_held_events WHERE tenant_id = 'tenant-omega'
        ViewRegistry->>PrimaryDB: DELETE FROM customer_view_table WHERE tenant_id = 'tenant-omega'
    and Cascade Purge from Derived Stores
        ViewRegistry->>DerivedSearch: POST /customer_search/_delete_by_query (tenant_id == 'tenant-omega')
        ViewRegistry->>DerivedCache: SCAN + UNLINK tenant-omega:*
    end
    ViewRegistry-->>Admin: 200 OK (Cryptographic Shredding & Data Elimination Verified)
```

---

## 9. Polyglot Target Fan-Out & Reconciliation Architecture

```mermaid
flowchart LR
    subgraph PrimaryStore ["Authoritative Primary Store (MV-04)"]
        PG["PostgreSQL Materialized View Tables<br/>(materialized_view_registry)<br/>(materialized_view_projection_state)"]
    end

    subgraph Changefeed ["Transactional Changefeed"]
        WAL["PostgreSQL WAL / CDC Connector"]
    end

    subgraph DerivedStores ["Disposable Secondary Stores (MV-05)"]
        REDIS["Valkey / Redis<br/>KV Fast Lookups & Caches"]
        OS["OpenSearch<br/>Full-Text & Vector Search"]
    end

    subgraph Reconciler ["Reconciliation & Self-Healing Engine"]
        WORKER["Periodic Reconciliation Worker<br/>Compares Checksum / Count"]
    end

    PG -->|CDC Stream| WAL
    WAL --> REDIS
    WAL --> OS
    WORKER -.->|Scan Primary Hashes| PG
    WORKER -.->|Verify & Repair Divergence| REDIS
    WORKER -.->|Rebuild Stale Indices| OS
```

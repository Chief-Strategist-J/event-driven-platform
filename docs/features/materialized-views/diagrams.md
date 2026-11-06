# Materialized Views Architecture & Execution Diagrams (WP-13)

This document contains Mermaid diagrams illustrating the runtime architectures, decision engines, state machines, and consistency models of the Materialized Views system.

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
    DEC -->|seq <= current && hash mismatch| STALE
    DEC -->|seq > current + 1| GAP
    DEC -->|seq == current + 1| APPLY
    APPLY --> CDC
    CDC --> REDIS
    CDC --> OS
```

---

## 2. Version Monotonicity & Gap Quarantine Decision Tree

```mermaid
flowchart TD
    START(["Receive Event (Seq: S, Hash: H)"]) --> FETCH["Fetch State (Current: V, LastHash: LH)"]
    FETCH --> COND1{"Is S <= V?"}

    COND1 -->|Yes| COND2{"Is S == V AND H == LH?"}
    COND2 -->|Yes| RES_DUP["Decision: DUPLICATE_NOOP<br/>Return Success (No DB Write)"]
    COND2 -->|No| RES_STALE["Decision: STALE_DROP<br/>Log Warning & Discard"]

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

## 4. Consistency Tokens & Read-Your-Own-Writes Flow

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

## 5. Polyglot Target Fan-Out & Reconciliation Architecture

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

# Idempotent Consumer Visual Architecture and Invariant Flows

---

## 1. Complete System Flow

```mermaid
flowchart TD
    subgraph INGRESS["1. Transport Ingress Gate"]
        K1["Kafka Topic / HTTP API Ingress"] --> VAL{"Ingress Validation Gate<br/>(Schema, UUIDv7, Non-Empty Strings)"}
        VAL -- "Invalid" --> REJ["HTTP 400 Bad Request<br/>(Structured Error Envelope)"]
        VAL -- "Valid" --> HASH["Compute SHA-256 Payload Digest"]
    end

    subgraph LAYER1["2. Layer 1: Event Identity Deduplication Gate (IC-03, IC-08)"]
        HASH --> INBOX{"INSERT INTO consumer_inbox<br/>(tenant_id, handler_id, event_id)<br/>ON CONFLICT DO NOTHING"}
        INBOX -- "Rows Affected == 0" --> SKIP1["ActionSkip (Transport Duplicate)<br/>Commit Offset & Return 200 OK"]
        INBOX -- "Rows Affected == 1" --> READ_PROJ["Query consumer_projections<br/>FOR UPDATE (tenant_id, handler_id, aggregate_id)"]
    end

    subgraph LAYER2["3. Layer 2: Version Guard & Split-Brain Gate (IC-04, IC-05, IC-06)"]
        READ_PROJ --> CHK_PARK{"Is Aggregate Parked?<br/>(is_parked == TRUE)"}
        CHK_PARK -- "Yes" --> PARK_SUPPRESS["ActionParkPoison<br/>HTTP 423 Locked / Suppress Execution"]
        CHK_PARK -- "No" --> CHK_HASH{"v == last_version AND<br/>hash != last_payload_hash?"}
        CHK_HASH -- "Yes (Split-Brain)" --> PARK_TRIGGER["ParkAggregate(is_parked=TRUE)<br/>ActionParkPoison (HTTP 423 Locked)"]
        CHK_HASH -- "No" --> EVAL_VER{"Evaluate Version & Semantic Class"}
        
        EVAL_VER -- "v <= last_version" --> SKIP2["ActionSkip (Duplicate / Stale Version)<br/>Safe No-Op"]
        EVAL_VER -- "v > last_version + 1 (DELTA)" --> HOLD["ActionHoldGap (Event Loss Detected)<br/>HTTP 409 Conflict / Hold Key"]
        EVAL_VER -- "v == last_version + 1 (DELTA)<br/>or v > last_version (STATE)" --> APPLY["ActionApply<br/>Execute Business Handler"]
    end

    subgraph ATOMIC_SINK["4. Storage Gate & Transaction Boundary (IC-01, IC-07)"]
        APPLY --> TX["BEGIN PostgreSQL Transaction"]
        TX --> M1["Mutate Business Read Model"]
        TX --> M2["Upsert consumer_projections<br/>(last_version = v, last_hash = hash)"]
        TX --> M3["Store consumer_partition_offsets"]
        TX --> COMMIT{"COMMIT TX"}
        COMMIT -- "Success" --> ACK["Acknowledge Kafka Offset"]
        COMMIT -- "Error" --> ROLLBACK["ROLLBACK<br/>Map DB Constraint & Retry/Nack"]
    end
```

---

## 2. Invariant IC-01: Atomic Effect Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Engine as Consumer Engine
    participant DB as PostgreSQL (Single Transaction Boundary)
    participant Broker as Kafka Broker

    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: INSERT INTO consumer_inbox ...
    Engine->>DB: UPDATE business_projection ...
    Engine->>DB: UPDATE consumer_projections SET last_version = v ...
    Engine->>DB: INSERT INTO consumer_partition_offsets ...
    Engine->>DB: COMMIT TRANSACTION
    DB-->>Engine: Transaction Committed Durably
    Engine->>Broker: Commit Kafka Consumer Group Offset
```

---

## 3. Invariant IC-05: Version Rule State Machine

```mermaid
stateDiagram-v2
    [*] --> CheckVersion
    
    state CheckVersion <<choice>>
    CheckVersion --> ActionSkip : v <= last_version (Stale / Transport Duplicate)
    CheckVersion --> EvaluateDelta : v > last_version (Newer Event)

    state EvaluateDelta <<choice>>
    EvaluateDelta --> ActionApply : SemanticClass == STATE (Strictly newer wins)
    EvaluateDelta --> CheckDeltaStep : SemanticClass == DELTA

    state CheckDeltaStep <<choice>>
    CheckDeltaStep --> ActionApply : v == last_version + 1 (Sequential Next)
    CheckDeltaStep --> ActionHoldGap : v > last_version + 1 (Version Gap / Event Loss)

    ActionSkip --> [*]
    ActionApply --> [*]
    ActionHoldGap --> [*]
```

---

## 4. Invariant IC-08: Deduplication Scoped Per Handler

```mermaid
flowchart LR
    Event["Inbound Event (event_id: evt-100)"] --> HA["Handler A: Order Billing"]
    Event --> HB["Handler B: Inventory Allocation"]

    HA --> IA["consumer_inbox<br/>(tenant-1, handler-A, evt-100)"]
    HB --> IB["consumer_inbox<br/>(tenant-1, handler-B, evt-100)"]

    IA -- "Recorded" --> PA["Order Billing State Applied"]
    IB -- "Recorded" --> PB["Inventory State Applied"]
```

---

## 5. Failure Case Sequence Diagrams

### 5.1 F3.1.5 & F4.4: Zombie Consumer Overwrite Prevention
```mermaid
sequenceDiagram
    autonumber
    actor ZC as Zombie Consumer (Stalled GC)
    actor NC as New Owner Consumer (Partition Reassigned)
    participant DB as PostgreSQL (consumer_projections)

    Note over NC: Processes Event V=3 successfully
    NC->>DB: UPDATE consumer_projections SET last_version = 3 WHERE last_version < 3
    DB-->>NC: 1 Row Updated (last_version = 3)

    Note over ZC: Awakens from GC pause, attempts to apply Event V=2
    ZC->>DB: UPDATE consumer_projections SET last_version = 2 WHERE last_version < 2
    DB-->>ZC: 0 Rows Affected (Conditional Update Failed)
    Note over ZC: Stale write silently rejected; zero lost updates!
```

### 5.2 F3.2.4: Split-Brain Same Version Divergent Content Collision
```mermaid
sequenceDiagram
    autonumber
    actor Producer as Upstream Producer (Dual-Leader / DB Restore)
    participant Engine as Consumer Engine
    participant DB as PostgreSQL (consumer_projections)

    Producer->>Engine: Deliver Event V=2 (Payload: {amount: 100}, Hash: 0xAAAA)
    Engine->>DB: Apply V=2 (last_version = 2, last_payload_hash = 0xAAAA)
    
    Producer->>Engine: Redeliver Event V=2 (Payload: {amount: 200}, Hash: 0xBBBB)
    Engine->>DB: Query Projection (last_version=2, last_payload_hash=0xAAAA)
    Note over Engine: Detect Hash Mismatch: 0xAAAA != 0xBBBB!
    Engine->>DB: UPDATE consumer_projections SET is_parked = TRUE
    Engine-->>Producer: HTTP 423 Locked (ActionParkPoison)
    Note over Engine: Aggregate isolated; alerts on-call dashboard
```

### 5.3 F3.2.5: Out-of-Order First Event Arrival ($V > 1$)
```mermaid
sequenceDiagram
    autonumber
    actor Broker as Kafka Partition
    participant Engine as Consumer Engine
    participant DB as PostgreSQL (consumer_projections)

    Broker->>Engine: Ingest First Event Seen: V=2 (OrderShipped)
    Engine->>DB: GetProjection (not found -> current_version = 0)
    Note over Engine: DELTA Event expects V=0+1=1. Received V=2!
    Engine-->>Broker: ActionHoldGap (HTTP 409 Conflict)
    Note over Engine: Key held; downstream ledger corruption prevented
```

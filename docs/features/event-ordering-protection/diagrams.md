# Event Ordering Protection — System Architecture & Workflow Diagrams

## 1. Context & Architecture Boundary
This document details the visual execution workflows for **Event Ordering Protection (WP-4)**, governing how Kafka partitions, consumer workers, database sequence trackers, and held event stores coordinate to guarantee strict ordering without halting partitions.

---

## 2. Component Relationship Diagram

```mermaid
graph TD
    subgraph Producer Boundary
        Agg[Aggregate Store] -->|Tx Commit| Outbox[Transactional Outbox]
        Outbox -->|Commit Order Poll| Relay[CDC / Relay Poller]
    end

    subgraph Streaming Infrastructure
        Relay -->|Partition by aggregate_id| Kafka[Kafka Partition Log]
    end

    subgraph Consumer Runtime
        Kafka -->|Poll Batch| Worker[Consumer Worker]
        Worker -->|1. SELECT FOR UPDATE| Tracker[(aggregate_sequence_tracker)]
        Worker -->|2. Gap Detected| HeldStore[(sequence_held_event)]
        Worker -->|3. Exact Next Match| Domain[Domain State Mutation]
        Worker -->|4. Integrity Mismatch| Quarantine[Dead Letter Quarantine]
        Worker -->|5. Commit Offset| Kafka
    end

    subgraph Recovery Subsystem
        HeldStore -->|Drain on Predecessor Arrival| Worker
        Quarantine -->|Operator Intervention| Resync[Snapshot Resync / Re-emit]
    end
```

---

## 3. Detailed Sequence Execution Flows

### 3.1 Normal Path: Exact-Next Sequential Processing
When an event arrives matching $E.version == last\_applied + 1$, it is applied directly to the business state.

```mermaid
sequenceDiagram
    autonumber
    actor Kafka as Kafka Partition
    participant Worker as Consumer Worker
    participant DB as aggregate_sequence_tracker
    participant Model as Domain Business Logic

    Kafka->>Worker: Consume Event (V=1, Offset=101)
    Worker->>DB: SELECT FOR UPDATE WHERE aggregate_id = 'ORD-77'
    DB-->>Worker: last_applied_version = 0, is_held = false
    
    rect rgb(235, 255, 235)
        Note over Worker: Exact Next Check: 1 == 0 + 1 (SUCCESS)
        Worker->>Model: Execute State Transition
        Worker->>DB: UPDATE tracker SET last_applied_version = 1, last_payload_hash = 'sha256'
        Worker->>Kafka: Commit Offset 101
    end
```

---

### 3.2 Gap Path: Out-of-Order Delivery & Aggregate Parking
When an event arrives ahead of sequence ($V=3$ when current is $V=1$), the event is durably held and the aggregate is parked. The partition is NOT stopped.

```mermaid
sequenceDiagram
    autonumber
    actor Kafka as Kafka Partition
    participant Worker as Consumer Worker
    participant Tracker as aggregate_sequence_tracker
    participant Held as sequence_held_event

    Kafka->>Worker: Consume Event (V=3, Offset=102)
    Worker->>Tracker: SELECT FOR UPDATE WHERE aggregate_id = 'ORD-77'
    Tracker-->>Worker: last_applied_version = 1, is_held = false
    
    rect rgb(255, 235, 235)
        Note over Worker: Gap Detected: 3 > 1 + 1 (Need V=2)
        Worker->>Held: INSERT sequence_held_event (V=3, payload, offset=102)
        Worker->>Tracker: UPDATE tracker SET is_held = true, hold_reason = 'GAP'
        Worker->>Kafka: Commit Offset 102
        Note over Worker,Kafka: Partition continues consuming next message immediately!
    end
```

---

### 3.3 Recovery Path: Predecessor Arrival & Multi-Version Cascade Drain
When missing version $V=2$ arrives, it applies and triggers a sequential drain of buffered future versions.

```mermaid
sequenceDiagram
    autonumber
    actor Kafka as Kafka Partition
    participant Worker as Consumer Worker
    participant Tracker as aggregate_sequence_tracker
    participant Held as sequence_held_event
    participant Model as Domain Business Logic

    Kafka->>Worker: Consume Delayed Event (V=2, Offset=105)
    Worker->>Tracker: SELECT FOR UPDATE WHERE aggregate_id = 'ORD-77'
    Tracker-->>Worker: last_applied_version = 1, is_held = true
    
    rect rgb(235, 255, 235)
        Note over Worker: Exact Next Match: 2 == 1 + 1 (Apply V=2)
        Worker->>Model: Execute State Transition (V=2)
        Worker->>Tracker: UPDATE tracker SET last_applied_version = 2
        
        Note over Worker: Trigger Cascade Drain Loop
        Worker->>Held: SELECT held events WHERE aggregate_id = 'ORD-77' ORDER BY version ASC
        Held-->>Worker: Returns [V=3, V=4]
        
        loop For Each Consecutive Held Event
            Worker->>Model: Execute State Transition (V=3)
            Worker->>Held: DELETE WHERE id = held_v3.id
            Worker->>Tracker: UPDATE tracker SET last_applied_version = 3
            
            Worker->>Model: Execute State Transition (V=4)
            Worker->>Held: DELETE WHERE id = held_v4.id
            Worker->>Tracker: UPDATE tracker SET last_applied_version = 4
        end
        
        Worker->>Tracker: UPDATE tracker SET is_held = false, hold_reason = NULL
        Worker->>Kafka: Commit Offset 105
    end
```

---

### 3.4 Security & Integrity Path: Content Mismatch on Duplicate
When a redelivered version has a payload digest that differs from the original, execution halts for that key and alerts are triggered.

```mermaid
sequenceDiagram
    autonumber
    actor Kafka as Kafka Partition
    participant Worker as Consumer Worker
    participant Tracker as aggregate_sequence_tracker
    participant DLQ as Quarantine / Security Incident

    Kafka->>Worker: Consume Redelivery Event (V=2, Offset=109)
    Worker->>Tracker: SELECT FOR UPDATE WHERE aggregate_id = 'ORD-77'
    Tracker-->>Worker: last_applied_version = 2, last_payload_hash = 'hash_original'
    
    rect rgb(255, 215, 215)
        Note over Worker: Compute SHA-256 of incoming payload
        Note over Worker: incoming_hash != 'hash_original' (INTEGRITY BREACH)
        Worker->>Tracker: UPDATE tracker SET is_held = true, hold_reason = 'CHECKSUM_MISMATCH'
        Worker->>DLQ: Route event to Quarantine with reason CHECKSUM_MISMATCH
        Worker->>Kafka: Commit Offset 109
        Note over Worker: Security Incident Alert dispatched to on-call
    end
```

---

## 4. Aggregate Ordering State Machine

```mermaid
stateDiagram-v2
    [*] --> ACTIVE: First Event (V=1) or Snapshot Bootstrap

    ACTIVE --> ACTIVE: Exact-Next Event (V = Last + 1)
    ACTIVE --> ACTIVE: Idempotent Duplicate (V <= Last && Hash Matches)
    
    ACTIVE --> HELD: Gap Detected (V > Last + 1)
    ACTIVE --> HELD: Checksum Mismatch (V <= Last && Hash Diverges)
    
    HELD --> HELD: Additional Future Events Arrive (V > Last + 1)
    HELD --> DRAINING: Missing Predecessor Arrives (V == Last + 1)
    
    DRAINING --> HELD: Partial Drain (Next sequential version missing in held store)
    DRAINING --> ACTIVE: Drain Complete (Held buffer empty up to date)
    
    HELD --> RESYNCING: Manual / Automated Snapshot Resync Triggered
    RESYNCING --> ACTIVE: Snapshot Applied at Target Version
```


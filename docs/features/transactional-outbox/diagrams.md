# Transactional Outbox: Architectural Diagrams

## 1. End-to-End Sequence Diagram (Write Path, Poller Relay & Consumer)

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Service as Business Domain Service
    participant DB as PostgreSQL (ACID)
    participant Relay as Outbox Relay Worker (Worker A)
    participant RelayB as Outbox Relay Worker (Worker B)
    participant Broker as Apache Kafka Broker
    participant Consumer as Downstream Consumer

    rect rgb(240, 248, 255)
    Note over Client,DB: Section 1: Ingress Write Path (Atomic Transaction)
    Client->>Service: POST /api/v1/outbox (x-idempotency-key, payload)
    activate Service
    Service->>DB: 1. Claim Idempotency Key (INSERT INTO outbox_idempotency_keys ON CONFLICT)
    alt Idempotency Key Already Claimed
        DB-->>Service: Return existing record_id
        Service-->>Client: 200 OK (Return existing OutboxRecord - no duplicate aggregate)
    else First Command Execution
        Service->>DB: 2. Mutate Aggregate (version = version + 1 under row lock)
        Service->>DB: 3. Insert OutboxRecord (status='PENDING', aggregate_version)
        Service->>DB: 4. COMMIT Transaction
        Service-->>Client: 201 Created (Staged OutboxRecord)
    end
    deactivate Service
    end

    rect rgb(255, 250, 240)
    Note over Relay,Broker: Section 4: Poller Relay Lease & Ownership Fencing
    Relay->>DB: ClaimInFlight(worker_id='worker-A', lease=30s, limit=50)
    DB-->>Relay: Return claimed batch (status='IN_FLIGHT', worker_id='worker-A')
    
    alt Happy Path (Broker Ack before Lease Expiry)
        Relay->>Broker: Publish to topic = aggregate_type (Key = aggregate_id)
        Broker-->>Relay: Broker ACK (all ISR replicas)
        Relay->>DB: MarkPublishedByWorker(id, worker_id='worker-A', lease_expires_at >= NOW())
        DB-->>Relay: Confirmed PUBLISHED
    else Lease Expiry & Race Condition (F4.6 Fencing)
        Note over Relay: Network delay stalls Worker A past 30s lease
        RelayB->>DB: ClaimInFlight(worker_id='worker-B', lease=30s)
        DB-->>RelayB: Reclaims record (worker_id='worker-B')
        Relay->>DB: Worker A attempts MarkPublishedByWorker('worker-A')
        DB--xRelay: 412 Precondition Failed / Worker Ownership Lost (Fenced Out)
        RelayB->>Broker: Worker B Publishes (INV-02 At-Least-Once)
        RelayB->>DB: Confirmed PUBLISHED by Worker B
    else Poison Pill Failure (F4.7 Aggregate Parking)
        Relay->>Broker: Publish Attempt
        Broker--xRelay: Permanent Broker Rejection (Bad Header / Payload)
        Relay->>DB: MarkFailed(retry_count = retry_count + 1)
        Note over DB: On retry_count >= 5 -> status='PARKED'
        Note over DB: Aggregate k+1 queries suppressed by NOT EXISTS (PARKED)
    end
    end

    rect rgb(245, 255, 245)
    Note over Broker,Consumer: Section 6: Consumer Tier & Idempotent Processing
    Broker->>Consumer: Deliver Event (aggregate_id, aggregate_version, event_id)
    activate Consumer
    alt Incoming version <= last_applied
        Note over Consumer: Duplicate detected -> Acknowledge and skip
    else Incoming version == last_applied + 1
        Note over Consumer: Apply state mutation and record event_id in consumer DB tx
    else Incoming version > last_applied + 1
        Note over Consumer: Gap detected (upstream loss) -> Hold key, do not apply, page on-call
    end
    deactivate Consumer
    end
```

## 2. Outbox Record State Machine Diagram

```mermaid
stateDiagram-v2
    [*] --> PENDING: StageEvent (Normal Poller Path)
    [*] --> DELETED: StageEvent (CDC Mode Step 5)
    
    PENDING --> IN_FLIGHT: ClaimInFlight (Worker ID + Lease Assigned)
    
    IN_FLIGHT --> PUBLISHED: Broker ACK + Active Lease Verified
    IN_FLIGHT --> FAILED: Broker Publish Error
    IN_FLIGHT --> PENDING: Lease Expired Without Broker Ack
    
    FAILED --> PENDING: Retry with Backoff (Retry Count < 5)
    FAILED --> PARKED: Retry Count >= 5 (Poison Event Isolated)
    
    PARKED --> PENDING: Manual Triage / Unpark (R3 Runbook)
    
    PUBLISHED --> PURGED: PurgeExpired (Age > 48h Retention)
    PURGED --> [*]
    DELETED --> [*]
```

## 3. Failure Mode Decision & Mitigation Tree

```mermaid
flowchart TD
    Start[Event Staging or Relay Request] --> C1{Is Payload > 1MB?}
    C1 -- Yes --> E1[Reject: 400 Size Limit / Require Claim-Check F1.7]
    C1 -- No --> C2{Contains Float in JSON?}
    C2 -- Yes --> E2[Reject: 400 Validation Error / Financial Float Forbidden F1.9]
    C2 -- No --> C3{Idempotency Key Claimed?}
    C3 -- Collision --> E3[Return Staged Record / Idempotent No-Op F1.2]
    C3 -- New --> StepAtomic[Insert in Single DB Transaction INV-01]
    
    StepAtomic --> Mode{Mode Selected}
    Mode -- CDC Path --> StepCDC[Insert & Immediate Delete in Same Tx Step 5]
    StepCDC --> WAL[Logical Decoding via Debezium]
    
    Mode -- Poller Path --> StepPending[Save as PENDING with aggregate_version]
    StepPending --> Relay[Poller Worker Claims Lease IN_FLIGHT F4.0]
    
    Relay --> Pub{Broker Publish}
    Pub -- Success --> FencedConfirm{Worker Still Owns Lease?}
    FencedConfirm -- Yes --> SetPublished[Mark PUBLISHED F4.0]
    FencedConfirm -- No --> FencedDrop[Drop / Reclaimed by Other Worker F4.6]
    
    Pub -- Fail --> IncRetry[Increment Retry Count]
    IncRetry --> C4{Retry Count >= 5?}
    C4 -- No --> SetFailed[Status PENDING for Backoff]
    C4 -- Yes --> SetParked[Status PARKED / Block Key k+1 F4.7]
```


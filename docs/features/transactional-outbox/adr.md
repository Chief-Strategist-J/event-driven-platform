# ADR: Transactional Outbox Pattern for Guaranteed At-Least-Once Event Publishing

* **Status:** Accepted
* **Date:** 2026-11-04
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `src/features/transactional-outbox/`, `src/api/`, `messaging/engine/`

---

## 1. Context and Problem Statement

When domain entities are modified in a relational database and corresponding events must be emitted to a message broker (e.g. Apache Kafka), performing a direct "dual-write" (writing to SQL, then calling `producer.Publish()`) guarantees data corruption:
- If the database transaction commits but the network call to the broker fails or times out, the event is permanently lost, causing downstream projections and sagas to become inconsistent.
- If the event is published first and the database transaction fails (e.g. constraint violation or crash), downstream consumers act on phantom events that never existed in the database.

Distributed transactions (2PC / XA) across Kafka and PostgreSQL are not supported, scale poorly, and introduce latency and operational fragility.

---

## 2. Decision Drivers & Invariants (from business-logic.md v3)

* **INV-01 Atomicity**: Business mutation and outbox insert share ONE connection, ONE transaction, ONE database.
* **INV-02 Delivery Semantics**: At-least-once end to end; exactly-once exists only as an idempotent consumer effect.
* **INV-03 Per-Aggregate Ordering**:
  - Message key = `aggregate_id`.
  - All event types of one aggregate go to ONE topic named after `aggregate_type`.
  - Partition count never changes.
  - Relay preserves commit order per key and never sends $k+1$ before $k$ is acked.
* **INV-04 Version Monotonicity**: Each event carries `aggregate_version = previous + 1`, enforced via unique constraint `(tenant_id, aggregate_type, aggregate_id, aggregate_version)`.
* **INV-05 Event Identity**: `event_id` generated once via RFC 9562 UUIDv7, immutable across retries and replays.
* **INV-06 Single Writer**: `source_service` header guarantees a single service writes an aggregate type.
* **INV-09 Business Idempotency**: Step 1 claims `x-idempotency-key` in `outbox_idempotency_keys` table; retry with identical key is a no-op.
* **INV-12 Payload Fidelity**: Financial numbers strictly forbidden as float; integer minor units or decimal strings only. Payloads > 1MB require claim-check.

---

## 3. End-to-End Architectural Diagrams

### 3.1 Write Path & Poller Relay with Fencing Sequence

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

---

### 3.2 Outbox Record State Machine Diagram

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

---

### 3.3 Failure Modes and Handling Matrix

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

---

## 4. Comprehensive Failure Mode Engineering Reference

| Failure Code | Scenario | Impact | Code Handling & Architectural Guardrail | Detection |
|---|---|---|---|---|
| **F1.1** | Dual-write attempt | Event Loss / Ghost State | Zero Kafka clients imported in domain feature; all writes go to `outbox` | CI dependency scanner |
| **F1.2** | Ambiguous commit / network drop | Duplicate aggregate & event | Step 1 claims `x-idempotency-key` in `outbox_idempotency_keys` table; retry returns existing record | Unique violation counter |
| **F1.5** | Concurrent mutations on same aggregate | Race condition / version collision | `UNIQUE(tenant_id, aggregate_type, aggregate_id, aggregate_version)` constraint | SQL Unique Violation (mapped to `ConflictError`) |
| **F1.7** | Oversized payload | Broker partition block / poison pill | Write-time rejection of payloads > 1MB (`IsPayloadSizeValid`); requires claim-check | Payload size histogram |
| **F1.9** | Floating-point numbers in payload | Financial precision loss & cents drift | `HasFloatingPointNumbers` scans payload and rejects floats (`.`, `e`, `E`) | Validation error counter |
| **F2.1** | Kafka down / Relay down | PostgreSQL WAL bloat | Outbox rows accumulate safely as `PENDING`; backpressure monitored | Slot lag bytes & WAL disk alerts |
| **F4.0** | Row locks held during network send | Lock pileup & DB outage | Lease model: records claimed as `IN_FLIGHT` with worker ID and lease expiry | Oldest transaction age |
| **F4.2** | Concurrent workers reorder versions | Downstream corruption | `ORDER BY aggregate_version ASC` + partition key affinity on `aggregate_id` | Consumer gap detector |
| **F4.6** | Worker lease expires during dispatch | Duplicates & race collision | `MarkPublishedByWorker` fences confirm step on worker ownership & active lease | Reclaim counter |
| **F4.7** | Poison event payload | Outage on partition | Transitions to `PARKED` after 5 retries; query suppresses versions $> k$ | Alert when `count(status='PARKED') > 0` |
| **F9.1** | Silent flow stoppage | Undetected outage | Synthetic canary heartbeat emitted every ~30s (`EmitCanaryHeartbeat`) | Canary consumer latency |

---

## 5. Consequences & Tradeoffs

### Positive
* Complete parity with `business-logic.md` v3 production decision tree.
* Worker lease claiming eliminates long database transactions while preventing worker race conditions via fencing.
* Financial precision and idempotency guarantees protect downstream ledgers from duplicate or rounded financial data.
* Poison-pill isolation operates per-aggregate, ensuring a bad record halts only that entity without impacting other tenants or aggregates.

### Negative / Tradeoffs
* Parking an aggregate blocks all subsequent versions for that specific entity until an engineer triages the payload (Section 11 R3 runbook).
* Lease model requires periodic clock synchronization across poller worker nodes to maintain lease accuracy.

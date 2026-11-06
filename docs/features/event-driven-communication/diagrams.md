# Event-Driven Communication — Architecture & Interaction Diagrams

This document contains Mermaid diagrams illustrating inter-service communication selection, protocol translation, saga choreography/orchestration, and consumer resilience pipelines.

---

## 1. Pattern Selection: Synchronous Read vs Asynchronous Mutation

```mermaid
flowchart TD
    REQ["Inbound Client Request / Inter-Service Call"] --> CHECK{"What is the interaction type?"}
    
    CHECK -- Synchronous Query --> RPC["ConnectRPC over HTTP/2"]
    subgraph SyncTier ["Synchronous Read Tier"]
        RPC --> CB["Circuit Breaker (Trip on 5 timeouts)"]
        CB --> READ_DB[("Read Replica / Materialized View")]
        READ_DB --> RESP["Immediate Strong Consistency Response"]
    end
    
    CHECK -- State Mutation --> OUTBOX["Transactional Outbox (ACID)"]
    subgraph AsyncTier ["Asynchronous Fact Tier"]
        OUTBOX --> DB_TX[("Postgres Primary (Commit)")]
        DB_TX --> RELAY["Outbox Relay (acks=all, idempotent)"]
        RELAY --> KAFKA["Apache Kafka Aggregate Topics"]
        KAFKA --> CONSUMER["Downstream Consumers (Eventual Consistency)"]
    end
    
    CHECK -- Multi-Step Flow --> SAGA["Saga Coordinator"]
```

---

## 2. Distributed Saga Orchestration & Reverse Compensation Flow

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Saga as OrderSagaCoordinator
    participant Order as OrderService
    participant Payment as PaymentService
    participant Inventory as InventoryService
    participant Outbox as Transactional Outbox

    Client->>Saga: Submit PlaceOrder Command
    activate Saga
    Saga->>Order: Create Pending Order (v=1)
    Order-->>Saga: OrderCreated (Pending)
    
    Saga->>Payment: Authorize Payment ($100)
    activate Payment
    alt Payment Succeeded
        Payment-->>Saga: PaymentAuthorized
        deactivate Payment
        
        Saga->>Inventory: Allocate Inventory (SKU-123)
        activate Inventory
        alt Inventory Out of Stock
            Inventory-->>Saga: InventoryAllocationFailed
            deactivate Inventory
            
            note over Saga,Payment: Trigger Reverse Compensation
            Saga->>Payment: Refund / Cancel Authorization
            Payment-->>Saga: PaymentRefunded
            
            Saga->>Order: Mark Order Cancelled
            Order-->>Saga: OrderCancelled
            Saga-->>Client: 422 Unprocessable (Out of Stock, Payment Refunded)
        else Inventory Succeeded
            Inventory-->>Saga: InventoryAllocated
            Saga->>Order: Finalize Order (Settled)
            Order-->>Saga: OrderSettled
            Saga-->>Client: 200 OK (Order Completed)
        end
    else Payment Declined
        Payment-->>Saga: PaymentDeclined
        Saga->>Order: Mark Order Cancelled (Payment Failed)
        Saga-->>Client: 402 Payment Required
    end
    deactivate Saga
```

---

## 3. Protocol Ingress Translation & Outbox Relay Flow

```mermaid
sequenceDiagram
    autonumber
    actor External as Mobile / Web Client (HTTP/REST)
    participant Gateway as Ingress API Gateway
    participant Handler as Service Command Handler
    participant DB as Postgres (State + Outbox)
    participant Relay as Outbox Debezium / Poller Daemon
    participant Kafka as Apache Kafka Broker

    External->>Gateway: POST /api/v1/orders (W3C traceparent injected)
    activate Gateway
    Gateway->>Handler: Forward Request with Claims & Trace
    activate Handler
    
    Handler->>DB: BEGIN TRANSACTION
    Handler->>DB: INSERT INTO orders VALUES (...)
    Handler->>DB: INSERT INTO transactional_outbox (event_id, payload, headers)
    Handler->>DB: COMMIT TRANSACTION
    
    Handler-->>Gateway: 202 Accepted (Order ID, Correlation ID)
    Gateway-->>External: 202 Accepted (Processing Asynchronously)
    deactivate Handler
    deactivate Gateway

    loop Continuous Polling / CDC WAL Tail
        Relay->>DB: SELECT * FROM transactional_outbox WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED
        Relay->>Kafka: ProduceRecord(Topic, Key, EncryptedPayload, Headers)
        Kafka-->>Relay: Broker ACK (Leader & ISR recorded)
        Relay->>DB: UPDATE transactional_outbox SET status = 'PUBLISHED'
    end
```

---

## 4. Asynchronous Consumer Pipeline & Error Triage

```mermaid
flowchart TD
    MSG["Kafka Broker Record Arrives"] --> EXTRACT["Extract W3C Trace Context from Record Headers"]
    EXTRACT --> IDEMP{"Idempotency Barrier Check<br/>(Query processed_events_ledger)"}
    
    IDEMP -- Already Processed --> ACK_DUP["Ack Offset Immediately (Skip Execution)"]
    IDEMP -- New Message --> PROCESS["Execute Domain Business Handler"]
    
    PROCESS --> RESULT{"Processing Outcome"}
    RESULT -- Success --> COMMIT["Commit Kafka Offset Manually"]
    
    RESULT -- Transient Error (DB blip, timeout) --> RETRY["Pause Partition Consumption<br/>Exponential Backoff + Jitter"]
    RETRY --> PROCESS
    
    RESULT -- Poison Pill / Validation Failure --> DLQ["Route to Dead-Letter Queue (DLQ)<br/>With Stacktrace & Error Headers"]
    DLQ --> COMMIT
```

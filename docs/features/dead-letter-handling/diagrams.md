# Dead-Letter Handling Architectural Diagrams

---

## 1. Quarantine & Key Parking Lifecycle

```mermaid
flowchart TD
    subgraph FAIL["1. Consumer Processing Failure"]
        E["Consumer Handler Exception"] --> CLS{"Classify Failure Code (DL-08)"}
        CLS -- "System / Infrastructure Outage (TIMEOUT)" --> CB["Trigger Circuit Breaker<br/>Pause Partition Consumption<br/>DO NOT Quarantine!"]
        CLS -- "Message Defect (Schema, Bug, Logic)" --> ENV["Construct Lossless Quarantine Envelope (DL-04)"]
    end

    subgraph QUAR["2. Durable Quarantine & Key Parking (DL-02, DL-05)"]
        ENV --> W1["INSERT INTO dead_letter_quarantine<br/>(status = 'QUARANTINED', redrive_count = 0)"]
        W1 --> W2["UPSERT INTO aggregate_parking_store<br/>(tenant_id, handler_id, aggregate_id, is_parked = TRUE)"]
        W2 --> ACK["Advance Consumer Group Offset"]
    end

    subgraph TRIAGE["3. Inspection, Redrive & Disposition (DL-01, DL-07, DL-11)"]
        W2 --> BLK["Subsequent Versions for Key Blocked at Ingress"]
        UI["Operator / Runbook Tool"] --> RD{"Select Action"}
        RD -- "Redrive (attempts < 3)" --> R1["Invoke Consumer with Original Payload (DL-06, DL-07)"]
        R1 -- "Success" --> U1["Update status = 'REDRIVEN_SUCCESS'<br/>UNPARK Aggregate Key"]
        R1 -- "Failure" --> U2["Increment redrive_count<br/>Keep Key Parked"]
        RD -- "Discard" --> D1["Audit Discard Reason<br/>Update status = 'DISCARDED'<br/>UNPARK Aggregate Key"]
    end
```

---

## 2. Invariant DL-02: Key Parking Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Consumer as Consumer Ingress
    participant DLQ as Dead Letter Service
    participant DB as PostgreSQL (aggregate_parking_store)

    Note over Consumer: Version V=2 fails schema validation
    Consumer->>DLQ: QuarantineEvent(V=2)
    DLQ->>DB: ParkKey(aggregate_id="ord-100", is_parked=TRUE)
    DB-->>Consumer: Key Parked Durably
    Consumer->>Consumer: Advance offset past V=2

    Note over Consumer: Version V=3 arrives on main topic
    Consumer->>DLQ: IsKeyParked("ord-100")
    DLQ->>DB: Check is_parked
    DB-->>Consumer: TRUE
    Note over Consumer: Suppress execution (ActionParkPoison)<br/>Ordering invariant DL-02 preserved!
```

---

## 3. Invariant DL-11: Bounded Redrive State Transitions

```mermaid
stateDiagram-v2
    [*] --> QUARANTINED
    QUARANTINED --> REDRIVING : Redrive Triggered (attempts < 3)
    REDRIVING --> REDRIVEN_SUCCESS : Consumer Returns Success
    REDRIVING --> QUARANTINED : Consumer Returns Failure (attempts < 3)
    REDRIVING --> QUARANTINED : Failure (attempts == 3, Locked for Human Review)
    QUARANTINED --> DISCARDED : Human Operator Authorized Abandonment
    QUARANTINED --> COMPENSATED : Human Operator Compensating Transaction
    REDRIVEN_SUCCESS --> [*]
    DISCARDED --> [*]
    COMPENSATED --> [*]
```


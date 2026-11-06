# Schema Evolution Architecture & Runtime Diagrams (WP-16)

This document contains Mermaid diagrams illustrating the runtime architectures, compatibility verification pipelines, recursive upcaster state machines, and dual-publishing lifecycle horizons of the Schema Evolution subsystem per the [Schema Evolution Business Logic Specification](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/schema-evolution/business-logic.md).

---

## 1. Schema Change Validation & CI Linter Gate

```mermaid
flowchart TD
    subgraph Authoring ["Schema Modification Proposed"]
        PR["Developer Pull Request<br/>(Modifies Protobuf / Avro / JSON Schema)"]
    end

    subgraph CI ["CI Compatibility Verification Gate"]
        FETCH["Fetch Current Production Schema<br/>(Apicurio / Schema Registry)"]
        LINT{"Run Compatibility Linter<br/>(Evaluates BACKWARD / FULL Rules)"}
        BREAK{"Is Breaking Change?"}
    end

    subgraph Outcomes ["Resolution Paths"]
        REJECT["Reject PR & Block Merge<br/>(Flags Incompatible Type/Field Removal)"]
        FORK["Require Topic Fork (.v2)<br/>& Register Upcaster Function"]
        PASS["Approve Schema Registration<br/>(Additive Evolution Permitted)"]
    end

    PR --> FETCH
    FETCH --> LINT
    LINT -->|Violates Compatibility| BREAK
    BREAK -->|Cannot Be Upcast| REJECT
    BREAK -->|Eligible for Dual-Publish| FORK
    LINT -->|Compatible| PASS
```

---

## 2. In-Memory Recursive Upcaster Replay Pipeline (SE-01, SE-02)

```mermaid
flowchart LR
    subgraph Storage ["Physical Immutable Storage"]
        LOG[("Raw Event Stream Log<br/>(Legacy Event v1 Payload)")]
    end

    subgraph Runtime ["Consumer Replay Runtime Engine"]
        INSP["Inspect Event Header<br/>(schema_version = 1)"]
        DAG{"Upcaster DAG Resolver<br/>(Finds Path: v1 -> v3)"}
        U1["UpcastV1ToV2(payload)<br/>(Pure Function: Splits Name)"]
        U2["UpcastV2ToV3(payload)<br/>(Pure Function: Currency to Cents)"]
    end

    subgraph Delivery ["Modern Application Layer"]
        ENV["Current Version Event Object (v3)<br/>(Strictly Typed Go Struct)"]
        PRJ["Materialized View Projection<br/>(Processes State Mutation)"]
    end

    LOG --> INSP
    INSP --> DAG
    DAG --> U1
    U1 --> U2
    U2 --> ENV
    ENV --> PRJ
```

---

## 3. Dual-Publishing & Consumer Migration Timeline (SE-05, SE-06)

```mermaid
sequenceDiagram
    autonumber
    participant Producer as Event Producer Service
    participant Outbox as Transactional Outbox
    participant T1 as Kafka Topic: order.v1
    participant T2 as Kafka Topic: order.v2
    participant C_Old as Legacy Consumer (Reads v1)
    participant C_New as Migrated Consumer (Reads v2)

    Note over Producer,C_New: Phase 1: Dual-Publishing Window (Day 1 - 30)
    Producer->>Outbox: Atomic Insert: [Event_v1, Event_v2] in same DB transaction
    Outbox->>T1: Publish v1 Event Payload
    Outbox->>T2: Publish v2 Event Payload
    T1-->>C_Old: Legacy Consumer processes v1
    T2-->>C_New: Modern Consumer processes v2

    Note over Producer,C_New: Phase 2: Consumer Migration Complete (Day 25)
    C_Old->>C_Old: All consumer groups upgraded to read T2 (order.v2)
    Note over T1: Zero Consumer Lag for 7 Consecutive Days

    Note over Producer,C_New: Phase 3: Decommissioning v1 Topic (Day 31)
    Producer->>Producer: Terminate v1 publishing; publish exclusively to T2
    T1->>T1: Archive topic partitions to cold S3/MinIO storage
```

---

## 4. Tolerant Reader Pattern & Quarantine Quarantine (SE-03, SE-08)

```mermaid
flowchart TD
    MSG["Incoming Raw Event Stream Message"]
    PARSE{"Tolerant Reader JSON / Avro Parser"}
    VAL{"Domain Invariants Valid?"}

    MSG --> PARSE
    PARSE -->|Malformed Byte Payload / Corrupt Header| DLQ["Quarantine to Dead-Letter Queue<br/>(Retains Raw Bytes + Correlation ID)"]
    PARSE -->|Unknown New Fields Present| IGNORE["Ignore Unrecognized Attributes<br/>(Tolerant Reader Principle 2)"]
    IGNORE --> VAL
    PARSE -->|Known Schema| VAL
    VAL -->|Missing Required Domain Fields| DLQ
    VAL -->|Valid| APPLY["Apply to Aggregate State / View Projection"]
    DLQ --> ALERT["Emit Metric & Page Owning Team"]
```


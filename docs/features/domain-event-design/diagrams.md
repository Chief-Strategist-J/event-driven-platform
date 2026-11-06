# Domain Event Design — Architecture & Lifecycle Diagrams

This document contains Mermaid architecture diagrams illustrating the domain event lifecycle, envelope validation, causality tracking, and aggregate version reservation.

---

## 1. Domain Event Lifecycle & In-Transaction Validation Flow

```mermaid
sequenceDiagram
    autonumber
    actor Client as External Client / Trigger
    participant CmdHandler as Command Handler (C-DOM)
    participant CEnv as Envelope Builder (C-ENV)
    participant AggLedger as Aggregate Root Ledger
    participant Outbox as Transactional Outbox (C-OBX)
    participant DB as PostgreSQL (WAL)

    Client->>CmdHandler: Submit Command (e.g. PlaceOrder)
    activate CmdHandler
    CmdHandler->>CmdHandler: Execute business invariants & aggregate decision
    
    CmdHandler->>AggLedger: Reserve next aggregate version (v -> v+1)
    alt Aggregate Closed / Terminal
        AggLedger-->>CmdHandler: Error: AGGREGATE_TERMINAL (422)
        CmdHandler-->>Client: Rejection: Aggregate is closed
    else OCC Conflict (expected != current)
        AggLedger-->>CmdHandler: Error: OCC_CONFLICT (409)
        CmdHandler-->>Client: Rejection: Concurrent modification retry required
    else Version Reserved Successfully
        AggLedger-->>CmdHandler: Reserved Version = v+1
        
        CmdHandler->>CEnv: Build Canonical Envelope (payload, event_type, causality)
        activate CEnv
        CEnv->>CEnv: Verify UUIDv7 event_id & RFC 3339 effective_at / recorded_at
        CEnv->>CEnv: Verify past-tense event_type against Catalog
        CEnv->>CEnv: Check causation_depth <= 16 (Loop Prevention)
        CEnv-->>CmdHandler: Canonical Envelope [Validated]
        deactivate CEnv
        
        CmdHandler->>DB: Begin ACID Transaction
        CmdHandler->>DB: Mutate Aggregate State
        CmdHandler->>Outbox: Insert Canonical Event into Outbox
        CmdHandler->>AggLedger: Commit aggregate current_version = v+1
        CmdHandler->>DB: Commit Transaction
        
        CmdHandler-->>Client: 200 OK (Command Accepted)
    end
    deactivate CmdHandler
```

---

## 2. Multi-Aggregate Transaction Group Barrier

```mermaid
flowchart TD
    subgraph ProducerTx ["Producer Atomic Transaction"]
        CMD["Command Execution"] --> OUTBOX["Outbox Insertion"]
        OUTBOX --> EVT1["Aggregate A Event<br/>(tx_group_id=X, size=2)"]
        OUTBOX --> EVT2["Aggregate B Event<br/>(tx_group_id=X, size=2)"]
    end

    subgraph KafkaBrokers ["Kafka Distributed Topics"]
        EVT1 --> T1["Topic: aggregate.a.events"]
        EVT2 --> T2["Topic: aggregate.b.events"]
    end

    subgraph ConsumerBarrier ["Consumer Completeness Barrier"]
        T1 --> C1["Consumer A"]
        T2 --> C2["Consumer B"]
        C1 --> BARRIER[("transaction_group_ledger<br/>(received_count / expected_group_size)")]
        C2 --> BARRIER
        
        BARRIER --> CHECK{"received_count ==<br/>expected_group_size?"}
        CHECK -- No --> WAIT["Wait / Hold Processing (Bounded Timeout)"]
        CHECK -- Yes --> EXECUTE["Trigger Downstream Projection / Reaction"]
    end
```

---

## 3. Causal Chain Verification & Loop Detection

```mermaid
flowchart LR
    E1["Root Command<br/>depth = 0"] --> E2["Domain Event A<br/>depth = 1"]
    E2 --> E3["Domain Event B<br/>depth = 2"]
    E3 --> E4["Reaction Event C<br/>depth = 3"]
    
    subgraph LoopCheck ["C-ENV Causal Verifier"]
        E4 --> DEPTH{"depth <= 16?"}
        DEPTH -- No --> ABORT1["FATAL: Causal Depth Exceeded"]
        DEPTH -- Yes --> HASH{"(aggregate, event_type)<br/>in causation path?"}
        HASH -- Yes --> ABORT2["FATAL: Causal Loop Detected"]
        HASH -- No --> PASS["Pass: Emit Event"]
    end
```

---

## 4. Aggregate Root Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> Initial: First Event (v=1)
    Initial --> Active: Aggregate Root Initialized
    
    Active --> Active: Command Mutates State (v -> v+1)
    Active --> Active: Multiple Events in Tx (v -> v+K)
    
    Active --> Terminal: Terminal Event Emitted (e.g. AccountClosed)
    Terminal --> [*]: Immutable Locked (v > v_terminal BANNED)
    
    note right of Terminal
        Any subsequent mutation returns
        422 Unprocessable Entity
        Aggregate is permanently sealed
    end note
```


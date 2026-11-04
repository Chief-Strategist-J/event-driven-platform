# Event Versioning — System Architecture & Workflow Diagrams

## 1. High-Level Architectural Context & Component Map

```mermaid
flowchart TD
    subgraph Governance ["C-GOV: CI/CD Schema Governance Gate"]
        LINT["G1: Schema Lint & Semantic Rules"]
        COMPAT["G2: Transitive Compatibility Check (FULL/BACKWARD)"]
        UPC_CHK["G4: Upcaster Chain Completeness Gate"]
        GOLDEN["G5: Golden Determinism Tests"]
        PROMO["G8: Registry Promotion Gate"]
        LINT --> COMPAT --> UPC_CHK --> GOLDEN --> PROMO
    end

    subgraph Registry ["C-REG: Schema Registry (Apicurio / PG Store)"]
        REG_STORE[("event_schema_version<br/>• schema_id<br/>• payload_hash<br/>• compatibility_mode")]
        UPC_STORE[("event_upcaster_registry<br/>• from_version<br/>• to_version<br/>• transformer_name")]
    end

    PROMO -->|Registers Artifact| REG_STORE
    PROMO -->|Registers Step Transform| UPC_STORE

    subgraph Producer ["C-SER: Producer Application Context"]
        APP_CMD["Business Command Handler"]
        OUTBOX_WRITER["Outbox Writer (C-OBX)<br/>Embeds Schema ID & Hash"]
        APP_CMD --> OUTBOX_WRITER
    end

    REG_STORE -.->|Build-time Pinned Schema ID| OUTBOX_WRITER

    subgraph Kafka ["Stream Broker (EV-01)"]
        TOPIC["orders.order.v1<br/>Partitioned by aggregate_id"]
    end

    OUTBOX_WRITER -->|Raw Wire Bytes| TOPIC

    subgraph Consumer ["C-DES & C-UPC: Consumer Runtime Pipeline"]
        C_RAW["1. Fetch Raw Bytes (Kafka Client)"]
        C_RESOLVE["2. Resolve Writer Schema ID (Registry Cache)"]
        C_UPC["3. Pure Upcaster Chain Execution (V1 -> V3)"]
        C_IDEMP["4. Idempotent Consumer & Version Guard"]
        C_EXEC["5. Business State Mutation & Commit"]
        C_DLQ["Quarantine Store (DLQ C-QS)"]

        C_RAW --> C_RESOLVE
        C_RESOLVE -->|Schema Resolved| C_UPC
        C_RESOLVE -->|Unknown Schema / Deserialization Failure| C_DLQ
        C_UPC -->|Upcast Success| C_IDEMP
        C_UPC -->|Missing Chain / Broken Transform| C_DLQ
        C_IDEMP --> C_EXEC
    end

    TOPIC --> C_RAW
    UPC_STORE -.->|Chain Steps| C_UPC
```

---

## 2. Invariant Enforcement Sequence Diagrams

### 2.1 EV-04: Transitive Compatibility CI Gate (`FULL_TRANSITIVE`)

```mermaid
sequenceDiagram
    autonumber
    actor Dev as Producer Engineer
    participant CI as C-GOV (Governance Pipeline)
    participant Reg as C-REG (Registry Service)
    participant Git as Repository Gate

    Dev->>CI: Submit PR with OrderPlaced V2 schema
    CI->>CI: G1: Lint rules (no bare floats, required units, non-empty names)
    CI->>Reg: G2: Fetch all historical lineage versions (V1)
    Reg-->>CI: Return V1 schema definition
    CI->>CI: Evaluate Full Transitivity (EV-04)
    alt New required field without default added
        CI-->>Dev: Incompatible schema modification
        Dev->>CI: Push fix with optional field
    end
    CI->>CI: G4: Verify Upcaster chain exists for OrderPlaced (V1 -> V2)
    CI->>CI: G5: Run golden property test with simulated historical payloads
    CI->>Reg: G8: Promote and lock schema (assigns permanent schema_id)
    CI-->>Git: Status check GREEN: Merge approved
```

---

### 2.2 EV-06: Deterministic In-Memory Upcasting Pipeline ($V_1 \to V_3$)

```mermaid
sequenceDiagram
    autonumber
    participant Broker as Kafka (Topic: orders.order.v1)
    participant RT as Consumer Runtime (C-RT)
    participant Cache as Schema & Upcaster Cache
    participant Engine as Upcaster Engine (C-UPC)
    participant Handler as Business Handler (Reads V3)

    Broker->>RT: Ingest Raw Record (Event: OrderPlaced, Writer Schema: V1)
    RT->>Cache: Lookup Upcaster Chain for (OrderPlaced, From: 1, To: 3)
    Cache-->>RT: Chain: [order_v1_to_v2, order_v2_to_v3]
    RT->>Engine: ExecuteChain(payload_v1, Chain)
    Note over Engine: Step 1 (V1 -> V2): Pure function converts 'price' float to integer 'amount_cents' + 'currency'
    Engine->>Engine: Transform V1 -> V2
    Note over Engine: Step 2 (V2 -> V3): Pure function adds explicit 'status' enum default without inventing facts
    Engine->>Engine: Transform V2 -> V3
    Engine-->>RT: Validated Logical V3 Payload
    RT->>Handler: Dispatch V3 Event into Handler
    Handler->>Handler: Check aggregate version guard & apply state mutation
    Handler-->>RT: Success
```

---

## 3. Major Stream Migration & Zero-Downtime Cutover (EV-09 / §11)

```mermaid
sequenceDiagram
    autonumber
    participant Prod as Producer App
    participant DB as Producer PostgreSQL (Outbox)
    participant V1 as Topic orders.order.v1
    participant V2 as Topic orders.order.v2 (Re-modeled)
    participant ConsV1 as Consumer V1 (Legacy)
    participant ConsV2 as Consumer V2 (Shadow / New)

    Note over Prod,DB: §11.2 Dual-Publishing Phase (Atomic Single Tx)
    Prod->>DB: INSERT Outbox Row V1 + INSERT Outbox Row V2 (Same aggregate version)
    DB-->>V1: Relay emits to orders.order.v1
    DB-->>V2: Relay emits to orders.order.v2

    Note over ConsV2: §11.3 Shadow Parity Validation
    V2->>ConsV2: Consume and compute shadow projection
    ConsV2->>ConsV2: Verify state parity against ConsV1 live projections (100% match)

    Note over ConsV1,ConsV2: Cutover Gate: Switch Consumer Groups
    ConsV1->>ConsV1: Stop consuming V1 stream
    ConsV2->>ConsV2: Promote to primary reader; advance offsets based on continuous aggregate_version (EV-09)

    Note over Prod,DB: §11.4 Contraction Phase
    Prod->>DB: Contract outbox writes: Stop emitting V1 rows
```


# CDC Integration — Architectural Diagrams

This document contains Mermaid diagrams illustrating the runtime components, state machines, and protocols governing Change Data Capture (CDC) integration.

---

## 1. High-Level Component Topology

```mermaid
flowchart TD
    subgraph StorageLayer ["PostgreSQL Primary (C-PG)"]
        PUB["Publication: cdc_pub (C-PUB)"]
        SLOT["Replication Slot: cdc_slot_srv (C-SLT)"]
        OBX["Transactional Outbox Table"]
        SIG["Signaling Table: cdc_signal_log (C-SIG)"]
        HB["Heartbeat Table: cdc_heartbeat_log (C-HB)"]
        AUD["Durable Outbox Audit: cdc_audit (C-AUD)"]
    end

    subgraph ConnectLayer ["Kafka Connect Cluster (C-KC)"]
        DBZ["Debezium PostgreSQL Connector (C-DBZ)"]
        ROUTER["Event Router SMT (by aggregate_id)"]
        CNV["Avro / Protobuf Converters (C-CNV)"]
        TASK["Single Worker Task"]
    end

    subgraph KafkaLayer ["Kafka Cluster (C-TGT)"]
        DOM_TOPIC["Domain Event Topics (by Aggregate)"]
        SIG_TOPIC["Signal / Status Topics"]
        HB_TOPIC["Heartbeat Topic"]
    end

    subgraph ControlPlane ["CDC Control & Monitoring (C-CTL / C-MON)"]
        API["CDC REST Control API"]
        CB["WAL Circuit Breaker Engine"]
        CANARY["Canary Synthetic Prober"]
    end

    OBX --> PUB
    SIG --> PUB
    HB --> PUB
    PUB --> SLOT
    SLOT --> DBZ
    DBZ --> TASK
    TASK --> ROUTER
    ROUTER --> CNV
    CNV --> DOM_TOPIC
    TASK --> SIG_TOPIC
    TASK --> HB_TOPIC

    API -->|Register / Pause / Resume| DBZ
    API -->|Insert Control Signal| SIG
    API -->|Emit Periodic Tick| HB
    CB -->|Polls Slot Lag & Disk Headroom| SLOT
    CB -->|Trips Emergency Pause| API
    CANARY -->|Injects Synthetic Event| OBX
    CANARY -->|Reconciles Latency SLA| DOM_TOPIC
```

---

## 2. Connector Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> UNREGISTERED

    UNREGISTERED --> PROVISIONING : RegisterConnector
    PROVISIONING --> RUNNING : DebeziumTaskStarted
    PROVISIONING --> FAILED : SlotOrPublicationError

    RUNNING --> PAUSED : ManualPause / CircuitBreakerTrip
    PAUSED --> RUNNING : ResumeConnector

    RUNNING --> FAILED : TaskCrash / SerializationError
    PAUSED --> FAILED : FatalStorageError

    FAILED --> RUNNING : RetryRecovery
    FAILED --> DECOMMISSIONED : TerminateConnector
    PAUSED --> DECOMMISSIONED : TerminateConnector
    RUNNING --> DECOMMISSIONED : TerminateConnector

    DECOMMISSIONED --> [*] : SlotDropped
```

---

## 3. Incremental Snapshot Signaling Protocol (C-SIG)

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Operator / Service
    participant API as CDC Control API
    participant DB as PostgreSQL (cdc_signal_log)
    participant Slot as Logical Replication Slot
    participant DBZ as Debezium Connector Task
    participant Kafka as Target Kafka Topic

    Admin->>API: POST /api/v1/cdc/signals (EXECUTE_SNAPSHOT, table="orders")
    API->>API: Validate Mode == M_RAWTABLE (Reject if M_OUTBOX)
    API->>DB: INSERT INTO cdc_signal_log (signal_type, payload)
    DB->>Slot: Logical WAL record written
    Slot->>DBZ: Debezium reads signal record from slot
    DBZ->>DB: Open read connection for chunk 1 (Primary Key Range)
    DB-->>DBZ: Return chunk rows
    DBZ->>Kafka: Produce chunk rows with snapshot watermark
    DBZ->>DB: Open read connection for chunk 2
    DB-->>DBZ: Return chunk rows
    DBZ->>Kafka: Produce chunk rows with snapshot watermark
    DBZ->>API: Signal execution completed
```

---

## 4. Idle Database Heartbeat & LSN Progression (C-HB)

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Heartbeat Scheduler
    participant API as CDC Service
    participant DB as PostgreSQL (cdc_heartbeat_log)
    participant Slot as Replication Slot
    participant DBZ as Debezium Connector
    participant PG_WAL as WAL Segment Manager

    loop Every 30 Seconds
        Cron->>API: POST /api/v1/cdc/heartbeat
        API->>DB: INSERT INTO cdc_heartbeat_log (lsn_position)
        DB->>Slot: Generate WAL record for heartbeat table
        Slot->>DBZ: Debezium decodes heartbeat WAL record
        DBZ-->>Slot: Acknowledge processed LSN position
        Slot->>PG_WAL: Advance confirmed_flush_lsn
        PG_WAL->>PG_WAL: Checkpoint recycles prior WAL files
    end
```

---

## 5. WAL Runaway Circuit Breaker Evaluation Loop

```mermaid
flowchart TD
    START["Telemetry Poller (Every 60s)"] --> READ_METRICS["Read WAL Lag Bytes & Disk Free Headroom"]
    READ_METRICS --> CALC_VELOCITY["Compute WAL Growth Rate (Bytes / Sec)"]
    CALC_VELOCITY --> CALC_THREAT["Calculate Threat Hours: (Free - Reserve) / (Velocity * 3600)"]

    CALC_THREAT --> CHECK_THRESHOLD{"Threat Hours < 1.0 hr?"}

    CHECK_THRESHOLD -- "YES (Threat < 1.0 hr)" --> TRIP_CB["Trip Circuit Breaker: TRIPPED_EMERGENCY_PAUSE"]
    TRIP_CB --> PAUSE_CONN["Dispatch Emergency Pause to Debezium Connector"]
    PAUSE_CONN --> NOTIFY_OPS["Send Urgent PagerDuty Notification"]

    CHECK_THRESHOLD -- "NO (Threat >= 1.0 hr)" --> CHECK_WARN{"Threat Hours < 4.0 hr?"}
    CHECK_WARN -- "YES" --> WARN_OPS["Log Warning & Elevate Metric Polling Frequency"]
    CHECK_WARN -- "NO" --> STATUS_OK["Mark Status HEALTHY & Persist Metrics"]
```


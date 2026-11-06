# Eventual Consistency Monitoring Architecture & Runtime Diagrams (WP-14)

This document contains Mermaid diagrams illustrating the runtime architectures, decision engines, state machines, three-valued logic pipelines, and self-healing safety circuits of the Eventual Consistency Monitoring subsystem per the [Eventual Consistency Monitoring Specification](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/eventual-consistency-monitoring/business-logic.md).

---

## 1. End-to-End Consistency Verification & Alerting Pipeline

```mermaid
flowchart TD
    subgraph Ingestion ["Telemetry & Probing Sources"]
        CAN["Stratified Synthetic Canary Prober<br/>(Every Stream Partition)"]
        WTR["Continuous Watermark Observer<br/>(Consumer LSN / Offset Lag)"]
        SWP["Background Merkle Bucket Sweeper<br/>(Range Checksums & Deep Audits)"]
        INV["Business Domain Invariant Probes<br/>(Sum debits == credits)"]
    end

    subgraph Comparator ["Three-Valued Logic Engine (EC-12)"]
        EVAL["Record Normalizer & Canonical Serializer<br/>(UTC, Minor Currency, Lowercase UUID)"]
        DEC{"Three-Valued Comparator<br/>(Handles Shard Drops & Timeouts)"}
    end

    subgraph Outcomes ["Evaluation State Space"]
        MTC["MATCH<br/>(Consistent, Increment Coverage SLO)"]
        UNK["UNKNOWN<br/>(Partial Shards, Lagging Replicas, Timeouts)"]
        MIS["MISMATCH<br/>(Quarantine in Tolerance Window)"]
    end

    subgraph Quarantine ["Hysteresis & Classification"]
        TOL{"Elapsed >= T_tolerance?"}
        CLF["Drift Taxonomy Classifier<br/>(GHOST | MISSING | ATTRIBUTE_MISMATCH | MANUAL_WRITE)"]
        HEAL["Rate-Limited Self-Healing Circuit<br/>(Max 2 heals/24h per key)"]
        DEAD["External Dead-Man's Switch<br/>(60s Invariant Heartbeat Out-of-Band)"]
    end

    CAN --> EVAL
    WTR --> EVAL
    SWP --> EVAL
    INV --> EVAL
    EVAL --> DEC
    DEC -->|Success & Exact Equality| MTC
    DEC -->|Error, Shard Timeout, Replica Lag| UNK
    DEC -->|Value or Version Divergence| MIS
    MIS --> TOL
    TOL -->|No (In-Flight Propagation)| MTC
    TOL -->|Yes (Persistent Divergence)| CLF
    CLF --> HEAL
    MTC --> DEAD
```

---

## 2. Three-Valued Decision Matrix (EC-12)

```mermaid
flowchart TD
    START["Evaluate Source vs Target View Record"]
    CHK_SRC{"Source Query Health?"}
    CHK_REP{"Replica Lag < Max Bound?"}
    CHK_TGT{"Target View Query Health?"}
    CHK_SHD{"Shards Succeeded == Total?"}
    EQU{"Canonical Values Equal?"}
    VER{"Target Version >= Source Version?"}

    START --> CHK_SRC
    CHK_SRC -->|DB Error / Timeout| UNKNOWN_RES["Outcome: UNKNOWN<br/>(Record Metric, No False Green)"]
    CHK_SRC -->|OK| CHK_REP
    CHK_REP -->|Lag Exceeded| UNKNOWN_RES
    CHK_REP -->|OK| CHK_TGT
    CHK_TGT -->|Timeout / Conn Refused| UNKNOWN_RES
    CHK_TGT -->|OK| CHK_SHD
    CHK_SHD -->|Partial Failure > 0| UNKNOWN_RES
    CHK_SHD -->|All Healthy| EQU
    EQU -->|Byte Equality| MATCH_RES["Outcome: MATCH<br/>(Advance Coverage Gauge)"]
    EQU -->|Mismatch| VER
    VER -->|Version Lagging & Within SLA| MATCH_TOL["Outcome: MATCH (TOLERANCE)<br/>(Pending Consumer Catchup)"]
    VER -->|Version Lagging & Past SLA| MISMATCH_VER["Outcome: MISMATCH<br/>(Class: VERSION_LAG)"]
    VER -->|Version Equal, Payload Differs| MISMATCH_ATTR["Outcome: MISMATCH<br/>(Class: ATTRIBUTE_MISMATCH)"]
```

---

## 3. Duplicate-Proof Merkle Bucket Checksum (F17.5)

```mermaid
flowchart LR
    subgraph Records ["Key-Value Rows in Bucket"]
        R1["Row 1: (K1, V1)"]
        R2["Row 2: (K2, V2)"]
        R3["Row 3: (K3, V3)"]
    end

    subgraph Hashing ["Independent Hash Computation"]
        H1["SHA256(K1, V1)"]
        H2["SHA256(K2, V2)"]
        H3["SHA256(K3, V3)"]
    end

    subgraph Composite ["Modular Arithmetic & Count Aggregation"]
        SUM["64-bit Modular Sum:<br/>(H1 + H2 + H3) mod 2^64"]
        CNT["Exact Record Count: N=3"]
        XOR["Auxiliary Key XOR:<br/>K1 ^ K2 ^ K3"]
    end

    subgraph Final ["Final Bucket Digest"]
        DIGEST["BucketHash = SHA256(ModularSum || N || KeyXOR)"]
    end

    R1 --> H1
    R2 --> H2
    R3 --> H3
    H1 --> SUM
    H2 --> SUM
    H3 --> SUM
    R1 --> CNT
    R2 --> CNT
    R3 --> CNT
    R1 --> XOR
    R2 --> XOR
    R3 --> XOR
    SUM --> DIGEST
    CNT --> DIGEST
    XOR --> DIGEST
```

---

## 4. Self-Healing Circuit & Repeat-Heal Breaker (F21.1)

```mermaid
stateDiagram-v2
    [*] --> DRIFT_CONFIRMED: Discrepancy exceeds tolerance window
    DRIFT_CONFIRMED --> QUERY_REPEAT_HISTORY: Check heal attempts (last 24h)
    
    state QUERY_REPEAT_HISTORY {
        direction tb
        EVAL_COUNT: Count >= 2?
    }

    QUERY_REPEAT_HISTORY --> CIRCUIT_OPEN: Attempts >= 2
    CIRCUIT_OPEN --> MANUAL_P1_ESCALATION: Auto-healing locked for key
    MANUAL_P1_ESCALATION --> [*]

    QUERY_REPEAT_HISTORY --> CHECK_RATE_LIMITER: Attempts < 2
    CHECK_RATE_LIMITER --> HEAL_DEFERRED: Token bucket empty
    HEAL_DEFERRED --> QUERY_REPEAT_HISTORY: Retry after backoff

    CHECK_RATE_LIMITER --> VERSION_GUARDED_APPLY: Token acquired
    VERSION_GUARDED_APPLY --> AUDIT_LOG_SUCCESS: Atomic update matched version
    AUDIT_LOG_SUCCESS --> [*]
```

---

## 5. External Dead-Man's Switch Heartbeat Architecture (EC-15)

```mermaid
sequenceDiagram
    autonumber
    participant Mon as Consistency Monitor Daemon
    participant Outbox as Independent Egress Gateway
    participant Pager as External Alert Provider (OpsGenie / PagerDuty)
    participant Eng as On-Call Reliability Engineer

    loop Every 60 Seconds
        Mon->>Mon: Verify Local Thread Liveness & Clock Skew
        Mon->>Outbox: Dispatch Heartbeat Ping with Sequence & Monotonic Timestamp
        Outbox->>Pager: HTTP POST /v1/heartbeat/eventual-consistency
        Pager-->>Outbox: 202 Accepted (Reset Watchdog Timer to 90s)
    end

    Note over Mon,Pager: Failure Mode F16.1: Monitor Crashes or Kafka/Internal Network Outage
    Mon-xOutbox: Heartbeat ceases (Process OOM / Network severed)
    Note over Pager: Watchdog Timer expires (90s elapsed without heartbeat)
    Pager->>Eng: P0 High Priority Page: "Dead-Man's Switch Ceased! Monitoring Plane Unresponsive"
```


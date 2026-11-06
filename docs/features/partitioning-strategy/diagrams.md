# Partitioning Strategy — Architectural Diagrams

This document contains Mermaid diagrams illustrating key partitioning mechanisms, hashing invariants, and migration protocols.

---

## 1. Key Hashing Pipeline & Murmur2 31-Bit Positive Bitmasking

```mermaid
flowchart TD
    subgraph Ingress ["Event Serialization & Key Construction"]
        APP["Application Service"]
        KB["Key Builder (C-KEY)"]
        CHECK["Sanitization & PII Check (PT-08)"]
    end

    subgraph HashingCore ["Canonical Murmur2 Engine (C-PRT)"]
        BYTES["UTF-8 Serialized Key: tenant:aggregate_type:id"]
        M2["Murmur2 32-bit Hash (Seed: 0x9747b28c)"]
        MASK["31-Bit Positive Mask: hash & 0x7fffffff"]
        MOD["Modulo Partition Count: masked % N"]
    end

    subgraph KafkaBrokers ["Kafka Topic (K-ORDERED)"]
        P0["Partition 0"]
        P1["Partition 1"]
        P2["Partition ..."]
        PN["Partition N-1"]
    end

    APP -->|Provides Entity Identifiers| KB
    KB --> CHECK
    CHECK -->|Valid Key String| BYTES
    BYTES --> M2
    M2 -->|32-bit Signed Integer| MASK
    MASK -->|Guaranteed Non-Negative Integer| MOD
    MOD -->|Deterministic Partition Index| P1
```

---

## 2. In-Place Expansion Hazard vs Blue/Green Stream Migration

```mermaid
flowchart TD
    subgraph Hazard ["Lethal Anti-Pattern: In-Place Expansion (F8.1 / F8.2)"]
        direction TB
        E1["Event 1 (Key: A, Mod 12 = 4)"] --> OLD_P4["Partition 4 (Source)"]
        ALTER["ALTER TOPIC: Partitions 12 -> 24"]
        E2["Event 2 (Key: A, Mod 24 = 16)"] --> NEW_P16["Partition 16 (Target)"]
        CONS["Consumer Group Reads Out-of-Order: Event 2 processed before Event 1!"]
        OLD_P4 -.-> CONS
        NEW_P16 --> CONS
    end

    subgraph Solution ["Standard Solution: Blue/Green Migration (C-MIG)"]
        direction TB
        S_TOP["Source Topic (N=12)"]
        T_TOP["Target Topic (N=24)"]
        PHASE1["1. Dual-Publish from Outbox Relay"]
        PHASE2["2. Pause Upstream & Drain Source Lag to 0"]
        PHASE3["3. Cutover Consumer Subscriptions to Target"]
        PHASE4["4. Decommission Source Topic"]

        PHASE1 --> PHASE2 --> PHASE3 --> PHASE4
    end
```

---

## 3. Stream Migration Runbook State Machine (C-MIG)

```mermaid
stateDiagram-v2
    [*] --> PLANNED : InitiateMigration

    PLANNED --> PROVISIONING_TARGET : TargetTopicConfigured
    PROVISIONING_TARGET --> DUAL_PUBLISHING : DualPublishingStarted
    PROVISIONING_TARGET --> DECOMMISSIONED : AbortMigration

    DUAL_PUBLISHING --> DRAINING_SOURCE : UpstreamWritesStopped
    DUAL_PUBLISHING --> DECOMMISSIONED : AbortMigration

    DRAINING_SOURCE --> CUTOVER : SourceLagReachedZero
    DRAINING_SOURCE --> DUAL_PUBLISHING : RollbackToDualPublish

    CUTOVER --> DECOMMISSIONED : ConsumersSwitchedToTarget

    DECOMMISSIONED --> [*] : SourceTopicArchived
```

---

## 4. Hot Partition Skew Detection & VIP Routing Engine

```mermaid
flowchart TD
    POLL["Telemetry Ingest (Per-Partition Throughput EPS)"] --> COMPUTE["Find Max EPS & Compute Median EPS"]
    COMPUTE --> CALC_RATIO["Calculate Skew Ratio: Max_EPS / Median_EPS"]

    CALC_RATIO --> EVAL{"Skew Ratio > 3.0?"}

    EVAL -- "NO (Balanced)" --> MAINTAIN["Action: MAINTAIN_CURRENT_ROUTING"]
    MAINTAIN --> LOG_OK["Persist Skew Metrics & Reset Alert"]

    EVAL -- "YES (Hot Partition Detected)" --> ALERT["Action: ISOLATE_HOT_AGGREGATE_TO_DEDICATED_TOPIC"]
    ALERT --> TRIP["Breach Alert Flag & Identify Hot Partition ID"]
    TRIP --> DISPATCH["Notify Platform Operations & Trigger VIP Topic Workflow"]
```

---

## 5. Cross-Language Partitioning Parity Verification

```mermaid
sequenceDiagram
    autonumber
    participant GoProd as Go Client (librdkafka)
    participant JavaProd as Java Client (org.apache.kafka)
    participant PyProd as Python Client (confluent-kafka)
    participant Topic as Kafka Topic (Partitions = 16)

    Note over GoProd,PyProd: Key: "tenant_corp:orders:ord_8849"

    GoProd->>GoProd: Murmur2 + 0x7fffffff % 16
    GoProd-->>Topic: Publish to Partition 7

    JavaProd->>JavaProd: DefaultPartitioner (Murmur2 + 0x7fffffff % 16)
    JavaProd-->>Topic: Publish to Partition 7

    PyProd->>PyProd: Murmur2 + 0x7fffffff % 16
    PyProd-->>Topic: Publish to Partition 7

    Note over Topic: Guaranteed Partition Affinity Across All Languages!
```


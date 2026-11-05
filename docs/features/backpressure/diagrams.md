# Backpressure — Architecture Diagrams & Control Topology

## 1. End-to-End Backpressure Chain Map (Hops H1 to H9)

```mermaid
flowchart TD
    subgraph Edge Layer
        H1[H1: Edge API Gateway] -->|HTTP Request| H2[H2: Producer Service and DB]
    end

    subgraph Producer Database Layer
        H2 -->|Transaction Commit| H3[H3: Outbox Table and WAL Slot]
        H3 -->|CDC / Poller Read| H4[H4: Outbox Relay Worker]
    end

    subgraph Streaming Backbone
        H4 -->|Durable Publish| H5[H5: Kafka NVMe Commit Log]
    end

    subgraph Consumer Cluster
        H5 -->|Fetch Batches| H6[H6: Consumer Prefetch Queues]
        H6 -->|Byte Budget Dispatch| H7[H7: Worker Lanes Bulkhead]
    end

    subgraph Storage Sink
        H7 -->|Adaptive Concurrency Execution| H8[H8: PostgreSQL / OpenSearch Sink]
        H8 -.->|Slowdown / Lock Waits| H7
        H7 -.->|Byte Budget Full| H6
        H6 -.->|Pause Intent| H5
    end
```

---

## 2. Multi-Tier Control Model (Layer A, Layer B, Layer C)

```mermaid
flowchart TD
    M([Execution Metrics Ingestion]) --> S{Saturation Signal Detected?}
    S -->|No| N[Steady State: Additive Concurrency Increase]
    S -->|Yes| EVAL{Evaluate Root Cause}

    EVAL -->|Transient Slowness| LA[Layer A: Adaptive Concurrency Limiter]
    LA --> LA_ACT[Halve Concurrency Limit: Floor = 1]

    EVAL -->|Queue Full OR Heap >= 70%| LB[Layer B: Partition Pause Controller]
    LB --> LB_ACT[Dispatch Pause Intent to Poll Thread]
    LB_ACT --> KAFKA_PAUSE[consumer.Pause assigned partitions]

    EVAL -->|Hard Error / Connection Outage| LC[Layer C: Circuit Breaker]
    LC --> LC_ACT[Open Breaker: Reject and Divert to Dead-Letter Quarantine]
```

---

## 3. Slow-Start Gradual Recovery Ramp Workflow

```mermaid
sequenceDiagram
    autonumber
    actor Watchdog as Health Watchdog Daemon
    participant Engine as Backpressure Service
    participant DB as backpressure_state (PostgreSQL)
    participant Poller as Kafka Consumer Poller Thread
    actor Worker as Consumer Worker Lane
    actor Sink as PostgreSQL Sink

    Note over Watchdog: Dependency latency healthy for 30s and Heap < 40%
    Watchdog->>Engine: ReportRecovery (HandlerID, DependencyID)
    Engine->>DB: Transition to RECOVERING (Concurrency Limit = 25% of pre-pause)
    Engine->>DB: Insert Audit Log (RECOVERY_STARTED)
    Engine->>Poller: Send Resume Command (Assigned Partitions)
    Poller->>Poller: consumer.Resume(partitions)
    Poller->>Worker: Stream records under slow-start limit
    Worker->>Sink: Low-concurrency writes (No thundering herd)
    Note over Watchdog,Sink: Stable execution sustained for 30s
    Watchdog->>Engine: ConfirmRecovery
    Engine->>DB: Transition to NORMAL (Full throughput restored)
    Engine->>DB: Insert Audit Log (RECOVERY_CONFIRMED)
```

---

## 4. Autoscaling Safety Interplay (BP-05 Enforcement)

```mermaid
flowchart TD
    LAG[Kafka Consumer Lag Detected] --> KEDA{KEDA Autoscaler Poll}
    KEDA --> BP_CHECK[Query Backpressure Engine for Handler and Topic]
    BP_CHECK --> STATE{State == SATURATED?}

    STATE -->|YES| FREEZE[Suppress Scale-Out: Record Reason DEPENDENCY_SATURATED]
    FREEZE --> ALERT[Emit Warning: Scaling Frozen to Protect Database]

    STATE -->|NO| HEADROOM{Dependency Connection Budget Available?}
    HEADROOM -->|YES| SCALE[Scale Out Consumer Pods up to Partition Limit]
    HEADROOM -->|NO| CAP[Cap Replicas at Max Connection Pool Budget]
```

---

## 5. Physical Bulkhead & Priority Isolation (BP-10)

```mermaid
flowchart LR
    subgraph Tier 0 [T0: Transactional Financial Payments]
        T0_Topic[Kafka Topic: payments.v1] --> T0_Consumer[T0 Consumer Pods]
        T0_Consumer --> T0_Pool[Dedicated DB Connection Pool: 20 slots]
        T0_Pool --> PrimaryDB[(PostgreSQL Primary)]
    end

    subgraph Tier 2 [T2: Background Telemetry and Audits]
        T2_Topic[Kafka Topic: telemetry.v1] --> T2_Consumer[T2 Consumer Pods]
        T2_Consumer --> T2_Pool[Dedicated DB Connection Pool: 5 slots]
        T2_Pool --> PrimaryDB
    end

    subgraph Isolation Guardrail
        T2_Spike[Telemetry Traffic Spike] -.->|Cannot Exhaust| T0_Pool
        T2_Spike -.->|Can Be Sampled or Dropped| T2_Consumer
    end
```


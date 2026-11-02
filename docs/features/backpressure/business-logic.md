# Backpressure — Business Logic Decision Tree

```
BACKPRESSURE (BUSINESS DECISION TREE)
│
├── 1. DOWNSTREAM CAPACITY DETECTION & SATURATION METRICS
│   └── Trigger: Consumer Pipeline Detects Downstream Slowdown
│       ├── Monitored Invariants:
│       │   ├── Metric 1: In-Flight Goroutine / Worker Pool Utilization (> 85%)
│       │   ├── Metric 2: PostgreSQL / OpenSearch Write Latency p99 (> 250ms)
│       │   ├── Metric 3: Process Resident Memory (RSS) approaching 75% of container limit
│       │   └── Metric 4: Internal Channel Buffer Fill Ratio (`len(workQueue) / cap(workQueue) > 0.8`)
│       │
│       ├── Evaluation: Has saturation threshold been crossed?
│       │   ├── NO ──> Maintain steady-state consumption (`poll()` continues normally)
│       │   └── YES ──> Trigger Backpressure Throttling Engine (Tier 2)
│
├── 2. CONSUMER-SIDE BACKPRESSURE APPLICATION
│   └── Trigger: Backpressure Triggered on Local Consumer Instance
│       ├── Action 1: Dynamic Partition Pausing (Zero Memory Explosion)
│       │   ├── Call: `consumer.Pause(assignedPartitions)`
│       │   ├── Impact: Client stops fetching new byte batches from Kafka brokers
│       │   ├── Broker Effect: Message backlog accumulates in Kafka disk log (Safe durable storage)
│       │   └── Heartbeat Safety: Background heartbeat thread continues pulsing to prevent rebalance eviction
│       │
│       ├── Action 2: Drain In-Flight Work Queue
│       │   ├── Worker pool processes existing buffered tasks to completion
│       │   ├── Memory usage drops below recovery watermark (50% RSS)
│       │   └── Database connection pool active count returns to normal
│       │
│       └── Action 3: Partition Resumption
│           ├── Condition: Recovery watermarks verified across all subsystems
│           ├── Call: `consumer.Resume(assignedPartitions)`
│           └── Normal polling loop resumes smoothly
│
└── 3. CLUSTER-LEVEL LOAD SHEDDING & KEDA ELASTIC SCALING
    └── Trigger: Sustained Traffic Burst (10x Normal Load)
        ├── Layer 1: Autoscaling (KEDA Kafka Trigger)
        │   ├── Rule: If total consumer lag across partitions > 5,000 for > 60s
        │   ├── Action: Scale out Kubernetes consumer pods up to partition count limit
        │   └── Constraint: Consumer replicas capped at `total_partitions` (excess pods sit idle)
        │
        └── Layer 2: Producer-Side Quotas & Load Shedding
            ├── Broker Quota Enforcement:
            │   ├── Kafka Client Quotas throttle abusive producer tenant IPs
            │   └── Producer encounters `ProduceRateExceededException` (Backs off at source)
            │
            └── Edge API Gateway (Envoy) Load Shedding:
                ├── Envoy sheds low-priority background telemetry traffic with HTTP 429
                └── Preserves bandwidth and compute for core transactional domain events
```

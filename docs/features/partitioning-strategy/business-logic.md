# Partitioning Strategy — Business Logic Decision Tree

```
PARTITIONING STRATEGY (BUSINESS DECISION TREE)
│
├── 1. PARTITION KEY ASSIGNMENT & HASHING TIER
│   └── Trigger: Producer Serializes Record for Kafka Topic
│       ├── Check: What is the primary entity boundary for event ordering?
│       │   ├── Rule: Partition key MUST be the root Domain Aggregate ID
│       │   │   ├── Example: `order_id` for order lifecycle, `account_id` for ledger transactions
│       │   │   └── Invariant: All events belonging to the same aggregate ID land on the exact same partition
│       │   │
│       │   ├── Check: Is the record missing a partition key?
│       │   │   ├── YES (Null Key) ──> Round-robin / Sticky Partitioning
│       │   │   │   └── Usage: Permitted ONLY for unordered telemetry; BANNED for domain aggregate events
│       │   │   └── NO ──> Murmur2 Hashing Algorithm
│       │   │       └── Target Partition = `Math.abs(Murmur2(key)) % total_partitions`
│       │   │
│       │   └── Edge Case: Hot Partition Risk (Celebrity Tenant / High-Volume Account)
│       │       ├── Detection: Partition throughput skew > 300% above median
│       │       ├── Remediation A (Compound Key): `key = aggregate_id + "_" + (seq % N)`
│       │       │   └── Tradeoff: Distributes load but requires downstream consumer reassembly
│       │       └── Remediation B: Dedicated Topic routing for high-volume VIP tenants
│       │
│       └── Invariant: Never use random UUIDs as partition keys for stateful aggregates (destroys ordering)
│
├── 2. CAPACITY PLANNING & PARTITION SIZING TIER
│   └── Trigger: Provisioning New Kafka Topic or Sizing Partitions
│       ├── Sizing Formula:
│       │   ├── `P = Max(Target_Produce_MB_s / Single_Partition_Write_MB_s, Target_Consume_MB_s / Single_Consumer_Read_MB_s)`
│       │   ├── Standard Baselines: Single partition write ~10MB/s, single consumer read ~25MB/s
│       │   └── Headroom Factor: Apply 2.5x multiplier for peak traffic bursts
│       │
│       ├── Topic Replication Configuration:
│       │   ├── `replication.factor=3`
│       │   ├── `min.insync.replicas=2`
│       │   └── `unclean.leader.election.enable=false` (Prioritize durability over availability)
│       │
│       └── Retention & Compaction Policy:
│           ├── Append-Only Event Stream: `cleanup.policy=delete`, `retention.ms=604800000` (7 days)
│           └── Entity State Topic: `cleanup.policy=compact` (Retain latest record per aggregate key)
│
└── 3. TOPIC EXPANSION & RE-PARTITIONING RUNBOOK TIER
    └── Trigger: Topic Reaches Capacity; Operator Needs to Increase Partition Count
        ├── Warning: Increasing partition count remaps keys: `Hash(key) % New_N != Hash(key) % Old_N`
        │   └── Immediate Impact: Subsequent events for an aggregate will land on a different partition, BREAKING ORDER
        │
        ├── Standard Procedure for Partition Expansion:
        │   ├── Phase 1: Drain In-Flight Events
        │   │   ├── Stop upstream producers or buffer events in outbox table
        │   │   └── Wait until consumer group lag on existing partitions reaches exactly 0
        │   │
        │   ├── Phase 2: Apply Partition Increase via Kafka Admin API
        │   │   └── Command: `kafka-topics.sh --alter --topic <name> --partitions <new_count>`
        │   │
        │   ├── Phase 3: Resume Processing
        │   │   ├── Restart consumer groups (rebalance assigns new partitions)
        │   │   └── Release producer buffer
        │   │
        │   └── Alternative (Zero-Downtime Safe Route):
        │       ├── Create new topic version with higher partition count: `<topic>.v2`
        │       └── Switch producers and consumers through staged blue/green migration window
```

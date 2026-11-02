# Offset Management — Business Logic Decision Tree

```
OFFSET MANAGEMENT (BUSINESS DECISION TREE)
│
├── 1. OFFSET COMMIT STRATEGY & LIFECYCLE TIER
│   └── Trigger: Consumer Completes Processing of Event Batch
│       ├── Check: What commit mechanism is enabled?
│       │   ├── Mode 1: Auto-Commit (`enable.auto.commit = true`) [STRICTLY BANNED]
│       │   │   ├── Violation: Commits offsets asynchronously on a periodic background timer
│       │   │   └── Catastrophic Failure: If consumer crashes mid-batch, uncommitted records are skipped (DATA LOSS)
│       │   │
│       │   └── Mode 2: Manual Synchronous / Asynchronous Commit (`enable.auto.commit = false`) [ENFORCED]
│       │       ├── Pattern A: Commit-After-Process (At-Least-Once Delivery)
│       │       │   ├── Step 1: Write state mutation + deduplication row to DB
│       │       │   ├── Step 2: DB Commit succeeds
│       │       │   └── Step 3: Issue `consumer.CommitOffsets(map[TopicPartition]Offset)`
│       │       └── Guarantee: Never skips uncommitted data; duplicates absorbed by idempotency layer
│       │
│       └── Initial Offset Reset Policy:
│           ├── `auto.offset.reset = earliest`: Consume from beginning of topic (Guarantees zero missed events)
│           └── `auto.offset.reset = latest`: Consume only new inbound events (Used ONLY for real-time telemetry)
│
├── 2. CONSUMER LAG MONITORING & DRIFT DETECTION
│   └── Trigger: Telemetry Collector Monitors Consumer Group Offsets
│       ├── SRE Metric Calculations:
│       │   ├── `LogEndOffset`: The highest offset committed to the Kafka partition log by producers
│       │   ├── `CurrentOffset`: The latest offset processed and committed by the consumer group
│       │   └── `ConsumerLag = LogEndOffset - CurrentOffset`
│       │
│       ├── Lag Severity Evaluation:
│       │   ├── Level 1: Normal Operation (`Lag < 1000`, Lag Latency < 5s)
│       │   │   └── State: HEALTHY (Steady state)
│       │   │
│       │   ├── Level 2: Traffic Surge / Transient Spike (`Lag > 10,000`, Duration < 5m)
│       │   │   └── Action: Horizontal Pod Autoscaler (HPA / KEDA) triggers scale-out up to max partition count
│       │   │
│       │   └── Level 3: Poison Pill / Consumer Starvation (`Lag` continuously growing, Rate = 0)
│       │       └── Action: Fire PagerDuty P1 alert: `ConsumerGroupStalledAlert`
│
└── 3. ADMINISTRATIVE OFFSET REWIND & FAST-FORWARD RUNBOOK
    └── Trigger: Disaster Recovery, Replay Request, or Poison Message Bypass
        ├── Safe Operation Invariant: ALWAYS stop all consumers in the group BEFORE resetting offsets
        │
        ├── Procedure 1: Rewind to Beginning / Specific Timestamp (State Rebuild / Replay)
        │   ├── Dry-Run Verification:
        │   │   ```bash
        │   │   kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group event-platform-group \
        │   │     --topic orders.order.placed.v1 --reset-offsets --to-datetime 2026-11-01T00:00:00.000 --dry-run
        │   │   ```
        │   ├── Execution: Execute with `--execute` flag
        │   └── Action: Start isolated replay consumers targeting shadow read store
        │
        └── Procedure 2: Fast-Forward Past Corrupted Poison Batch (Emergency Bypass)
            ├── When: Poison batch stalls main partition and DLQ redrive fails
            ├── Action: Shift offset forward: `--shift-by +N --execute`
            └── Post-Action: Audit dropped offsets from raw disk segments for forensic analysis
```

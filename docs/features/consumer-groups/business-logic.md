# Consumer Groups — Business Logic Decision Tree

```
CONSUMER GROUPS (BUSINESS DECISION TREE)
│
├── 1. GROUP MEMBERSHIP & REBALANCE PROTOCOL TIER
│   └── Trigger: Consumer Instance Boots Up or Scales Out
│       ├── Check: What is the partition assignment strategy configured?
│       │   ├── Standard 1: CooperativeStickyAssignor [RECOMMENDED]
│       │   │   ├── Invariant: Incremental cooperative rebalancing; zero "stop-the-world" pauses
│       │   │   ├── Mechanism: Unaffected partitions continue processing while only reassigned partitions migrate
│       │   │   └── Behavior: Prevents catastrophic lag spikes during rolling pod deployments
│       │   │
│       │   └── Standard 2: Eager Rebalance (RangeAssignor / RoundRobinAssignor) [LEGACY / BANNED]
│       │       └── Violation: Revokes all partitions across all consumers, stalling consumer throughput
│       │
│       ├── Membership Identity Configuration:
│       │   ├── Static Membership (`group.instance.id = ${POD_NAME}`)
│       │   │   ├── When: Kubernetes StatefulSet or deterministic pod names
│       │   │   ├── Advantage: Pod restarts within `session.timeout.ms` (45s) DO NOT trigger rebalances
│       │   │   └── Invariant: Avoids rebalance storms during routine container restarts
│       │   │
│       │   └── Dynamic Membership (Default / Ephemeral)
│       │       └── Constraint: Triggers immediate rebalance when consumer joins or leaves
│       │
│       └── Timeout & Heartbeat Invariants:
│           ├── `session.timeout.ms = 45000` (Broker marks consumer dead after 45s of missed heartbeats)
│           ├── `heartbeat.interval.ms = 15000` (Heartbeat thread pulses every 15s - 1/3 of session timeout)
│           └── `max.poll.interval.ms = 300000` (5 minutes max allowable processing time per polled batch)
│
├── 2. PARTITION CONSUMPTION LOOP & BATCH SIZING TIER
│   └── Trigger: Consumer Thread Calls `consumer.Poll(timeout)`
│       ├── Step 1: Batch Ingestion Limits
│       │   ├── `max.poll.records = 500` (Caps processing time to prevent hitting `max.poll.interval.ms`)
│       │   └── `fetch.max.bytes = 52428800` (50MB maximum data fetched per poll across all partitions)
│       │
│       ├── Step 2: Processing Loop Execution
│       │   ├── Sub-Step A: Iterate records sequentially or partition-parallelized
│       │   ├── Sub-Step B: Execute business projection logic within transactional boundary
│       │   └── Sub-Step C: Monitor processing duration
│       │       ├── Condition: Batch processing time approaches 80% of `max.poll.interval.ms`
│       │       └── Action: Pause partition fetching (`consumer.Pause(partitions)`); finish active batch
│       │
│       └── Step 3: Offset Commit Execution
│           ├── Rule: Commit offset ONLY after batch has fully committed to local DB read store
│           └── Method: Synchronous commit on shutdown, asynchronous commit during continuous poll
│
└── 3. REBALANCE STORM MITIGATION & GRACEFUL SHUTDOWN TIER
    └── Trigger: Pod Receives SIGTERM from Kubelet (Node Drain / Deploy / Scale-In)
        ├── Step 1: Revocation Listener Invocation
        │   ├── Traps: `onPartitionsRevoked` callback triggered
        │   ├── Action: Flush in-flight database transactions immediately
        │   └── Action: Execute blocking synchronous commit for latest processed offsets (`CommitSync()`)
        │
        ├── Step 2: Leave Group Notice
        │   ├── Call: `consumer.Close()`
        │   ├── Action: Sends explicit `LeaveGroupRequest` to Kafka Group Coordinator
        │   └── Result: Coordinator immediately reassigns partitions without waiting for session timeout
        │
        └── Failure Scenario: Consumer Thread Hung / Deadlocked
            ├── Condition: `poll()` not called within `max.poll.interval.ms`
            ├── Broker Action: Coordinator evicts consumer from group; triggers partition rebalance
            └── Process Action: Internal watchdog thread detects poll stall; issues hard `os.Exit(1)`
```

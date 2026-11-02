# Event Ordering Protection — Business Logic Decision Tree

```
EVENT ORDERING PROTECTION (BUSINESS DECISION TREE)
│
├── 1. PRODUCER-SIDE ORDERING GUARANTEES
│   └── Trigger: Producer Emits Series of Events for a Single Aggregate
│       ├── Rule 1: Monotonic Sequence Assignment
│       │   ├── Action: Query local DB aggregate state for `current_version`
│       │   ├── Assign: `aggregate_seq = current_version + 1`
│       │   └── Invariant: Every event for an aggregate carries an unbroken consecutive integer (1, 2, 3...)
│       │
│       ├── Rule 2: Producer Connection Serialization
│       │   ├── Constraint: `max.in.flight.requests.per.connection <= 5` WITH `enable.idempotence=true`
│       │   ├── Mechanism: Kafka broker tracks Producer ID (PID) + Sequence Number per batch
│       │   └── Guarantee: Broker rejects out-of-sequence batches, preventing in-flight reordering
│       │
│       └── Broker Response Evaluation:
│           ├── Success: Sequence persisted in order on single partition
│           └── OutOfOrderSequenceException from Broker:
│               ├── Root Cause: Network dropped a preceding batch that was retried out of sequence
│               └── Action: Fail transaction; reset producer sequence cache; replay from outbox
│
├── 2. CONSUMER-SIDE SEQUENCE VALIDATION & GAP DETECTION
│   └── Trigger: Consumer Receives Event with `aggregate_id` and `aggregate_seq`
│       ├── Check: Compare `incoming_seq` with `last_applied_seq` in consumer read store:
│       │   │
│       │   ├── Case A: `incoming_seq == last_applied_seq + 1` (Exact Consecutive Order)
│       │   │   ├── Action: Apply event to read model projection
│       │   │   ├── Update: `last_applied_seq = incoming_seq`
│       │   │   └── Check: Are there buffered future events waiting in memory for this aggregate?
│       │   │       ├── YES ──> Process buffered events sequentially
│       │   │       └── NO ──> Commit Kafka partition offset
│       │   │
│       │   ├── Case B: `incoming_seq <= last_applied_seq` (Duplicate / Stale Event)
│       │   │   ├── Action: Silently ignore payload (Idempotency absorption)
│       │   │   ├── Metric: Increment `duplicate_events_absorbed_total`
│       │   │   └── Commit Kafka offset without modifying read model
│       │   │
│       │   └── Case C: `incoming_seq > last_applied_seq + 1` (SEQUENCE GAP DETECTED!)
│       │       ├── Root Cause: Preceding event dropped, delayed in retry loop, or rebalance anomaly
│       │       └── Action: Enter Quarantine / Buffer-and-Wait Loop (Tier 3)
│
└── 3. SEQUENCE GAP RESOLUTION & QUARANTINE TIER
    └── Trigger: Sequence Gap Detected (`incoming_seq = N+2`, but expected `N+1`)
        ├── Step 1: In-Memory Holding Buffer
        │   ├── Hold `incoming_seq` in memory buffer with deadline (e.g., 5 seconds)
        │   └── Check: Does missing event `N+1` arrive on the partition within deadline?
        │       ├── YES ──> Process `N+1`, then immediately process buffered `N+2` (Order restored)
        │       └── NO (Timeout Expired) ──> Escalate to State Store Query
        │
        ├── Step 2: Query Authoritative Event Store / Outbox DB
        │   ├── Query: `SELECT payload FROM event_store WHERE aggregate_id = ? AND seq = N+1`
        │   ├── Event Found in DB:
        │   │   ├── Fetch and apply missing `N+1` event to projection
        │   │   └── Release buffered `N+2` event into pipeline
        │   └── Event NOT Found in DB (True Gap / Data Loss):
        │       ├── [ALERT: Critical Event Loss Incident Triggered]
        │       ├── Action: Halt consumer group partition processing to prevent state corruption
        │       └── Operator Runbook: Trigger partition rewind or manual administrative reconciliation
```

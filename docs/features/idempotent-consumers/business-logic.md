# Idempotent Consumers — Business Logic Decision Tree

```
IDEMPOTENT CONSUMERS (BUSINESS DECISION TREE)
│
├── 1. AT-LEAST-ONCE DUPLICATE INGESTION SCENARIOS
│   └── Real-World Triggers Causing Message Duplication:
│       ├── Scenario A: Producer Network Timeout (Producer retries; broker writes duplicate before ack)
│       ├── Scenario B: Consumer Rebalance (Consumer processes records, crashes before committing offset)
│       └── Scenario C: Redelivery from Retry / DLQ Topics
│
├── 2. IDEMPOTENCY MECHANISM SELECTION TIER
│   └── Trigger: Consumer Evaluates Inbound Event Payload
│       ├── Mechanism 1: RDBMS Deduplication Table (Transactional Sink) [GOLD STANDARD]
│       │   ├── Schema: `processed_events (event_id UUID PRIMARY KEY, processed_at TIMESTAMP, aggregate_id TEXT)`
│       │   ├── Execution: Single PostgreSQL ACID Transaction
│       │   │   ├── Step A: `INSERT INTO processed_events (event_id, ...) VALUES ($1, ...)`
│       │   │   │   ├── Primary Key Conflict (Error 23505 Unique Violation):
│       │   │   │   │   └── Action: Roll back transaction immediately; acknowledge Kafka offset (DUPLICATE ABSORBED)
│       │   │   │   └── Insert Succeeds:
│       │   │   │       ├── Step B: Apply business projection mutation (`UPDATE read_model ...`)
│       │   │   │       └── Step C: Commit ACID transaction
│       │   │   └── Guarantee: Exactly-once state mutation effect despite at-least-once transport
│       │   │
│       │   ├── Mechanism 2: Natural Key / Upsert Idempotency
│       │   │   ├── Usage: Key-value stores, OpenSearch documents, Redis hashes
│       │   │   ├── Pattern: `PUT /indexes/orders/_doc/<order_id>` (Idempotent by nature)
│       │   │   └── Version Guard: Check `incoming_version >= current_doc_version` before overwriting
│       │   │
│       │   └── Mechanism 3: Redis Distributed TTL Deduplication Filter (High-Throughput Sink)
│       │       ├── Pattern: `SET dedupe:event:<event_id> "LOCKED" NX EX 86400`
│       │       ├── Key Already Exists (Returns nil):
│       │       │   └── Action: Drop message; skip downstream processing
│       │       └── Key Acquired (Returns OK):
│       │           └── Action: Execute processing; update to "COMPLETED" on success
│
└── 3. STATE DRIFT & REPLAY DEDUPLICATION INVARIANTS
    └── Trigger: Replaying Historical Events from Beginning of Topic
        ├── Risk: Previous deduplication table entries prevent historical replay from modifying projections
        │
        ├── Isolation Rule for Replay:
        │   ├── Replay workloads MUST connect to an isolated, newly provisioned shadow projection table
        │   ├── Deduplication table for replay is initialized EMPTY
        │   └── Live production deduplication table is NEVER purged or mutated during replays
        │
        └── Table Pruning Runbook:
            ├── TTL Retention: Retain `processed_events` records for topic retention window (e.g. 7 days + 1 day buffer)
            └── Automated Purge: Scheduled pg_cron job drops partition older than 8 days
```

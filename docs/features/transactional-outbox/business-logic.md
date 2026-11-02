# Transactional Outbox — Business Logic Decision Tree

```
TRANSACTIONAL OUTBOX (BUSINESS DECISION TREE)
│
├── 1. ATOMIC COMMIT & DUAL-WRITE ELIMINATION TIER
│   └── Trigger: Application Service Executes Business Mutation
│       ├── Check: Can the service write directly to PostgreSQL AND Kafka simultaneously?
│       │   └── Decision: STRICTLY FORBIDDEN (Dual-write anti-pattern risks partial failure)
│       │
│       ├── Atomic Unit of Work Execution (Single PostgreSQL ACID Transaction):
│       │   ├── Step 1: Execute primary business mutations (e.g. `INSERT INTO orders`, `UPDATE inventory`)
│       │   ├── Step 2: Insert outbound event into outbox table:
│       │   │   ```sql
│       │   │   INSERT INTO outbox (
│       │   │     id, aggregate_type, aggregate_id, event_type, 
│       │   │     payload, headers, status, created_at
│       │   │   ) VALUES (
│       │   │     uuidv7(), 'order', $1, 'OrderPlaced', 
│       │   │     $payload, $headers, 'PENDING', NOW()
│       │   │   );
│       │   │   ```
│       │   └── Step 3: Transaction COMMIT
│       │       ├── Commit Fails ──> Neither business state NOR event is persisted (Clean rollback)
│       │       └── Commit Succeeds ──> State and Outbox record atomically guaranteed in WAL
│
├── 2. RELAY & DISPATCH MECHANISM (POLLING VS CDC)
│   └── Trigger: Extracting Outbox Records for Transmission to Kafka Broker
│       ├── Implementation Path A: Log-Based CDC Relay (Debezium Outbox Event Router) [RECOMMENDED]
│       │   ├── Architecture: Debezium Connect monitors PostgreSQL Write-Ahead Log (`wal_level=logical`)
│       │   ├── SMT Configuration: `io.debezium.transforms.outbox.EventRouter`
│       │   │   ├── Maps `aggregate_id` -> Kafka Message Key (ensuring partition affinity)
│       │   │   ├── Maps `aggregate_type` + `event_type` -> Destination Topic
│       │   │   └── Unpacks `payload` into Kafka record value
│       │   └── Pros: Zero polling overhead, zero DB row locking, sub-second latency
│       │
│       └── Implementation Path B: Application Poller Relay (Fallback / Lightweight)
│           ├── Worker Loop: `SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED`
│           ├── Transmission: Worker pushes batch to Kafka producer with `acks=all`
│           └── State Update: On broker ACK, `UPDATE outbox SET status = 'PUBLISHED' WHERE id IN (...)`
│
└── 3. RUNTIME SAFEGUARDS & WAL REPLICATION SLOT MONITORING
    └── Trigger: Outbox Relay Operates Continuously in Production
        ├── Risk: CDC Replication Slot Bloat & WAL Disk Exhaustion
        │   ├── Root Cause: Downstream Kafka outage causes Debezium to stop acknowledging WAL LSN
        │   ├── Impact: PostgreSQL retains WAL segments indefinitely; host disk fills to 100%
        │   │
        │   └── Monitoring & Prevention Safeguards:
        │       ├── Metric 1: Track `pg_replication_slots.wal_status` ('normal' vs 'extended')
        │       ├── Metric 2: Alert when `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 5GB`
        │       └── Database Guardrail: Set `max_slot_wal_keep_size = 20GB`
        │           └── Failsafe: Slot drops before host disk fills, protecting write availability
        │
        └── Outbox Table Truncation & Compaction Runbook:
            ├── Automated Job: Truncate / delete records where `status = 'PUBLISHED' AND created_at < NOW() - INTERVAL '48 hours'`
            └── Partitioning: Partition outbox table by `created_at` (Daily partitions dropped via `DROP TABLE`)
```

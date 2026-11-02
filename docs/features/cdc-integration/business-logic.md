# CDC Integration — Business Logic Decision Tree

```
CDC INTEGRATION (BUSINESS DECISION TREE)
│
├── 1. CHANGE-DATA CAPTURE ENGINE SELECTION & BOOTSTRAP TIER
│   └── Trigger: Provisioning Debezium Connect for PostgreSQL Ingestion
│       ├── Check: Is database engine properly tuned for logical decoding?
│       │   ├── Check 1: `wal_level = logical`
│       │   ├── Check 2: `max_wal_senders >= 10`
│       │   ├── Check 3: `max_replication_slots >= 10`
│       │   └── All Checked ──> Proceed to Debezium Connector Configuration
│       │
│       ├── Initial Snapshot Mode Selection:
│       │   ├── Mode A: `initial` (Default)
│       │   │   ├── Action: Takes consistent table snapshot before streaming continuous WAL changes
│       │   │   └── Usage: Required when bootstrapping new read models from pre-existing tables
│       │   │
│       │   ├── Mode B: `schema_only`
│       │   │   ├── Action: Records table structure; streams ONLY changes occurring AFTER startup
│       │   │   └── Usage: Used when historical data is already loaded or irrelevant
│       │   │
│       │   └── Mode C: `incremental` (Signal Table Driven)
│       │       └── Action: Takes low-impact windowed chunk snapshots without exclusive table locks
│       │
│       └── Plugin Selection: `plugin.name = pgoutput` (Native PostgreSQL logical decoding plugin)
│
├── 2. TOPIC ROUTING, FILTERING & OUTBOX TRANSFORMATION TIER
│   └── Trigger: Debezium Captures Row Mutation from WAL
│       ├── Routing Strategy A: Raw Table CDC
│       │   ├── Destination Topic: `<server>.<schema>.<table>` (e.g. `dbserver1.public.orders`)
│       │   └── Payload Structure: Debezium envelope with `before`, `after`, `op` (c/u/d), `source`, `ts_ms`
│       │   └── Risk: Leaks internal database schema directly to downstream consumers (Coupling anti-pattern)
│       │
│       └── Routing Strategy B: Outbox Event Router SMT [STRONGLY RECOMMENDED]
│           ├── Configuration:
│           │   ```properties
│           │   transforms=outbox
│           │   transforms.outbox.type=io.debezium.transforms.outbox.EventRouter
│           │   transforms.outbox.route.by.field=aggregate_type
│           │   transforms.outbox.route.topic.replacement=${routedByValue}.events
│           │   transforms.outbox.table.fields.additional.placement=event_type:header:eventType
│           │   ```
│           ├── Mechanism: Unpacks application domain events from outbox table directly into clean Kafka topics
│           └── Guarantee: Downstream consumers receive clean domain envelopes without DB metadata pollution
│
└── 3. WAL BLOAT MITIGATION & FAILOVER RUNBOOK TIER
    └── Trigger: Debezium Connector Encounters Downstream Failure
        ├── Hazard: Replication Slot Pinning PostgreSQL WAL
        │   ├── Root Cause: If Debezium stalls or Kafka rejects writes, replication slot LSN fails to advance
        │   ├── Consequence: PostgreSQL retains all WAL logs on disk; storage fills to 100% (DB PANIC)
        │   │
        │   └── Monitoring & Circuit Breakers:
        │       ├── Monitor: `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag FROM pg_replication_slots;`
        │       ├── Warning: Alert when replication slot lag > 5GB
        │       └── Safety Guardrail: Set PostgreSQL parameter `max_slot_wal_keep_size = 20GB`
        │           └── Result: Slot invalidated automatically before host runs out of disk space
        │
        └── Debezium Failover Recovery:
            ├── Scenario: Replication slot dropped or marked invalid
            ├── Action: Remove broken connector config from Debezium REST API (:8083)
            ├── Re-provisioning: Recreate connector with fresh replication slot name
            └── Data Resync: Trigger incremental snapshot via Debezium signaling table to backfill state
```

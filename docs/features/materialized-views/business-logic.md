# Materialized Views — Business Logic Decision Tree

```
MATERIALIZED VIEWS (BUSINESS DECISION TREE)
│
├── 1. QUERY MODEL SELECTION & PROJECTION STORAGE TIER
│   └── Trigger: Domain Event Emitted from Core Aggregates
│       ├── Check: What is the primary query access pattern for the read model?
│       │   ├── Access Pattern A: Full-Text Search, Multi-Facet Filters & Analytics
│       │   │   └── Target Store: OpenSearch / Elasticsearch
│       │   │       ├── Rationale: Highly optimized inverted indices, tokenizers, and geo/range aggregation
│       │   │       └── Update Mechanism: Direct document partial upsert via REST API (`POST /index/_update/<id>`)
│       │   │
│       │   ├── Access Pattern B: Sub-Millisecond Key-Value Lookup (Profile / Session)
│       │   │   └── Target Store: Redis (Hashes / Strings)
│       │   │       ├── Rationale: In-memory retrieval with sub-5ms SLA
│       │   │       └── Update Mechanism: Atomic Redis pipelines (`HSET`, `EXPIRE`)
│       │   │
│       │   └── Access Pattern C: Relational Joins & Financial Reporting
│       │       └── Target Store: PostgreSQL (Dedicated Read Replica / Schema)
│       │           ├── Rationale: Normalized multi-table projections, standard SQL querying
│       │           └── Update Mechanism: UPSERT (`INSERT ... ON CONFLICT (id) DO UPDATE`)
│       │
│       └── Architectural Rule: Write model database is NEVER queried by read-heavy user dashboards
│
├── 2. PROJECTION UPDATE PIPELINE & EVENT PROCESSING
│   └── Trigger: Projection Worker Consumes Event from Kafka Topic
│       ├── Step 1: Idempotency Verification
│       │   ├── Check: Has this `event_id` already been applied to this view?
│       │   ├── YES ──> Skip projection logic; acknowledge offset
│       │   └── NO ──> Proceed to State Projection Transformation
│       │
│       ├── Step 2: Version Monotonicity Guard
│       │   ├── Check: Is `incoming_event.aggregate_seq > current_view_version`?
│       │   ├── YES ──> Apply mutation:
│       │   │   ```sql
│       │   │   UPDATE order_materialized_view 
│       │   │   SET status = 'PAID', total_paid = total_paid + $1, version = $2, updated_at = NOW()
│       │   │   WHERE id = $3 AND version = $2 - 1;
│       │   │   ```
│       │   └── NO (Stale / Out-of-Sequence Event):
│       │       └── Action: Quarantine event in local buffer; avoid regressing newer state
│       │
│       └── Step 3: Record Materialization Latency
│           └── Metric: Calculate `materialization_lag_ms = Now() - event.occurred_at`
│
└── 3. VIEW REBUILD & RE-PROJECTION RUNBOOK
    └── Trigger: Read Model Schema Updated or Data Inconsistency Detected
        ├── Strategy: Zero-Downtime Blue/Green Read Store Rebuild
        │   ├── Phase 1: Deploy new view schema to shadow index/table: `orders_summary_v2`
        │   ├── Phase 2: Start new projection consumer group from `offset = 0` (earliest)
        │   ├── Phase 3: Wait for shadow consumer group lag to reach zero (Full catch-up)
        │   ├── Phase 4: Atomic Alias Switch:
        │   │   ├── OpenSearch: Update alias `orders_summary` from `v1` to `v2`
        │   │   └── PostgreSQL: Swap table names inside transaction
        │   └── Phase 5: Decommission old `orders_summary_v1` table after 48-hour burn-in period
        │
        └── Failure Recovery: Projection Worker Crash
            ├── Action: Kubernetes restarts worker container
            └── Resumption: Worker resumes from last committed Kafka offset; idempotency absorbs duplicates
```

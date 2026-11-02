# Eventual Consistency Monitoring — Business Logic Decision Tree

```
EVENTUAL CONSISTENCY MONITORING (BUSINESS DECISION TREE)
│
├── 1. DRIFT DETECTION & WATERMARK CALCULATION TIER
│   └── Trigger: Periodic Consistency Audit Daemon Runs Every 60 Seconds
│       ├── Measurement Phase A: Temporal Lag (Replication Delay)
│       │   ├── Formula: `TemporalLag = CurrentTime - LatestEventTimestamp`
│       │   ├── Target: 99% of events projected in < 200ms
│       │   └── Breach: If `TemporalLag > 5000ms`, trigger warning alert
│       │
│       ├── Measurement Phase B: Sequence Number Drift (Version Gap)
│       │   ├── Query Write Model: `SELECT MAX(version) FROM orders WHERE id = $1` -> `WriteVer`
│       │   ├── Query Read Model: `GET /orders_view/_doc/$1._source.version` -> `ReadVer`
│       │   ├── Formula: `VersionDrift = WriteVer - ReadVer`
│       │   │   ├── Condition 1: `VersionDrift == 0` ──> State is 100% In-Sync (Consistent)
│       │   │   ├── Condition 2: `VersionDrift > 0 AND DriftAge < 10s` ──> Normal In-Flight Eventual Propagation
│       │   │   └── Condition 3: `VersionDrift > 0 AND DriftAge > 60s` ──> [STALLED DRIFT ANOMALY DETECTED]
│       │
│       └── Measurement Phase C: Total Row Count Parity
│           ├── Compare: Total active rows in PostgreSQL vs Total active documents in OpenSearch
│           └── Anomaly: Parity divergence > 0.01% triggers automated reconciliation
│
├── 2. RECONCILIATION & AUTO-HEALING ENGINE
│   └── Trigger: Stalled Drift Anomaly Confirmed on Aggregate ID
│       ├── Step 1: Query Authoritative Write Model State
│       │   └── Fetch latest complete aggregate record from PostgreSQL primary
│       │
│       ├── Step 2: Query Kafka Topic for Unconsumed Offsets
│       │   ├── Check: Is the missing event pending in Kafka partition queue?
│       │   │   ├── YES (Consumer Lag) ──> Allow consumer pipeline to complete catch-up
│       │   │   └── NO (Event Dropped / Consumer Skipped Offset):
│       │   │       └── Action: Trigger Direct Projection Healing
│       │
│       ├── Step 3: Direct Projection Healing
│       │   ├── Synthesize snapshot projection document from PostgreSQL write model
│       │   ├── Write directly to OpenSearch read store with optimistic version lock
│       │   ├── Metric: Increment `reconciliation_healed_entities_total`
│       │   └── Log: Emit structured audit log of reconciliation event
│
└── 3. OBSERVABILITY DASHBOARD & SLA REPORTING TIER
    └── Metrics Exposed to Prometheus:
        ├── `eventual_consistency_lag_seconds{view="orders"}`: Histogram of propagation delays
        ├── `eventual_consistency_drift_count{view="orders"}`: Gauge of currently out-of-sync entities
        ├── `eventual_consistency_reconciliation_total`: Counter of auto-healed aggregates
        │
        └── Alerting Rules:
            ├── P2 Alert: `eventual_consistency_lag_seconds{quantile="0.99"} > 5` for > 5 minutes
            └── P1 Alert: `eventual_consistency_drift_count > 100` sustained for > 15 minutes
```

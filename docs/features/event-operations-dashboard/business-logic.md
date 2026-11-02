# Event Operations Dashboard — Business Logic Decision Tree

```
EVENT OPERATIONS DASHBOARD (BUSINESS DECISION TREE)
│
├── 1. DASHBOARD COMPONENT TOPOLOGY & TELEMETRY SINK TIER
│   └── Trigger: SRE / Platform Engineer Opens Grafana Dashboard (:3000)
│       ├── Provisioned Data Source Routing:
│       │   ├── Source 1: Prometheus (:9090) ──> Real-time gauges, rates, latency histograms, error counters
│       │   ├── Source 2: Grafana Tempo (:3200) ──> End-to-end distributed trace spans and waterfall diagrams
│       │   ├── Source 3: Grafana Loki (:3100) ──> Structured log streams filtered by service, trace_id, level
│       │   └── Source 4: OpenSearch (:9200) ──> Event catalogue search, payload exploration, DLQ inspection
│       │
│       └── Dashboard Structure Hierarchy:
│           ├── Panel Group A: Platform High-Level Executive Overview (Throughput, Global Error Rate, SLA)
│           ├── Panel Group B: Kafka Cluster & Partition Health (Lag, Under-Replicated Partitions, Broker Load)
│           ├── Panel Group C: Consumer Group Metrics (Processing Latency, Rebalances, Batch Duration)
│           └── Panel Group D: Dead-Letter Queue & Reliability Triage (Poison Messages, Retry Count)
│
├── 2. OPERATIONAL DECISION & INCIDENT TRIAGE TREE
│   └── Trigger: Dashboard Visualizes SRE Metric Anomaly
│       ├── Anomaly 1: Red Spike on "Kafka Consumer Lag" (> 100,000 records)
│       │   ├── Step 1: Check "Consumer Processing Rate (events/sec)" vs "Producer Ingestion Rate"
│       │   │   ├── Condition: Consume Rate = 0 ──> [PANIC: Consumer group stalled or deadlocked]
│       │   │   │   └── Action: Check Loki logs for `OutOfMemoryError` or unhandled panic; restart deployment
│       │   │   └── Condition: Consume Rate > 0 but < Produce Rate ──> [Traffic Burst / Under-Provisioned]
│       │   │       └── Action: Check HPA status; verify if pod count scaled to maximum partition limit
│       │   └── Step 2: Check "Under-Replicated Partitions (URP)" Gauge
│       │       ├── URP > 0 ──> Broker node failure in Kafka cluster; trigger broker recovery runbook
│       │       └── URP == 0 ──> Kafka storage cluster healthy; issue is purely application consumer speed
│       │
│       ├── Anomaly 2: Surge in "Dead-Letter Queue (DLQ) Publish Rate"
│       │   ├── Step 1: Click DLQ panel to open correlated Loki logs filtered by `level="ERROR"`
│       │   ├── Step 2: Inspect `X-DLQ-Exception-Class` header distribution:
│       │   │   ├── Pattern A: `SchemaMismatchException` > 90% ──> Upstream producer deployed incompatible schema
│       │   │   │   └── Action: Rollback producer deployment or hotfix consumer schema version in Apicurio
│       │   │   └── Pattern B: `DownstreamTimeoutException` > 90% ──> Downstream third-party dependency outage
│       │   │       └── Action: Engage dependency vendor; pause DLQ redrive until recovery
│       │   └── Step 3: Trigger rate-limited redrive tool once root cause is remediated
│       │
│       └── Anomaly 3: Elevated "Materialization Propagation Lag" (> 10s)
│           ├── Inspect: Database write latency on OpenSearch / PostgreSQL read stores
│           ├── Check: CPU & Disk I/O utilization on OpenSearch data nodes
│           └── Resolution: Scale OpenSearch data nodes or increase bulk index flush intervals
│
└── 3. RUNBOOK AUTOMATION & REMEDIATION SHORTCUTS
    └── Quick-Action Integration Links embedded inside Grafana:
        ├── Action 1: "Pause Consumer Group" ──> Executes administrative API call to pause traffic safely
        ├── Action 2: "Trigger HPA Scale-Out" ──> Forces manual replica override for emergency draining
        └── Action 3: "Initiate Safe DLQ Redrive" ──> Launches CLI job with rate-limit and audit tracking
```

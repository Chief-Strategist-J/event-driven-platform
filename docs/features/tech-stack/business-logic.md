# Event-Driven Platform Technology Stack — Architecture & Runtime Decision Tree

```
EVENT-DRIVEN PLATFORM TECHNOLOGY STACK (ARCHITECTURE DECISION TREE)
│
├── 1. CLIENT INGRESS & SERVICE COMMUNICATION LAYER
│   ├── Component: Edge API Gateway
│   │   └── Choice: Envoy Proxy (v1.31)
│   │       ├── Rationale: HTTP/2 multiplexing, gRPC-Web / ConnectRPC bridging, dynamic rate limiting, circuit breaking
│   │       ├── Network Invariant: External entrypoint on :8080 (REST/HTTP) & :8443 (mTLS); Admin stats on :9901
│   │       └── Upstream Routing: Envoy clusters strictly route to backend microservices over internal Docker/K8s networks
│   │
│   └── Component: Service API Contract
│       └── Choice: ConnectRPC / REST (Go 1.23)
│           ├── Rationale: Type-safe Protobuf contracts, native HTTP/1.1 and HTTP/2 wire protocol without heavyweight gRPC proxies
│           ├── Port Allocation: :50051 (ConnectRPC primary), :8081 (Admin / Health / Metrics)
│           └── Protocol Handling: Protobuf binary payloads over wire; JSON fallback for debugging
│
├── 2. EVENT STREAMING, SCHEMA GOVERNANCE & CONTRACT EVOLUTION
│   ├── Component: Durable Event Distribution Backbone
│   │   └── Choice: Apache Kafka (v3.8+ KRaft Mode, ZooKeeper-free)
│   │       ├── Partitioning Strategy: Partition keys assigned strictly by aggregate_id (e.g., order_id, account_id)
│   │       ├── Replication Contract: RF=3, min.insync.replicas=2, producer acks=all, enable.idempotence=true
│   │       └── Port Mapping: Internal listener on kafka:9092; Host binding on 127.0.0.1:9092
│   │
│   ├── Component: Event Schema Management
│   │   └── Choice: Apicurio Registry (v2.6)
│   │       ├── Rationale: Centralized governance for Protobuf and Avro event contracts, backwards compatibility enforcement
│   │       ├── Port Allocation: Internal on apicurio:8080; Host loopback on 127.0.0.1:8082
│   │       └── CI/CD Gate: Schema compatibility checks run before producing breaking topic mutations
│   │
│   └── Component: Serialization Formats
│       └── Choice: Protocol Buffers (Protobuf v3) & Apache Avro
│           ├── Schema Rules: Mandatory envelope (event_id UUIDv7, aggregate_id, version, timestamp, traceparent)
│           └── Evolution Invariant: Non-breaking additive changes; breaking modifications mandate new versioned topic suffix
│
├── 3. PERSISTENCE, CACHING & DUAL-MODEL CQRS STORAGE
│   ├── Component: Command Store & Transactional Ledger (Write Model)
│   │   └── Choice: PostgreSQL 16 (Logical Replication Enabled)
│   │       ├── Rationale: Strict ACID transactions, foreign keys, deterministic concurrency via optimistic locking
│   │       ├── Engine Tuning: wal_level=logical, max_wal_senders=10, max_replication_slots=10
│   │       └── Port Mapping: Internal db:5432; Host loopback 127.0.0.1:5432
│   │
│   ├── Component: Change-Data Capture (CDC) Pipeline
│   │   └── Choice: Debezium Connect (v2.7)
│   │       ├── Rationale: Non-intrusive log-based capture streaming PostgreSQL WAL changes to Kafka outbox topics
│   │       ├── Port Mapping: debezium:8083 (REST API)
│   │       └── Delivery Guarantee: At-least-once streaming with consumer deduplication
│   │
│   ├── Component: Read Model & Search Projections (Query Model)
│   │   └── Choice: OpenSearch (v2.17)
│   │       ├── Rationale: Fast full-text search, aggregation-heavy projection queries decoupled from relational write load
│   │       ├── Port Mapping: opensearch:9200
│   │       └── Consistency Contract: Eventual consistency; projections rebuilt via Kafka offset reset
│   │
│   ├── Component: Ephemeral State, Rate Limiting & Distributed Coordination
│   │   └── Choice: Redis (v7.4)
│   │       ├── Rationale: Sub-millisecond read/write cache, atomic Lua scripting for idempotent deduplication windows
│   │       ├── Tuning: allkeys-lru memory policy, 200MB bounded cache
│   │       └── Port Mapping: cache:6379; Host loopback 127.0.0.1:6379
│   │
│   └── Component: Immutable Event Archive & Cold Storage
│       └── Choice: MinIO / AWS S3
│           ├── Rationale: Long-term S3-compliant cold event logs, batch analytical export, and compliance retention
│           └── Port Mapping: minio:9000 (S3 API), minio:9001 (Web Console)
│
├── 4. OBSERVABILITY, METRICS & DISTRIBUTED TRACING FABRIC
│   ├── Component: Telemetry Collection & Routing
│   │   └── Choice: OpenTelemetry Collector Contrib (v0.108)
│   │       ├── Ingestion: OTLP gRPC (:4317) and OTLP HTTP (:4318)
│   │       └── Processing: Batching, resource attribute enrichment, trace-to-metric span parsing
│   │
│   ├── Component: Metrics Engine
│   │   └── Choice: Prometheus (v2.54)
│   │       ├── Scrape Targets: OTel Collector Prometheus exporter (:8889), Go app metrics (:8081/metrics)
│   │       └── Port Mapping: prometheus:9090
│   │
│   ├── Component: Distributed Trace Storage
│   │   └── Choice: Grafana Tempo (v2.6) / Jaeger Compatible
│   │       ├── Storage: Local WAL and filesystem object backend
│   │       └── Ingestion Port: tempo:4317 (OTLP gRPC)
│   │
│   ├── Component: Structured Log Aggregation
│   │   └── Choice: Grafana Loki (v3.1)
│   │       ├── Ingestion: HTTP push from OTel Collector (:3100)
│   │       └── Storage: Filesystem TSDB chunk format
│   │
│   └── Component: Visualization & Alerting Dashboards
│       └── Choice: Grafana (v11.2)
│           ├── Provisioning: Automated mounting of Prometheus, Tempo, and Loki data sources
│           └── Port Mapping: grafana:3000
│
└── 5. ORCHESTRATION, GITOPS & RELIABILITY VERIFICATION TIER
    ├── Component: Container Build & Hardening
    │   └── Choice: Multi-Stage Docker (syntax=docker/dockerfile:1.7)
    │       ├── Base: Debian Bookworm slim with tini, CA certs, tzdata, UID 10001:10001
    │       └── Invariants: Zero compilers in runtime, pinned @sha256 digests, tmpfs for /tmp
    │
    ├── Component: Kubernetes Deployment & Package Management
    │   └── Choice: Kubernetes 1.30+ & Helm 3
    │       ├── Manifests: Deployment (PDB minAvailable: 1, HPA min: 2 max: 15), ClusterIP Service, Ingress
    │       └── Environment Overlays: Kustomize overlays for dev, staging, prod, and local-polyglot
    │
    ├── Component: GitOps Engine
    │   └── Choice: Argo CD
    │       ├── Declarations: AppProject with namespace whitelisting, ApplicationSets for multi-cluster environments
    │       └── Sync Contract: Automated sync, self-heal, prune enabled
    │
    ├── Component: Cloud Infrastructure as Code
    │   └── Choice: Terraform (Google Cloud Platform)
    │       └── Modules: VPC custom subnets, GKE private cluster, Cloud SQL Postgres logical replication, GCS archive
    │
    ├── Component: Automated CI/CD Pipeline
    │   └── Choice: GitHub Actions (.github/workflows/deploy.yml)
    │       └── Workflow Steps: Hadolint Dockerfile lint, Compose config verify, Testcontainers test, Buildx compile
    │
    ├── Component: Integration Testing
    │   └── Choice: Testcontainers for Go
    │       └── Test Scenarios: Spin up real PostgreSQL and Kafka KRaft containers, verify event produce/consume
    │
    ├── Component: Load & Latency Benchmarks
    │   └── Choice: k6
    │       └── Scenarios: Multi-stage throughput ramping, ConnectRPC POST requests, SLO threshold p(99) < 300ms
    │
    └── Component: Chaos Failure Injection
        └── Choice: LitmusChaos
            └── Experiments: Random pod-delete on Kafka StatefulSet brokers, measuring lag recovery & zero message loss
```

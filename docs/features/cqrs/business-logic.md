# CQRS — Business Logic Decision Tree

```
CQRS (BUSINESS DECISION TREE)
│
├── 1. COMMAND VS QUERY RESPONSIBILITY SEGREGATION TIER
│   └── Trigger: Client Initiates Inbound Request at API Gateway
│       ├── Check: Request Method & Mutation Semantics
│       │   ├── Branch A: State Mutation / Business Intent (COMMAND)
│       │   │   ├── Wire Protocol: ConnectRPC POST /orders.v1.OrderService/PlaceOrder
│       │   │   ├── Target Store: PostgreSQL Write Model (Normalized ACID schema)
│       │   │   ├── Execution:
│       │   │   │   ├── Step 1: Validate invariant constraints & business rules in memory
│       │   │   │   ├── Step 2: Acquire aggregate optimistic lock via `version`
│       │   │   │   ├── Step 3: Atomic write: Aggregate state + Outbox event row
│       │   │   │   └── Step 4: Return synchronous response: `{aggregate_id: "...", aggregate_version: 4}`
│       │   │   └── Invariant: Commands NEVER return projected query views; return aggregate ID + version ONLY
│       │   │
│       │   └── Branch B: State Retrieval / Dashboard Query (QUERY)
│       │       ├── Wire Protocol: ConnectRPC GET/POST or REST GET /api/v1/orders/{id}
│       │       ├── Target Store: OpenSearch Read Store OR Redis Cache (Denormalized Materialized View)
│       │       ├── Execution: Retrieve pre-computed document; zero table joins across normalized relational models
│       │       └── Invariant: Queries NEVER execute business mutations or write to outbox logs
│       │
│       └── Architectural Rule: Write model DB is strictly unreachable from public reporting / query endpoints
│
├── 2. ASYNCHRONOUS DATA PROPAGATION & WATERMARK TRACKING
│   └── Trigger: Outbox Relay Streams Command Event to Kafka Topic
│       ├── Downstream Projection Processor:
│       │   ├── Step 1: Consume event from `orders.events` topic
│       │   ├── Step 2: Apply state delta to OpenSearch index: `PUT /orders_view/_doc/<id>`
│       │   └── Step 3: Commit projection watermark: Record `projected_version = event.aggregate_seq`
│       │
│       └── Lag Latency Monitoring:
│           ├── Calculate: `PropagationDelay = Now() - event.occurred_at`
│           └── SLA Threshold: Alert if p99 propagation delay exceeds 2000ms
│
└── 3. READ-YOUR-WRITES CONSISTENCY CONTRACT
    └── Trigger: Client Submits Command, then Immediately Requests Query View (e.g. Refresh Page)
        ├── Problem: Asynchronous propagation means Query store may be slightly stale (Eventual Consistency)
        │
        ├── Client Synchronization Mechanism:
        │   ├── Command Response: Returns `aggregate_version = 4` to client
        │   ├── Subsequent Query: Client includes header `X-Min-Aggregate-Version: 4`
        │   │
        │   ├── Read Store Resolver Evaluation:
        │   │   ├── Check: Is `Current_Doc_Version >= X-Min-Aggregate-Version`?
        │   │   │   ├── YES ──> Return projected document immediately (Consistent Read)
        │   │   │   └── NO (Read Model Lagging by Few Milliseconds):
        │   │   │       ├── Action: Bounded Poll Wait (Wait up to 500ms with 25ms intervals)
        │   │   │       ├── Condition A: Watermark catches up ──> Return fresh document
        │   │   │       └── Condition B: 500ms Timeout Expired:
        │   │   │           ├── Fallback: Read directly from PostgreSQL Write Model primary record
        │   │   │           └── Metric: Increment `read_your_writes_timeout_fallback_total`
```

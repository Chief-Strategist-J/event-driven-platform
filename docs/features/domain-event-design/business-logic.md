# Domain Event Design — Business Logic Decision Tree

```
DOMAIN EVENT DESIGN (BUSINESS DECISION TREE)
│
├── 1. EVENT CLASSIFICATION & INTENT TIER
│   └── Trigger: Domain Aggregate State Transition or Business Milestone Achieved
│       ├── Check: What is the semantic classification of the domain occurrence?
│       │   ├── Branch A: Domain Event (Business Fact)
│       │   │   ├── Invariant: Past-tense naming (e.g., `OrderPlaced`, `PaymentSettled`, `InventoryAllocated`)
│       │   │   ├── Rule: Never use CRUD terms (`OrderCreated`, `UserUpdated`, `RecordDeleted` are BANNED)
│       │   │   └── Immutability: Once committed to WAL/log, can never be altered or retroactively erased
│       │   │
│       │   ├── Branch B: Ephemeral Command / Intent
│       │   │   └── Rejection: Commands must be directed synchronously via ConnectRPC/REST, NOT published as facts
│       │   │
│       │   └── Branch C: Telemetry / Operational Metric
│       │       └── Action: Route directly to OpenTelemetry Collector, NOT domain Kafka aggregate topics
│       │
│       └── Check: Granularity & Payload Weight Strategy
│           ├── Option 1: Event-Carried State Transfer (ECST / Fat Event)
│           │   ├── When: High-fanout downstream consumers requiring state without calling back to source DB
│           │   ├── Envelope Constraint: Must include aggregate snapshot delta + current version sequence
│           │   └── Risk Mitigation: PII fields masked or encrypted per field-level encryption policies
│           │
│           └── Option 2: Domain Notification (Thin Event)
│               ├── When: Sensitive core entities where downstream consumers must query authoritative API
│               └── Payload: Only contains aggregate identifiers, timestamp, event type, and causation ID
│
├── 2. ENVELOPE SPECIFICATION & SCHEMA COMPLIANCE TIER
│   └── Trigger: Producer Serializes Outbound Domain Event
│       ├── Header & Metadata Envelope Validation:
│       │   ├── `event_id`: UUIDv7 (Time-sortable, sub-millisecond precision, globally unique)
│       │   ├── `schema_version`: Semantic integer (e.g., `1`, `2`) corresponding to Schema Registry ID
│       │   ├── `aggregate_id`: Strongly typed string UUID partition routing key
│       │   ├── `aggregate_type`: Qualified domain aggregate name (e.g., `billing.invoice`)
│       │   ├── `aggregate_seq`: Monotonically increasing sequence number per aggregate instance
│       │   ├── `occurred_at`: ISO-8601 UTC timestamp of original domain fact creation
│       │   ├── `causation_id`: `event_id` or `command_id` directly triggering this transition
│       │   ├── `correlation_id`: End-to-end distributed transaction / user request flow ID
│       │   ├── `tenant_id`: Multi-tenant isolation partition boundary
│       │   └── `trace_context`: W3C traceparent and tracestate for OpenTelemetry propagation
│       │
│       ├── Validation Gate: Does event adhere strictly to Canonical Envelope Schema?
│       │   ├── NO ──> [FATAL: Reject Event at Producer Barrier before Network Serialization]
│       │   └── YES ──> Proceed to Schema Registry Fingerprint Validation
│       │
│       └── Serialization Encoding:
│           ├── Production Standard: Protocol Buffers (Protobuf v3) or Apache Avro binary encoding
│           └── Magic Byte Injection: 5-byte Confluent/Apicurio wire prefix (1 byte magic + 4 byte schema ID)
│
└── 3. CAUSALITY, IDEMPOTENCY & RECOVERY TIER
    └── Trigger: Outbox Relay or Direct Producer Submits Event to Kafka Broker
        ├── Concurrency Guard: Optimistic Concurrency Control Check
        │   ├── Condition: Current aggregate_seq matches expected DB version
        │   │   ├── YES ──> Persist event to Transactional Outbox atomically with aggregate state
        │   │   └── NO ──> [ABORT: Concurrency Conflict 409 — Reject stale concurrent modification]
        │   │
        │   └── Kafka Producer Delivery Configuration:
        │       ├── `enable.idempotence=true`
        │       ├── `acks=all` (wait for all in-sync replicas)
        │       ├── `max.in.flight.requests.per.connection <= 5` (guarantees per-partition ordering)
        │       └── `retries=MAX_INT` with delivery timeout bounded
        │
        └── Downstream Consumption Contract:
            ├── Consumer verifies `event_id` against deduplication table
            ├── Gaps in `aggregate_seq` trigger out-of-order quarantine or buffer-and-wait timeout
            └── Duplicate `event_id` silently acknowledged without executing side-effects
```

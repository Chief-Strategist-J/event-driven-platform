# Event-Driven Communication — Business Logic Decision Tree

```
EVENT-DRIVEN COMMUNICATION (BUSINESS DECISION TREE)
│
├── 1. INTER-SERVICE COMMUNICATION PATTERN SELECTION
│   └── Trigger: Service A Needs to Interact with Service B
│       ├── Check: What is the temporal and consistency requirement of the interaction?
│       │   ├── Branch A: Query for Real-Time State (Synchronous Read)
│       │   │   └── Pattern: ConnectRPC over HTTP/2 with strict timeout & deadline propagation
│       │   │       ├── Rationale: Low latency, strong consistency, caller blocks for result
│       │   │       └── Circuit Breaker: Trip on 5 consecutive timeouts; return cached fallback or error
│       │   │
│       │   ├── Branch B: State-Changing Mutation with Downstream Side-Effects (Asynchronous Fact)
│       │   │   └── Pattern: Event-Driven Publish/Subscribe over Apache Kafka
│       │   │       ├── Rationale: Decoupled availability; producer succeeds even if consumer is down
│       │   │       └── Guarantee: At-least-once delivery with eventual consistency
│       │   │
│       │   └── Branch C: Multi-Step Distributed Business Transaction
│       │       └── Pattern: Orchestrated or Choreographed Saga
│       │           ├── Happy Path: Events trigger forward steps sequentially
│       │           └── Failure Path: Compensating events triggered in reverse order to rollback state
│       │
│       └── Validation: Is synchronous REST/gRPC being used for cross-domain notifications?
│           ├── YES ──> [VIOLATION: Anti-pattern creates temporal coupling; refactor to domain events]
│           └── NO ──> Architecture compliant
│
├── 2. PRODUCER TOPOLOGY & DELIVERY GUARANTEE TIER
│   └── Trigger: Producer Publishes Event to Kafka
│       ├── Step 1: Local Transactional Boundary
│       │   └── Atomic Action: Insert business record + outbox event in same DB transaction
│       │
│       ├── Step 2: Reliable Log Transmission (Outbox Relay)
│       │   ├── Producer Settings:
│       │   │   ├── `acks=all` (All in-sync replicas must acknowledge receipt)
│       │   │   ├── `enable.idempotence=true` (Kafka broker eliminates duplicate PID/Sequence batches)
│       │   │   ├── `max.in.flight.requests.per.connection=5`
│       │   │   ├── `compression.type=zstd` (Optimal balance of ratio and CPU overhead)
│       │   │   └── `linger.ms=5` + `batch.size=65536` (Micro-batching for throughput)
│       │   │
│       │   └── Broker Acknowledgement Check:
│       │       ├── Success ──> Mark outbox row processed or advance WAL slot
│       │       └── Broker Failure (LeaderNotAvailable / Timeout):
│       │           ├── Retry with Exponential Backoff + Jitter
│       │           └── Delivery Timeout Exceeded ──> Alert on-call; do not drop message
│       │
│       └── Distributed Context Propagation:
│           ├── Extract W3C Trace Context from current OpenTelemetry span
│           └── Inject `traceparent` and `tracestate` into Kafka Record Headers
│
└── 3. ASYNCHRONOUS CONSUMER PIPELINE & ERROR RESOLUTION
    └── Trigger: Consumer Group Member Receives Message Batch
        ├── Step 1: Trace Context Extraction
        │   └── Action: Restore OpenTelemetry Span context from Record Headers for distributed tracing
        │
        ├── Step 2: Idempotency Barrier Check
        │   ├── Query local deduplication store using `event_id`
        │   ├── Already Processed ──> Acknowledge offset immediately; skip duplicate execution
        │   └── New Message ──> Proceed to Domain Event Handler
        │
        ├── Step 3: Domain Event Processing
        │   ├── Success ──> Commit Kafka offset manually (NO auto-commit)
        │   ├── Transient Failure (DB connection drop, network blip):
        │   │   └── Action: Pause partition consumption; retry with exponential backoff
        │   └── Permanent Poison Failure (Malformed business logic / validation):
        │       └── Action: Route to Dead-Letter Queue (DLQ) with error metadata; commit offset
```

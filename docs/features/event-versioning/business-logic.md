# Event Versioning — Business Logic Decision Tree

```
EVENT VERSIONING (BUSINESS DECISION TREE)
│
├── 1. VERSION TAXONOMY & IDENTIFIER CONVENTIONS
│   └── Trigger: Defining Schema Versioning Contract for Event Payloads
│       ├── Hierarchy Tier 1: Domain-Level Versioning
│       │   ├── Rule: Bounded context major changes reflect in topic naming
│       │   └── Pattern: `<domain>.<aggregate>.<event>.v<Major>` (e.g., `orders.order.placed.v1`)
│       │
│       ├── Hierarchy Tier 2: Schema-Level Versioning
│       │   ├── Rule: Registered inside Apicurio Schema Registry metadata
│       │   ├── Metadata Field: `schema_version` (Monotonically incrementing integer: 1, 2, 3...)
│       │   └── Envelope Field: `version` in message payload header for fast client routing
│       │
│       └── Hierarchy Tier 3: Aggregate Sequence Versioning
│           ├── Rule: Per-aggregate instance monotonic counter (`aggregate_seq: 1, 2, 3...`)
│           └── Guarantee: Strictly orders state transitions of a single aggregate ID
│
├── 2. VERSION UPCASTING & STATE PROJECTION TIER
│   └── Trigger: Replay Consumer or Projection Builder Encounters Legacy Event
│       ├── Check: What is the event payload version?
│       │   ├── Case A: Current Version (e.g. v3)
│       │   │   └── Action: Pass directly to projection event handler
│       │   │
│       │   └── Case B: Historical Version (e.g. v1 or v2)
│       │       ├── Action: Route through In-Memory Upcaster Pipeline
│       │       │   ├── Step 1: UpcasterV1toV2.Transform(eventV1) -> intermediateEventV2
│       │       │   │   └── Injection: Add missing default fields (e.g., currency_code = 'USD')
│       │       │   ├── Step 2: UpcasterV2toV3.Transform(eventV2) -> currentEventV3
│       │       │   │   └── Transformation: Restructure nested address object into normalized format
│       │       │   └── Step 3: Pass standardized v3 event to projection handler
│       │       │
│       │       └── Invariant: Original immutable event in Kafka / Event Store remains untouched
│       │
│       └── Validation: Is there an unbroken upcasting path from v1 to Current?
│           ├── YES ──> Event successfully projected into latest read model
│           └── NO (Missing Upcaster Link) ──> [FATAL: Halt consumer group; emit critical alert]
│
└── 3. DEPRECATION & SUNSET LIFECYCLE TIER
    └── Trigger: Deprecating an Outdated Event Version
        ├── Phase 1: Mark Deprecated in Registry
        │   ├── Action: Set `deprecated: true` in Apicurio Schema metadata
        │   └── Notification: Broadcast deprecation warning to all downstream consumer owners
        │
        ├── Phase 2: Dual Publishing Observation Window
        │   ├── Monitoring: Track consumer lag and active client connections to legacy topics
        │   └── Condition: Active consumer count reaches zero across all production environments
        │
        └── Phase 3: Archive & Purge
            ├── Step 1: Cease emitting legacy version from producer outbox
            ├── Step 2: Archive Kafka topic to MinIO/S3 immutable Parquet/Avro bucket
            └── Step 3: Soft-delete schema version in Apicurio Registry
```

# Schema Evolution — Business Logic Decision Tree

```
SCHEMA EVOLUTION (BUSINESS DECISION TREE)
│
├── 1. EVOLUTION VALIDATION & CHANGE CLASSIFICATION TIER
│   └── Trigger: Engineer Proposes Event Schema Modification
│       ├── Check: What type of change is being introduced?
│       │   ├── Category A: Safe Non-Breaking Change (Additive)
│       │   │   ├── Condition: New optional field with non-null default value
│       │   │   ├── Action: Permitted under BACKWARD and FULL compatibility
│       │   │   └── Procedure: Register new version under same subject; consumers ignore new fields until updated
│       │   │
│       │   ├── Category B: Safe Non-Breaking Deletion
│       │   │   ├── Condition: Removing a field that was previously optional
│       │   │   ├── Invariant: Older versions must have had defaults defined
│       │   │   └── Action: Deprecate field first; remove only after grace window
│       │   │
│       │   └── Category C: Breaking Structural Mutation
│       │       ├── Condition: Renaming existing field, changing data type, removing required field
│       │       ├── Action: FORBIDDEN under current Subject / Topic
│       │       └── Resolution: Trigger Dual-Publishing & Upcaster Strategy (Tier 2)
│       │
│       └── Validation Tooling: Run Protobuf / Avro Schema Compatibility Linter in CI
│           ├── Linter Fails ──> Block pull request merge immediately
│           └── Linter Passes ──> Register schema candidate in Apicurio Staging Registry
│
├── 2. BREAKING CHANGE TRANSITION STRATEGY (DUAL-PUBLISH & UPCASTING)
│   └── Trigger: Domain Aggregate Undergoes Incompatible Redesign
│       ├── Step 1: Topic Version Forking
│       │   ├── Create new versioned Kafka topic: `<domain>.<aggregate>.<event>.v2`
│       │   └── Register v2 schema under new independent Subject in Apicurio
│       │
│       ├── Step 2: Dual-Publishing Window (Producer Side)
│       │   ├── Producer emits both v1 payload to `.v1` topic AND v2 payload to `.v2` topic
│       │   ├── Consistency Contract: Published in same transactional outbox unit of work
│       │   └── Sunset Timeline: Set TTL deadline (e.g. 30 days) for all consumers to migrate
│       │
│       ├── Step 3: Consumer Migration & Upcasting Pipeline
│       │   ├── Modern Consumers: Switch consumer group subscription to `.v2` topic
│       │   └── Historical Replay Engine: Upcaster Chain Implementation
│       │       ├── When replaying historical events from offset 0:
│       │       │   ├── Read raw event from `.v1` topic
│       │       │   ├── Apply `UpcastV1ToV2(eventV1) -> eventV2` transformation function in memory
│       │       │   └── Feed upcasted v2 model into state projection engine
│       │       └── Invariant: Never overwrite immutable raw storage log during upcasting
│       │
│       └── Step 4: Decommissioning v1 Topic
│           ├── Audit: Verify zero active consumer lag on `.v1` topic for 7 consecutive days
│           └── Action: Terminate dual-publishing in producer; archive `.v1` topic to MinIO/S3 cold storage
│
└── 3. TOLERANT READER PATTERN (CONSUMER RESILIENCE)
    └── Trigger: Downstream Consumer Parses Event Message
        ├── Structural Tolerant Reader Principles:
        │   ├── Principle 1: Consumer parses ONLY the fields it strictly needs for its projection
        │   ├── Principle 2: Unknown new fields encountered during deserialization are ignored, not faulted
        │   └── Principle 3: Missing optional fields fall back gracefully to domain defaults
        │
        └── Deserialization Exception Handling:
            ├── Incompatible Payload Encountered (Malformed contract):
            │   ├── Action: Quarantine message to Dead-Letter Queue with raw byte payload
            │   ├── Metric: Increment `schema_evolution_deserialization_error_total`
            │   └── Notification: Page owning service team via PagerDuty/Slack
            └── Consumer Continues Processing Next Offset
```

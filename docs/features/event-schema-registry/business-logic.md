# Event Schema Registry — Business Logic Decision Tree

```
EVENT SCHEMA REGISTRY (BUSINESS DECISION TREE)
│
├── 1. SCHEMA REGISTRATION & ARTIFACT DISCOVERY TIER
│   └── Trigger: CI/CD Build or Producer Startup Submits Schema Definition
│       ├── Check: Schema Artifact Subject Naming Strategy
│       │   ├── Strategy 1: TopicNameStrategy (`<topic-name>-value` / `<topic-name>-key`)
│       │   │   └── Standard: Used for single-aggregate dedicated Kafka topics
│       │   ├── Strategy 2: RecordNameStrategy (`<namespace>.<RecordName>`)
│       │   │   └── Usage: Used when polyglot events share a single event router topic
│       │   └── Strategy 3: TopicRecordNameStrategy (`<topic-name>-<RecordName>`)
│       │       └── Usage: Strict multitenant or multiplexed topics requiring explicit binding
│       │
│       ├── Action: Query Apicurio Registry for Schema Hash / Fingerprint
│       │   ├── Check: Does exact schema hash already exist under Subject?
│       │   │   ├── YES ──> Return existing schema ID (Cached in Producer memory)
│       │   │   └── NO ──> Trigger Compatibility Enforcement Engine (Tier 2)
│       │   └── Failure Scenario: Apicurio Schema Registry Unreachable
│       │       ├── In-Memory Local Cache Hit ──> Use cached schema ID and continue producing
│       │       └── Cache Miss ──> [HALT: Block producer serialization; never produce unverified payloads]
│
├── 2. COMPATIBILITY RULE ENFORCEMENT TIER
│   └── Trigger: New Schema Version Proposed for Registration
│       ├── Compatibility Mode Evaluator:
│       │   ├── Mode A: BACKWARD (Default for Event Sourcing Consumers)
│       │   │   ├── Invariant: New schema can read data written by previous schema version
│       │   │   └── Allowed: Add optional fields with default values; delete non-required fields
│       │   │
│       │   ├── Mode B: BACKWARD_TRANSITIVE
│       │   │   ├── Invariant: New schema can read data written by ALL historical schema versions
│       │   │   └── Requirement: Essential for Event Sourcing replays from offset 0
│       │   │
│       │   ├── Mode C: FORWARD / FORWARD_TRANSITIVE
│       │   │   ├── Invariant: Older consumer versions can read data written by new schema
│       │   │   └── Usage: Staged consumer rollouts where producers upgrade first
│       │   │
│       │   └── Mode D: FULL_TRANSITIVE (Gold Standard)
│       │       └── Invariant: Both old and new consumers can read old and new payloads interchangeably
│       │
│       ├── Compatibility Verification Execution:
│       │   ├── Evaluation Result: INCOMPATIBLE (e.g. Renamed required field, removed default value)
│       │   │   └── Action: Apicurio Registry rejects schema with HTTP 409 Conflict
│       │   │       └── CI/CD Enforcement: PR build fails; deployment blocked
│       │   │
│       │   └── Evaluation Result: COMPATIBLE
│       │       └── Action: Allocate new global schema ID, increment version integer (vN+1), return ID
│
└── 3. RUNTIME WIRE FORMAT & CLIENT DESERIALIZATION TIER
    └── Trigger: Consumer Polls Kafka Message Batch
        ├── Header Inspection: Extract First 5 Magic Bytes
        │   ├── Byte 0: Must equal 0x00 (Confluent / Apicurio wire standard indicator)
        │   ├── Bytes 1-4: 32-bit big-endian integer representing Schema ID
        │   │
        │   ├── Action: Fetch Schema Definition by ID from Local Memory Cache
        │   │   ├── Cache Hit ──> Retrieve Proto/Avro deserializer instance
        │   │   └── Cache Miss ──> Fetch from Apicurio Registry REST API & cache locally
        │   │
        │   └── Deserialization Execution:
        │       ├── Success ──> Emit Strongly-Typed Go Event Struct to Consumer Pipeline
        │       └── Poison Message / Schema Deserialization Failure:
        │           ├── Root Cause: Corrupted payload, malformed magic byte, or unknown schema ID
        │           ├── Action: DO NOT CRASH CONSUMER LOOP
        │           └── Resolution: Route raw message + stack trace directly to Dead-Letter Queue (DLQ)
```

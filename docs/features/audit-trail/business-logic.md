# Audit Trail — Business Logic Decision Tree

```
AUDIT TRAIL (BUSINESS DECISION TREE)
│
├── 1. AUDIT RECORD INGESTION & CLASSIFICATION TIER
│   └── Trigger: Security Event, Administrative Action, or Financial Mutation Occurs
│       ├── Check: What is the compliance classification of the action?
│       │   ├── Class A: Financial Transaction (e.g. Ledger transfer, refund, fee adjustment)
│       │   │   └── Compliance: SOC2 / PCI-DSS / SOX strict immutability standard
│       │   ├── Class B: Security & Identity Event (e.g. Login, role escalation, API key created)
│       │   │   └── Compliance: ISO 27001 / Zero-Trust access verification
│       │   └── Class C: Schema & Topology Mutation (e.g. Topic altered, DLQ purged)
│       │       └── Compliance: Administrative change management governance
│       │
│       ├── Envelope Standardization:
│       │   ├── `audit_id`: Monotonic UUIDv7
│       │   ├── `actor`: `{id: "usr_123", type: "USER"|"SERVICE", ip: "192.168.1.5", user_agent: "..."}`
│       │   ├── `action`: Past-tense qualified string (e.g. `iam.role.assigned`, `orders.order.cancelled`)
│       │   ├── `target`: `{type: "order", id: "ord_994"}`
│       │   ├── `state_diff`: `{before: {...}, after: {...}}` (PII scrubbed)
│       │   ├── `reason`: Mandatory text justification for administrative overrides
│       │   └── `timestamp`: Authoritative UTC NTP-synchronized timestamp
│       │
│       └── Invariant: Audit records are strictly append-only; UPDATE and DELETE verbs are physically revoked
│
├── 2. WRITE INTEGRITY & CRYPTOGRAPHIC CHAINING TIER
│   └── Trigger: Audit Record Prepared for Durability Sink
│       ├── Cryptographic Tamper-Evidence Protocol:
│       │   ├── Formula: `Record_Hash = SHA256(Previous_Record_Hash || Audit_Payload_Bytes || Salt)`
│       │   ├── Block Chain: Every audit record references hash of immediately preceding record
│       │   └── Verification: Periodic audit daemon recalculates hash chain from offset 0
│       │       ├── Chain Intact ──> Audit log integrity certified
│       │       └── Hash Mismatch ──> [SECURITY INCIDENT: Data tampering / unauthorized DB mutation detected]
│       │
│       └── Multi-Tier Storage Topology:
│           ├── Tier 1 (Hot Operational Store): PostgreSQL `audit_log` table (Queryable via API, 90-day retention)
│           ├── Tier 2 (Event Streaming Bus): Kafka topic `audit.events` (Replicated to SIEM / Splunk / Datadog)
│           └── Tier 3 (Cold Compliance Archive): MinIO / AWS S3 Glacier with Object Lock (WORM - Write Once Read Many)
│               ├── Object Lock Mode: COMPLIANCE mode (Cannot be deleted even by root account)
│               └── Retention Period: Mandatory 7 Years (2555 Days)
│
└── 3. QUERYING, FORENSICS & COMPLIANCE ACCESS CONTROL
    └── Trigger: Auditor or Security Engineer Queries Audit Trail
        ├── Access Control Verification:
        │   ├── Principle: Dual-authorization / Four-Eyes Principle for raw audit extraction
        │   ├── Role Check: Requires `role:compliance_auditor`
        │   └── Log Inspection: The query itself is logged as an immutable audit record (`audit.log.queried`)
        │
        └── Query Execution:
            ├── Filter by Aggregate: `SELECT * FROM audit_log WHERE target_id = $1 ORDER BY timestamp ASC`
            ├── Filter by Actor: `SELECT * FROM audit_log WHERE actor_id = $1 AND timestamp BETWEEN $2 AND $3`
            └── Forensic Export: Generates signed, tamper-evident JSON-LD bundle with SHA-256 manifest
```

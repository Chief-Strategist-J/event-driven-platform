# Dead-Letter Handling — Business Logic Decision Tree

```
DEAD-LETTER HANDLING (BUSINESS DECISION TREE)
│
├── 1. QUARANTINE INGESTION & METADATA PRESERVATION
│   └── Trigger: Message Exhausts Retry Budget OR Fails Deserialization
│       ├── Action: Publish to `<topic-name>.dlq` Topic
│       │
│       ├── Mandatory Diagnostic Header Enrichment:
│       │   ├── `X-DLQ-Original-Topic`: Original domain topic name
│       │   ├── `X-DLQ-Original-Partition`: Source partition ID
│       │   ├── `X-DLQ-Original-Offset`: Exact source offset for log tracing
│       │   ├── `X-DLQ-Failed-At`: ISO-8601 UTC timestamp of failure
│       │   ├── `X-DLQ-Exception-Class`: Exception name (e.g., `SchemaMismatchException`)
│       │   ├── `X-DLQ-Exception-Message`: Detailed error string
│       │   ├── `X-DLQ-Stacktrace`: Formatted stack trace up to 2KB
│       │   └── `X-DLQ-Retry-Attempts`: Total attempts prior to quarantine
│       │
│       └── Commit Source Offset:
│           ├── Invariant: Commit main/retry partition offset immediately after DLQ publish succeeds
│           └── Result: Main pipeline unblocked; zero head-of-line blocking
│
├── 2. TRIAGE, CLASSIFICATION & ALERTING TIER
│   └── Trigger: DLQ Producer Records Metric `dlq_messages_produced_total`
│       ├── Alerting Rules:
│       │   ├── Warning Alert: Single DLQ event emitted ──> Post diagnostic summary to Slack channel
│       │   └── Critical PagerDuty Alert: DLQ message rate > 10 events/min ──> Page on-call immediately
│       │
│       ├── Automated Triage Classification:
│       │   ├── Classification 1: Schema Incompatibility / Poison Pill
│       │   │   └── Root Cause: Producer published malformed contract unreadable by consumers
│       │   │   └── Remediation: Hotfix consumer schema upcaster; re-drive message
│       │   │
│       │   ├── Classification 2: Upstream Partner Down (> 1 hour)
│       │   │   └── Root Cause: Third-party payment gateway or KYC vendor outage
│       │   │   └── Remediation: Hold in DLQ until partner recovers; bulk re-drive
│       │   │
│       │   └── Classification 3: Data Integrity / Corrupt Payload
│       │       └── Action: Mark permanently non-retryable; log forensic record to cold storage
│
└── 3. DLQ REDRIVE & OPERATIONAL RUNBOOK TIER
    └── Trigger: Operator or Automated Tool Initiates DLQ Redrive
        ├── Safety Pre-Flight Checks:
        │   ├── Check 1: Has the root-cause bug in consumer code been patched and deployed?
        │   ├── Check 2: Are target downstream services healthy and capacity available?
        │   └── Check 3: Is a rate-limiter configured on the redrive tool to prevent consumer flooding?
        │
        ├── Redrive Execution Modes:
        │   ├── Mode A: Filtered Single-Message Redrive
        │   │   ├── Command: `dlq-tool redrive --dlq-topic orders.dlq --event-id <uuid> --rate-limit 10/s`
        │   │   └── Verification: Confirm event processes successfully into target read store
        │   │
        │   └── Mode B: Batch Redrive
        │       ├── Target: Re-inject messages back into `.retry.1m` or main topic
        │       └── Invariant: Messages retain original `event_id` so idempotent consumers absorb safely
        │
        └── Deletion / Dead-End Policy:
            ├── DLQ Retention: Set to 30 days (`retention.ms=2592000000`)
            └── Archival: Messages exceeding retention automatically archived to MinIO/S3 cold storage
```

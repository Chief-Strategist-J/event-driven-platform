# Retry Topics — Business Logic Decision Tree

```
RETRY TOPICS (BUSINESS DECISION TREE)
│
├── 1. ERROR CLASSIFICATION & RETRY ELIGIBILITY TIER
│   └── Trigger: Consumer Encountered Exception during Event Processing
│       ├── Check: What is the nature of the encountered failure?
│       │   ├── Category A: Transient Failure (Retryable)
│       │   │   ├── Conditions: Downstream DB network timeout, HTTP 503, Redis connection drop, lock contention
│       │   │   └── Decision: ELIGIBLE FOR RETRY ──> Route to Tiered Retry Pipeline
│       │   │
│       │   ├── Category B: Permanent Business Logic Failure (Non-Retryable)
│       │   │   ├── Conditions: Invalid schema, negative account balance, missing required entity, validation error
│       │   │   └── Decision: NOT RETRYABLE ──> Route immediately to Dead-Letter Queue (DLQ)
│       │   │
│       │   └── Category C: Fatal System Bug / Code Crash
│       │       └── Action: Quarantine record to DLQ; page on-call engineering
│       │
│       └── Invariant: Main consumer thread MUST NEVER block waiting on long sleeps (destroys partition throughput)
│
├── 2. TIERED RETRY DELAY TOPIC TOPOLOGY
│   └── Trigger: Transient Error Routed to Retry Infrastructure
│       ├── Staged Delay Architecture:
│       │   ├── Tier 1 Topic: `<topic>.retry.5s` (Delay: 5 Seconds)
│       │   ├── Tier 2 Topic: `<topic>.retry.1m` (Delay: 1 Minute)
│       │   ├── Tier 3 Topic: `<topic>.retry.10m` (Delay: 10 Minutes)
│       │   └── Final Tier Topic: `<topic>.dlq` (Poison Storage)
│       │
│       ├── Routing Execution:
│       │   ├── Read record headers: Extract `X-Retry-Count` (Default 0)
│       │   ├── Increment: `X-Retry-Count = X-Retry-Count + 1`
│       │   ├── Inject Headers: `X-Original-Topic`, `X-Original-Partition`, `X-Original-Offset`, `X-Exception-Message`
│       │   │
│       │   ├── Evaluation:
│       │   │   ├── If count == 1 ──> Publish to `.retry.5s` topic; commit main partition offset
│       │   │   ├── If count == 2 ──> Publish to `.retry.1m` topic; commit retry-1 offset
│       │   │   ├── If count == 3 ──> Publish to `.retry.10m` topic; commit retry-2 offset
│       │   │   └── If count > 3  ──> Exhausted budget ──> Publish to `.dlq` topic
│       │
│       └── Delayed Consumer Implementation:
│           ├── Consumer polls `.retry.<delay>` topic
│           ├── Check: `Now() - record.Timestamp >= TargetDelay`
│           │   ├── NO ──> `consumer.Pause(partition)`; sleep remaining delta; `consumer.Resume()`
│           │   └── YES ──> Process record through business handler
│
└── 3. NON-BLOCKING THROUGHPUT GUARANTEES
    ├── Core Principle: Poison / slow messages on aggregate X do not block aggregate Y on main topic
    ├── Tradeoff Awareness: Ordering across different aggregates is relaxed in retry topics; aggregate-level sequence preserved via partition keys
    └── Success Metric: Over 98% of transient blips resolve in Tier 1 (`.retry.5s`) without human intervention
```

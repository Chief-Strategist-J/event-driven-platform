# Replay Mechanism — Business Logic Decision Tree

```
REPLAY MECHANISM (BUSINESS DECISION TREE)
│
├── 1. REPLAY INTENT & BOUNDARY DEFINITION TIER
│   └── Trigger: Business Requires Event Stream Replay
│       ├── Valid Replay Scenarios:
│       │   ├── Scenario A: Projection Model Bug Fix (Rebuild state store with corrected logic)
│       │   ├── Scenario B: New Read Model Introduction (Hydrate brand new OpenSearch view from history)
│       │   ├── Scenario C: Security Audit & State Reconstruction (Forensic rewind to exact point-in-time)
│       │   └── Scenario D: Disaster Recovery (Cold-start hydration after catastrophic database loss)
│       │
│       └── Replay Scope Boundaries:
│           ├── Dimension 1: Global Topic Replay (Offset 0 to Current Log End)
│           ├── Dimension 2: Bounded Time Window (`startTime <= event_time <= endTime`)
│           └── Dimension 3: Single Aggregate Replay (`WHERE aggregate_id = ? ORDER BY seq ASC`)
│
├── 2. REPLAY ISOLATION & SIDE-EFFECT SUPPRESSION TIER
│   └── Trigger: Initializing Replay Consumer Pipeline
│       ├── Invariant 1: Dedicated Replay Consumer Group
│       │   ├── Naming: `replay-<feature>-<timestamp>` (e.g. `replay-orders-view-20261102`)
│       │   └── Rule: NEVER use active live production consumer group for replay (prevents production rebalance)
│       │
│       ├── Invariant 2: Blue/Green Read Store Isolation
│       │   ├── Live Read Table: `orders_view_v1` (Serves active user traffic)
│       │   ├── Shadow Replay Table: `orders_view_v2` (Target for replay writes)
│       │   └── Cutover Procedure: Build shadow table to 100% parity, then execute atomic alias swap
│       │
│       └── Invariant 3: External Side-Effect Suppression Flag
│           ├── Context Header Check: `IsReplay == true`
│           │   ├── Rule: External emails, SMS, webhooks, and payment charges MUST BE SUPPRESSED
│           │   └── Allowed Actions: State mutations inside shadow projection ONLY
│           └── Violation: Replaying without side-effect suppression causes duplicate customer charges!
│
└── 3. REPLAY EXECUTION, UPCASTING & PARITY CUTOVER
    └── Trigger: Replay Consumer Begins Streaming Historical Records
        ├── Step 1: Upcasting Pipeline Execution
        │   ├── As historical events are consumed:
        │   ├── Apply in-memory upcasters: `v1 -> v2 -> v3`
        │   └── Project normalized current state into `orders_view_v2`
        │
        ├── Step 2: Catch-Up Synchronization
        │   ├── Replay reaches real-time log head (Lag approaches 0)
        │   └── Verify projection parity between live write-model DB and new read model
        │
        └── Step 3: Zero-Downtime Atomic Cutover
            ├── Relational DB: Rename tables inside single transaction:
            │   ```sql
            │   BEGIN;
            │   ALTER TABLE orders_view_v1 RENAME TO orders_view_v1_backup;
            │   ALTER TABLE orders_view_v2 RENAME TO orders_view;
            │   COMMIT;
            │   ```
            ├── OpenSearch: Update index alias pointing to new index version
            └── Post-Action: Terminate replay consumer group; drop old backup table after 48h
```

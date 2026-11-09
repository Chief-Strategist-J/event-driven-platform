# Architectural Decision Record: Historical Stream Replay Engine & Side-Effect Control (WP-9)

## 1. Title & Metadata
* **Status**: APPROVED
* **Date**: 2026-11-05
* **Decision Makers**: Platform Architecture Guild, Chief Distributed Systems Architect
* **Governing Specification**: [Replay Mechanism Production System Design Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/replay-mechanism/business-logic.md)

---

## 2. Context & Problem Statement
In event-sourced and event-driven architectures, historical event streams represent the immutable ledger of truth. However, replaying historical messages through existing business consumer logic introduces severe platform hazards:

1. **Production Group Rebalance & Live Partition Freeze (F6.1)**: If a replay task joins the live consumer group or reads with an existing group ID, Kafka triggers stop-the-world partition rebalances, evicting live pods and corrupting real-time event streaming.
2. **Duplicate Side-Effect Disasters (F7.2, F7.3)**: Handlers frequently fire external side-effects (e.g. charging credit cards via Stripe, sending emails via SES, emitting outbox events to downstream microservices). Replaying 6 months of historical data naively re-executes these actions, producing catastrophic financial and operational liabilities.
3. **State Corruption via Non-Deterministic Handlers (F8.1, F8.2)**: Handlers generating random UUIDs or evaluating `time.Now()` at replay time generate state projections with diverging IDs and false "expired" statuses that differ from original historical reality.
4. **Data Loss from Broken Replay-to-Kafka Handoffs (F3.4)**: When replaying beyond Kafka log retention via cold object storage archives, failing to verify retention margins before handing off to live partitions creates unfillable sequence gaps.
5. **Lock Contention During Live Cutover (F12.1)**: Swapping shadow read models under live high-throughput traffic using long table locks causes database lock queues to cascade into platform-wide API outages.

---

## 3. Decision Drivers & Architecture Invariants (RP-01 to RP-12)

* **RP-01: Replay Never Touches Production Groups or Stores [Certain]**: Orchestration strictly rejects manifests targeting live group IDs or active production tables. Every replay run executes with an ephemeral group ID (`replay.<target>.<timestamp>`) and writes to isolated shadow tables or indices.
* **RP-02: Effects Fail Closed [Certain]**: The Effect Gateway (`C-EFG`) strictly suppresses all external mutations (payments, emails, webhooks, downstream outbox events) unless the run mode is `LIVE` or the effect is explicitly permitted.
* **RP-03: One Effect Path [Certain]**: Handlers possess zero direct network clients for external providers. All outbound mutations are evaluated through the centralized Effect Gateway interface.
* **RP-04: Replay Coverage is Proven, Not Assumed [Certain]**: Before historical streaming begins, source offsets across archive storage and live Kafka partitions must be verified to form a contiguous, gapless range.
* **RP-05: Per-Aggregate Ordering & Exact-Next Application Hold During Replay [Certain]**: Replay runners enforce the exact same sequence tracking, held-key mechanics, and version guards as live consumers.
* **RP-06: Deterministic Handlers [Certain]**: Projections use injected event time (never wall clock) and derive primary keys deterministically from `(event_id, purpose)`.
* **RP-07: Write Ownership Hands Off, Never Dropped [Certain]**: During cutover, the replay runner is promoted to a live writer before the legacy consumer is decommissioned, preventing gaps in projection updates.
* **RP-08: Rollback Stays Cheap Until Decommission [Certain]**: The legacy projection remains maintained in dual-run mode throughout a 7-day observation window. Rollback requires only a read-pointer redefinition.
* **RP-09: Parity Against Source of Truth [Certain]**: State equivalence is validated at matching aggregate versions. Any unexplained difference between target projection and source write model blocks promotion.
* **RP-10: Run Manifest is Immutable & Complete [Certain]**: Manifests pin handler build SHA, schema versions, start/end offsets, and effect policies. Altering a run mid-flight is forbidden.
* **RP-11: Replay Honors Cryptographic Erasure [Certain]**: Replaying historical streams respects GDPR/privacy shredding; crypto-shredded records remain unreadable ciphertext.
* **RP-12: Replay Cannot Degrade Live Traffic Beyond Budget [Certain]**: Read rates from Kafka and write throughput to target stores are throttled. Automatic backpressure halts replay if live production consumer lag breaches SLOs.

---

## 4. Run Modes & Effect Gateway Policy

The Effect Gateway (`C-EFG`) enforces strict policy modes per run intent:

| Run Mode | Target Namespace | External Payments | Notifications | Consumer Outbox | Search / Cache |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`REBUILD`** | Shadow table / index | **SUPPRESS** | **SUPPRESS** | **SUPPRESS** | **ALLOW** (namespaced) |
| **`HYDRATE`** | Shadow table / index | **SUPPRESS** | **SUPPRESS** | **SUPPRESS** | **ALLOW** (namespaced) |
| **`FORENSIC`** | Read-only audit store | **SUPPRESS** | **SUPPRESS** | **SUPPRESS** | **SUPPRESS** |
| **`RECOVER`** | Restored primary store | **RECONCILE** | **RECONCILE** | **RECONCILE** | **ALLOW** |
| **`LIVE`** | Production store | **ALLOW** | **ALLOW** | **ALLOW** | **ALLOW** |

---

## 5. Storage Gate Contract & Database Writing Rules

Complies strictly with [05-db-writing-rules.md](file:///home/btpl-lap-22/live/System-Architect/llm-obs-policies/rules/folder-structure/05-db-writing-rules.md) and [01-schema-migrations-and-code-guidelines.md](file:///home/btpl-lap-22/live/System-Architect/llm-obs-policies/rules/database/migrations/01-schema-migrations-and-code-guidelines.md).

### 5.1 The 5 Identifier Roles Mapping
1. **Role 1 (Surrogate PK)**: UUIDv7 generated internally (`id`). Immutable.
2. **Role 2 (Public ID)**: Regex-constrained external strings (`rpl_...` for runs, `rpe_...` for effect logs, `rpa_...` for audits) matching `^[a-z]{3}_[0-9a-zA-Z]{26}$`.
3. **Role 3 (Tenant Scope)**: `tenant_id` leading composite primary keys and unique indices.
4. **Role 4 (Natural Upstream Key)**: Composite unique constraint on `(tenant_id, source_service, external_id)` with `btrim` validation.
5. **Role 5 (Correlation Trace)**: `correlation_id` UUID propagated from upstream OpenTelemetry trace context. Immutable.

### 5.2 Optimistic Concurrency & Audit Columns
- `replay_run` includes `version BIGINT NOT NULL DEFAULT 1`, auto-incremented by trigger on update.
- `replay_effect_log` and `replay_audit_log` are append-only ledgers protected by plpgsql triggers rejecting `UPDATE` and `DELETE`.

### 5.3 Entity-Relationship Diagram

```mermaid
erDiagram
    replay_run {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (rpl_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        varchar target_projection "Target read-model projection identifier"
        varchar mode "REBUILD, HYDRATE, FORENSIC, RECOVER, REPAIR, LIVE"
        varchar state "INITIALIZING, STREAMING, FOLLOWING, CAUGHT_UP, PROMOTED, COMPLETED, ABORTED"
        varchar scope "GLOBAL_TOPIC, TIME_WINDOW, AGGREGATE_SET"
        bigint start_offset "Starting offset coordinate"
        bigint current_offset "Latest processed offset coordinate"
        bigint end_offset "Target catch-up offset coordinate"
        varchar replay_consumer_group "Unique isolated consumer group name"
        bigint version "Optimistic concurrency version counter"
        timestamptz created_at "Record creation timestamp"
        timestamptz updated_at "Record last update timestamp"
    }

    replay_effect_log {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (rpe_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        uuid run_id FK "Reference to replay_run (tenant_id, id)"
        varchar effect_type "EXTERNAL_PAYMENT, EXTERNAL_NOTIFICATION, OUTBOX_EVENT, SEARCH_INDEX, CACHE_WRITE"
        varchar effect_mode "SUPPRESS, ALLOW, RECONCILE"
        varchar decision "SUPPRESSED, EXECUTED, RECONCILED"
        varchar idempotency_key "Deterministic provider deduplication key"
        varchar payload_digest "SHA-256 hash of effect payload"
        timestamptz created_at "Record creation timestamp"
    }

    replay_audit_log {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (rpa_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        uuid run_id FK "Reference to replay_run (tenant_id, id)"
        varchar from_state "State prior to transition"
        varchar to_state "New state after transition"
        varchar event "START_STREAMING, FOLLOW_LIVE, CATCH_UP_REACHED, PROMOTE_WRITER, COMPLETE_CUTOVER, ABORT_RUN"
        varchar reason "Mandatory operational justification"
        timestamptz created_at "Record creation timestamp"
    }

    replay_run ||--o{ replay_effect_log : "records effect decisions"
    replay_run ||--o{ replay_audit_log : "tracks state machine history"
```

---

## 6. State Machine & Execution Workflows

### 6.1 State Machine Lifecycle
```mermaid
stateDiagram-v2
    [*] --> INITIALIZING
    INITIALIZING --> STREAMING : START_STREAMING
    STREAMING --> FOLLOWING : FOLLOW_LIVE
    STREAMING --> CAUGHT_UP : CATCH_UP_REACHED
    FOLLOWING --> CAUGHT_UP : CATCH_UP_REACHED
    CAUGHT_UP --> PROMOTED : PROMOTE_WRITER
    PROMOTED --> COMPLETED : COMPLETE_CUTOVER

    INITIALIZING --> ABORTED : ABORT_RUN
    STREAMING --> ABORTED : ABORT_RUN
    FOLLOWING --> ABORTED : ABORT_RUN
    CAUGHT_UP --> ABORTED : ABORT_RUN
    PROMOTED --> ABORTED : ABORT_RUN
```

### 6.2 Side-Effect Gateway Interception Workflow
```mermaid
sequenceDiagram
    autonumber
    actor Handler as Replay Consumer Handler
    participant Gateway as Effect Gateway (C-EFG)
    participant Ledger as replay_effect_log (PostgreSQL)
    actor Provider as External Provider (Stripe / SES)

    Handler->>Gateway: InterceptEffect(PaymentIntent, IdempotencyKey)
    Gateway->>Gateway: ResolveEffectMode(RunMode)
    alt Mode == REBUILD / HYDRATE / FORENSIC
        Gateway->>Ledger: InsertEffectLog(Decision: SUPPRESSED)
        Gateway-->>Handler: Return Suppressed (No network call made)
    else Mode == RECOVER (Disaster Recovery)
        Gateway->>Gateway: Check Idempotency Ledger
        Gateway->>Provider: ReconcilePayment(IdempotencyKey)
        Gateway->>Ledger: InsertEffectLog(Decision: RECONCILED)
        Gateway-->>Handler: Return Reconciled Result
    else Mode == LIVE
        Gateway->>Provider: ExecutePayment(PaymentIntent)
        Gateway->>Ledger: InsertEffectLog(Decision: EXECUTED)
        Gateway-->>Handler: Return Execution Result
    end
```

---

## 7. Verification & Test Evidence
Automated test suite located at `src/features/replay-mechanism/tests/`:
* `TestRuleGenerateReplayConsumerGroup`: Verifies isolation of ephemeral consumer group names (RP-01).
* `TestRuleResolveEffectMode_Rebuild`: Verifies payments and notifications default to SUPPRESS in REBUILD mode (RP-02).
* `TestRuleResolveEffectMode_Recover`: Verifies RECONCILE mode in disaster recovery (RP-02).
* `TestRuleValidateOffsetAdvance_RegressionRejected`: Verifies monotonic offset advancement.
* `TestMachineTransition_Lifecycle`: Verifies full deterministic state machine transitions.
* `TestServiceCreateReplayRun`: Verifies run record initialization and audit trail creation.
* `TestServiceEvaluateEffect_SuppressExternalInRebuild`: Verifies end-to-end effect gateway ledger logging.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

When consumer code contains a bug that corrupts read models, or when a downstream database suffers catastrophic data loss, traditional systems must restore from slow backups and lose interim transactions. Because event logs are immutable and durable, event-driven architectures can rebuild corrupted state by seeking consumers backwards and replaying historical events.

### 2. Internal Algorithmic Mechanics & State Invariants

The replay coordinator receives an authorized Replay Request specifying topic, partition, and target boundary (timestamp or offset). It signals active consumers to pause forward processing, reset their partition offsets backwards via seek(), and begin replaying events. During replay, external side-effects (e.g. sending emails, debiting credit cards) are strictly suppressed using in-memory idempotency flags, and projections are written to shadow tables.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Bounded Temporal and Offset Windowing
* **Design Philosophy & Epistemology**:
  Replaying an unbounded event stream from the beginning of time is a dangerous operation that can take days, overwhelm databases, and exhaust storage. The philosophy is Bounded Replay Horizons: all replay operations must specify explicit start and stop boundaries defined by ISO 8601 timestamps or discrete partition offsets.
* **Threat Vector Defeated**: Uncontrolled historical event floods overwhelming downstream relational databases and triggering cascading platform outages during disaster recovery.
* **Storage & State Invariant**:
```text
ReplayWindow == [StartOffset, EndOffset] OR [StartTimestamp, EndTimestamp]
Processing halts automatically when EndOffset is reached
```
* **Implementation Mechanism**:
  The replay harness initializes a temporary consumer with a unique consumer group (`replay-worker-<uuid>`). It uses `consumer.offsetsForTimes()` to resolve timestamps into exact offsets, seeks to the start offset, and shuts down cleanly once the end offset is acknowledged.
* **Non-Negotiable Trade-Off**: Operational preparation: operators must determine exact failure timestamps before launching replay jobs.

---

#### Principle 2: Downstream Side-Effect Suppression (Dry-Run Replays)
* **Design Philosophy & Epistemology**:
  Replaying historical events must reconstruct state, but must NEVER re-trigger external physical side effects (e.g., sending emails to customers, charging credit cards, or notifying third-party logistics). The philosophy is Explicit Replay Context Propagation: replayed events carry a prominent flag instructing side-effect handlers to operate in read-only mode.
* **Threat Vector Defeated**: Catastrophic customer impact caused by historical replays re-sending 500,000 duplicate emails or double-charging credit cards.
* **Storage & State Invariant**:
```text
EventHeader('x-is-replay') == 'true'
If EventHeader('x-is-replay') == 'true' ==> SkipExternalSideEffects()
```
* **Implementation Mechanism**:
  The replay harness injects `x-is-replay: true` into Kafka record headers. Consumer side-effect middleware checks this header: state updates to databases proceed normally, while outbound notifications and payment calls are short-circuited and logged as dry-runs.
* **Non-Negotiable Trade-Off**: Requires all consumer codebases to strictly respect the `x-is-replay` header before triggering side effects.

---

#### Principle 3: Shadow Table State Reconstruction
* **Design Philosophy & Epistemology**:
  Replaying events directly into live, active production tables while live traffic is actively mutating them introduces race conditions and corrupts user sessions. The philosophy is Isolated Shadow Reconstruction: replay historical events into a separate shadow table; verify integrity; cutover atomically.
* **Threat Vector Defeated**: Live database corruption and race conditions caused by mixing historical replay writes with concurrent real-time user mutations.
* **Storage & State Invariant**:
```text
ReplayTargetTable == 'table_shadow_v2'
LiveProductionTable remains 100% isolated until Cutover
```
* **Implementation Mechanism**:
  The replay worker targets a newly provisioned shadow table. Once the replay catches up with the real-time stream, an atomic SQL transaction renames the tables (`ALTER TABLE ... RENAME`), achieving instantaneous zero-downtime cutover.
* **Non-Negotiable Trade-Off**: Temporary 2x database disk storage requirement during shadow table reconstruction.

---

#### Principle 4: Multi-Signature Administrative Audit Authority
* **Design Philosophy & Epistemology**:
  An event replay alters downstream reality and has massive compliance and operational consequences. Allowing a single engineer to trigger a mass replay without peer review is an unacceptable security and operational risk. The philosophy is Dual-Custody Administrative Governance: replays require multi-party approval and immutable audit logging.
* **Threat Vector Defeated**: Accidental or malicious mass event replays triggered by rogue operators or fat-finger terminal mistakes.
* **Storage & State Invariant**:
```text
ReplayExecution requires Approval(Engineer_1) AND Approval(Engineer_2)
AuditRecord permanently logged before replay starts
```
* **Implementation Mechanism**:
  The replay API requires cryptographically signed approval tokens from two independent engineers. The approval, target topic, offset range, and rationale are permanently committed to the immutable audit trail before the replay worker can boot.
* **Non-Negotiable Trade-Off**: Administrative latency during emergency production incident remediation.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is using a temporary consumer group ID mandatory when replaying events for a specific service?**
   * *Answer*: If you reset the offsets of the active production consumer group, the active service immediately stops processing live events and begins replaying historical data, disrupting real-time production traffic. Using a temporary consumer group isolates the replay completely.
2. **How does a consumer know where to stop replaying?**
   * *Answer*: The replay orchestrator queries the current High Watermark offset of each partition before starting. It instructs the replay consumer to terminate processing the moment `current_offset >= high_watermark`.
3. **What happens if a historical event schema is incompatible with current consumer code during a replay?**
   * *Answer*: The consumer's in-flight Upcaster pipeline transforms the historical v1 schema into the current v3 schema in memory before passing it to the business logic, ensuring seamless replay across schema eras.

---
## Authoritative References & Production Citations

* **[Martin Fowler: Event Sourcing Replay Patterns](https://martinfowler.com/eaaDev/EventSourcing.html)**
  - *Architectural Rationale & Implementation Grounding*: The foundational article explaining how to reconstruct past application states by replaying log events from genesis.
* **[Apache Kafka Offset Seek & Reprocessing Architecture](https://kafka.apache.org/documentation/#basic_ops_consumer_lag)**
  - *Architectural Rationale & Implementation Grounding*: Official documentation on using Kafka's seek APIs to reset consumer position for disaster recovery and historical replay.
* **[Uber Engineering: CADENCE / Temporal Workflow Replay Architecture](https://temporal.io/how-it-works)**
  - *Architectural Rationale & Implementation Grounding*: Production mechanics of deterministic event history replaying to reconstruct in-memory state machines without side-effect re-execution.
* **[Confluent: Event Sourcing, Replay, and Stream Processing Design Patterns](https://www.confluent.io/blog/event-sourcing-vs-event-driven-architecture/)**
  - *Architectural Rationale & Implementation Grounding*: Deep comparison between traditional database snapshots and dynamic log-driven state reconstruction.

## Amendment 2026-11-07: Advance state gate, run-id validation, effect-log persistence

### Problem observed
1. `POST /replay/runs/advance` was accepted for a run still in `INITIALIZING`.
2. A non-UUID `runId` (for example the public id `rpl_...`) reached Postgres and returned raw `pq:` text.
3. `POST /replay/effects/evaluate` returned `200 SUPPRESSED` but nothing appeared in `replay_effect_log`.

### Root cause
1. `AdvanceReplay` validated only offset monotonicity/bounds, not the run state.
2. No run-id validation before the UUID-typed query (RC2).
3. `EvaluateEffect` discarded the `RecordEffectLog` error (`_ =`, RC3). The insert always failed because `source_service`, `external_id` and `correlation_id` are required by DB CHECKs and a minimal request left them empty.

### Decision / fix
- `rules.CanAdvanceOffset(state)` gates `AdvanceReplay`; violations return `ErrReplayNotAdvanceable` (409). Advance state table:

| Run state | Advance offset allowed |
|---|---|
| INITIALIZING | No (409) |
| STREAMING | Yes |
| FOLLOWING | Yes |
| CAUGHT_UP | Yes |
| PROMOTED | No (409) |
| COMPLETED | No (409) |
| ABORTED | No (409) |

  Offset rules still apply when allowed: not below the current offset, and not above `end_offset` when `end_offset > 0` (`ValidateOffsetAdvance`, 400).
- `contracts.ValidateRunID` (required, must parse as UUID) runs in `StartStreaming`, `AbortReplay`, `GetReplayRun`, and the advance path: `ErrMissingRunID` / `ErrInvalidRunID` ("runId must be a valid UUID") -> 400. Use the `id` field, not the `rpl_*` public id.
- Handler mapping (`ReplayHandler.writeServiceError`): `ErrReplayRunNotFound` 404; `ErrInvalidReplayTransition`, `ErrReplayNotAdvanceable`, `ErrReplayConflict` 409; `ErrReplayStorageFailure` generic 500 ("internal server error", detail not leaked); everything else 400.
- `EvaluateEffect` defaults `source_service` to `replay-effects-api`, `external_id` to `eff_<uuidv7>`, `correlation_id` to a generated UUIDv7, validates a supplied `correlation_id` as UUID, and returns the `RecordEffectLog` error instead of ignoring it. Defaulting was chosen so a minimal call is recorded and auditable, while the error return guarantees no "suppressed but unlogged" state.

### API-visible behavior change
- Advance in non-advanceable state: `200` -> `409` (**breaking**).
- Non-UUID runId: raw pq `400/500` -> clean `400`; unknown run `404`.
- Effects evaluate: now persists a row; storage failure -> 500 instead of false 200.

### Verification
Live: advance in INITIALIZING -> 409, non-UUID -> 400, unknown -> 404, effects/evaluate wrote a `replay_effect_log` row. Unit tests in `src/features/replay-mechanism/tests/replay_test.go`.

### Remaining gaps
Create-run still accepts a non-UUID `correlation_id` (not fixed). The Postgres replay adapter paths are not covered by CI integration tests.


## Amendment 2026-11-07 (second pass): correlationId validation

`POST /replay/runs` rejects a non-UUID `correlationId` (and a non-UUID `X-Correlation-ID` fallback) with 400 `ErrInvalidCorrelationID`; `replay_run.correlation_id` is a UUID column.

Verified live on the compose-deployed image (`chiefj/event-driven-platform:v2`) and by unit tests; see ADR-0002 "Closed in a second pass".

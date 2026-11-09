# Architectural Decision Record (ADR): Idempotent Consumers with Two-Layer Defense

* **Status:** Accepted
* **Date:** 2026-11-04
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `src/features/idempotent-consumers/`, `src/api/rest/v1/`, `database/migrations/`
* **Governing Decision Tree:** `docs/features/idempotent-consumers/business-logic.md` (v2)

---

## 1. Context and Problem Statement

In distributed message brokers like Apache Kafka, delivery guarantees are strictly **at-least-once**. Network partitions, consumer group rebalances, worker crashes prior to offset commits, and producer outbox retries routinely deliver duplicate and out-of-order events.

Without dedicated consumer idempotency and version fencing:
- **Delta Mutations** (e.g. account debit, inventory decrement, order item additions) get double-applied, corrupting financial ledgers and state projections.
- **Stale Replays** (e.g. DLQ redrives, historical reconciliation replays, or partition rebalance zombies) overwrite newer aggregate state.
- **Split-Brain Collision** (e.g. database restore on upstream producer minting new payload for an already emitted version) silently diverges read models.

---

## 2. Invariants & Decision Drivers (from `business-logic.md` v2)

* **IC-01 Atomic Effect**: Dedupe record (`consumer_inbox`) + aggregate projection state (`consumer_projections`) + partition offset (`consumer_partition_offsets`) are updated in **ONE local database transaction**.
* **IC-02 Effect-Once Semantics**: Exactly-once exists solely as an idempotent effect at the consumer; transport is at-least-once forever.
* **IC-03 Identity**: Dedupe key is the producer-assigned `event_id`, identical across redeliveries.
* **IC-04 Two Layers of Protection**:
  - *Layer 1 (Event Identity)*: Catches transport and relay duplicates via `consumer_inbox (tenant_id, handler_id, event_id)`.
  - *Layer 2 (Aggregate Version Guard)*: Catches stale, reordered, and replayed events permanently via `consumer_projections.last_version`.
* **IC-05 Version Rule**:
  - $v \le \text{last\_version} \implies$ duplicate/stale $\implies$ acknowledge, skip.
  - $v = \text{last\_version} + 1 \implies$ apply effect and advance version.
  - $v > \text{last\_version} + 1 \implies$ **GAP** (upstream loss) $\implies$ hold key, never apply, page on-call.
* **IC-06 Delta vs State Semantics**:
  - *State Events*: Full snapshots allow "strictly newer wins" ($v > \text{last\_version}$).
  - *Delta Events*: Strict next-version ($v = \text{last\_version} + 1$) + `event_id` dedupe required.
* **IC-07 Offset After Effect**: Offset is advanced only after the database effect commits.
* **IC-08 Dedupe Scope**: Scoped to `(tenant_id, handler_id, event_id)`, never `event_id` alone.
* **IC-10 No Ordering Bypass**: DLQ redrives must pass through the version guard; stale events are skipped.

---

## 3. Comprehensive Visual Architectural Diagrams

### 3.1 End-to-End Consumer Pipeline Architecture

```mermaid
flowchart TD
    subgraph INGRESS["1. Transport Ingress Gate"]
        K1["Kafka Topic / HTTP API Ingress"] --> VAL{"Ingress Validation Gate<br/>(Schema, UUIDv7, Non-Empty Strings)"}
        VAL -- "Invalid" --> REJ["HTTP 400 Bad Request<br/>(Structured Error Envelope)"]
        VAL -- "Valid" --> HASH["Compute SHA-256 Payload Digest"]
    end

    subgraph LAYER1["2. Layer 1: Event Identity Deduplication Gate (IC-03, IC-08)"]
        HASH --> INBOX{"INSERT INTO consumer_inbox<br/>(tenant_id, handler_id, event_id)<br/>ON CONFLICT DO NOTHING"}
        INBOX -- "Rows Affected == 0" --> SKIP1["ActionSkip (Transport Duplicate)<br/>Commit Offset & Return 200 OK"]
        INBOX -- "Rows Affected == 1" --> READ_PROJ["Query consumer_projections<br/>FOR UPDATE (tenant_id, handler_id, aggregate_id)"]
    end

    subgraph LAYER2["3. Layer 2: Version Guard & Split-Brain Gate (IC-04, IC-05, IC-06)"]
        READ_PROJ --> CHK_PARK{"Is Aggregate Parked?<br/>(is_parked == TRUE)"}
        CHK_PARK -- "Yes" --> PARK_SUPPRESS["ActionParkPoison<br/>HTTP 423 Locked / Suppress Execution"]
        CHK_PARK -- "No" --> CHK_HASH{"v == last_version AND<br/>hash != last_payload_hash?"}
        CHK_HASH -- "Yes (Split-Brain)" --> PARK_TRIGGER["ParkAggregate(is_parked=TRUE)<br/>ActionParkPoison (HTTP 423 Locked)"]
        CHK_HASH -- "No" --> EVAL_VER{"Evaluate Version & Semantic Class"}
        
        EVAL_VER -- "v <= last_version" --> SKIP2["ActionSkip (Duplicate / Stale Version)<br/>Safe No-Op"]
        EVAL_VER -- "v > last_version + 1 (DELTA)" --> HOLD["ActionHoldGap (Event Loss Detected)<br/>HTTP 409 Conflict / Hold Key"]
        EVAL_VER -- "v == last_version + 1 (DELTA)<br/>or v > last_version (STATE)" --> APPLY["ActionApply<br/>Execute Business Handler"]
    end

    subgraph ATOMIC_SINK["4. Storage Gate & Transaction Boundary (IC-01, IC-07)"]
        APPLY --> TX["BEGIN PostgreSQL Transaction"]
        TX --> M1["Mutate Business Read Model"]
        TX --> M2["Upsert consumer_projections<br/>(last_version = v, last_hash = hash)"]
        TX --> M3["Store consumer_partition_offsets"]
        TX --> COMMIT{"COMMIT TX"}
        COMMIT -- "Success" --> ACK["Acknowledge Kafka Offset"]
        COMMIT -- "Error" --> ROLLBACK["ROLLBACK<br/>Map DB Constraint & Retry/Nack"]
    end
```

---

## 4. Invariant-Specific Mermaid Diagrams

### 4.1 Invariant IC-01: Atomic Effect & Offset Boundary
```mermaid
sequenceDiagram
    autonumber
    participant Engine as Consumer Engine
    participant DB as PostgreSQL (Single Transaction Boundary)
    participant Broker as Kafka Broker

    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: INSERT INTO consumer_inbox ...
    Engine->>DB: UPDATE business_projection ...
    Engine->>DB: UPDATE consumer_projections SET last_version = v ...
    Engine->>DB: INSERT INTO consumer_partition_offsets ...
    Engine->>DB: COMMIT TRANSACTION
    DB-->>Engine: Transaction Committed Durably
    Engine->>Broker: Commit Kafka Consumer Group Offset
```

### 4.2 Invariant IC-05: Version Rule Gate (DELTA vs STATE)
```mermaid
stateDiagram-v2
    [*] --> CheckVersion
    
    state CheckVersion <<choice>>
    CheckVersion --> ActionSkip : v <= last_version (Stale / Transport Duplicate)
    CheckVersion --> EvaluateDelta : v > last_version (Newer Event)

    state EvaluateDelta <<choice>>
    EvaluateDelta --> ActionApply : SemanticClass == STATE (Strictly newer wins)
    EvaluateDelta --> CheckDeltaStep : SemanticClass == DELTA

    state CheckDeltaStep <<choice>>
    CheckDeltaStep --> ActionApply : v == last_version + 1 (Sequential Next)
    CheckDeltaStep --> ActionHoldGap : v > last_version + 1 (Version Gap / Event Loss)

    ActionSkip --> [*]
    ActionApply --> [*]
    ActionHoldGap --> [*]
```

### 4.3 Invariant IC-08: Deduplication Scoped Per Handler
```mermaid
flowchart LR
    Event["Inbound Event (event_id: evt-100)"] --> HA["Handler A: Order Billing"]
    Event --> HB["Handler B: Inventory Allocation"]

    HA --> IA["consumer_inbox<br/>(tenant-1, handler-A, evt-100)"]
    HB --> IB["consumer_inbox<br/>(tenant-1, handler-B, evt-100)"]

    IA -- "Recorded" --> PA["Order Billing State Applied"]
    IB -- "Recorded" --> PB["Inventory State Applied"]
```

---

## 5. Failure Case Sequence Diagrams

### 5.1 F3.1.5 & F4.4: Zombie Consumer Overwrite Prevention
```mermaid
sequenceDiagram
    autonumber
    actor ZC as Zombie Consumer (Stalled GC)
    actor NC as New Owner Consumer (Partition Reassigned)
    participant DB as PostgreSQL (consumer_projections)

    Note over NC: Processes Event V=3 successfully
    NC->>DB: UPDATE consumer_projections SET last_version = 3 WHERE last_version < 3
    DB-->>NC: 1 Row Updated (last_version = 3)

    Note over ZC: Awakens from GC pause, attempts to apply Event V=2
    ZC->>DB: UPDATE consumer_projections SET last_version = 2 WHERE last_version < 2
    DB-->>ZC: 0 Rows Affected (Conditional Update Failed)
    Note over ZC: Stale write silently rejected; zero lost updates!
```

### 5.2 F3.2.4: Split-Brain Same Version Divergent Content Collision
```mermaid
sequenceDiagram
    autonumber
    actor Producer as Upstream Producer (Dual-Leader / DB Restore)
    participant Engine as Consumer Engine
    participant DB as PostgreSQL (consumer_projections)

    Producer->>Engine: Deliver Event V=2 (Payload: {amount: 100}, Hash: 0xAAAA)
    Engine->>DB: Apply V=2 (last_version = 2, last_payload_hash = 0xAAAA)
    
    Producer->>Engine: Redeliver Event V=2 (Payload: {amount: 200}, Hash: 0xBBBB)
    Engine->>DB: Query Projection (last_version=2, last_payload_hash=0xAAAA)
    Note over Engine: Detect Hash Mismatch: 0xAAAA != 0xBBBB!
    Engine->>DB: UPDATE consumer_projections SET is_parked = TRUE
    Engine-->>Producer: HTTP 423 Locked (ActionParkPoison)
    Note over Engine: Aggregate isolated; alerts on-call dashboard
```

### 5.3 F3.2.5: Out-of-Order First Event Arrival ($V > 1$)
```mermaid
sequenceDiagram
    autonumber
    actor Broker as Kafka Partition
    participant Engine as Consumer Engine
    participant DB as PostgreSQL (consumer_projections)

    Broker->>Engine: Ingest First Event Seen: V=2 (OrderShipped)
    Engine->>DB: GetProjection (not found -> current_version = 0)
    Note over Engine: DELTA Event expects V=0+1=1. Received V=2!
    Engine-->>Broker: ActionHoldGap (HTTP 409 Conflict)
    Note over Engine: Key held; downstream ledger corruption prevented
```

---

## 6. PostgreSQL Storage Gate Constraint Registry

To satisfy **05-db-writing-rules.md Section 8**, database constraint violations are translated deterministically:

| Constraint Name | Description | PostgreSQL Type | Canonical Error Code |
|---|---|---|---|
| `consumer_inbox_dedupe_uk` | Duplicate `(tenant_id, handler_id, event_id)` | UNIQUE | `CONFLICT` (409) |
| `consumer_inbox_tenant_id_ck` | Non-empty trimmed tenant string | CHECK | `VALIDATION_FAILED` (400) |
| `consumer_inbox_handler_id_ck` | Non-empty trimmed handler string | CHECK | `VALIDATION_FAILED` (400) |
| `consumer_inbox_aggregate_version_ck` | Monotonic version $\ge 1$ | CHECK | `VALIDATION_FAILED` (400) |
| `consumer_inbox_payload_hash_ck` | SHA-256 64-char hex format | CHECK | `VALIDATION_FAILED` (400) |
| `consumer_projections_last_version_ck`| Non-negative projection version $\ge 0$ | CHECK | `VALIDATION_FAILED` (400) |

---

## 7. Consequences and Trade-offs

* **Positive**:
  - Eliminates duplicate mutations under any transport failure or redelivery scenario.
  - Guarantees per-aggregate ordering safety and halts corruption before state changes occur.
  - Zero application leakage of database internals via centralized error mapping.
* **Negative**:
  - Version gaps (`ActionHoldGap`) block partition processing unless delegated to non-blocking retry topics.
  - Inbox table requires periodic partition archiving (`PurgeIntervalHours = 48`) to prevent storage bloat.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Distributed message delivery in systems like Apache Kafka is strictly at-least-once. Network timeouts, consumer group rebalances, and producer retries routinely deliver duplicate copies of the same event. If a consumer naively applies mutations upon every arrival, financial accounts are debited twice, duplicate orders are shipped, and system state is permanently corrupted.

### 2. Internal Algorithmic Mechanics & State Invariants

The service implements a Two-Layer Defense: Layer 1 (Transport Identity) inserts the event_id into a dedicated consumer_inbox table inside the same local database transaction as the business mutation (ON CONFLICT DO NOTHING); if 0 rows are affected, it is a duplicate and immediately skipped. Layer 2 (Aggregate Version Guard) checks the aggregate's last_version: v = last+1 applies; v <= last drops as stale; v > last+1 holds as a gap. If an event shares the current version but has a divergent payload hash, it is quarantined as a split-brain poison pill.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Two-Layer Deduplication (Inbox Ledger + Version Fencing)
* **Design Philosophy & Epistemology**:
  Deduplicating exclusively by `event_id` is dangerously incomplete: it prevents duplicate processing of the exact same message, but fails against out-of-order state mutations. If an `OrderUpdated(Status=Shipped)` event arrives before an older `OrderUpdated(Status=Processing)` retry, simple event deduplication allows the older event to overwrite the newer state. The philosophy is Dual-Layer Invariant Guarding: protect identity with an Inbox ledger, and protect causal sequence with Monotonic Version Fencing.
* **Threat Vector Defeated**: State corruption where stale, retried events overwrite newer database records, or where duplicate events trigger duplicate physical actions (e.g., double-charging credit cards).
* **Storage & State Invariant**:
```text
Layer 1: ProcessedEvents(EventID) is Unique (Idempotent Delivery)
Layer 2: Aggregate.Version == IncomingEvent.Version - 1 (Monotonic Sequence Progression)
```
* **Implementation Mechanism**:
  The consumer acquires a database transaction. Step 1 attempts to insert `event_id` into `inbox_records` (`ON CONFLICT DO NOTHING`). Step 2 executes a conditional state update: `UPDATE orders SET status = $1, version = version + 1 WHERE id = $2 AND version = $3`. If either condition fails, the transaction safely rolls back or acknowledges the duplicate.
* **Non-Negotiable Trade-Off**: Relational storage write amplification from recording every processed `event_id` in an inbox table, requiring automated background pruning.

---

#### Principle 2: Atomic Local Transactional Boundary (The Inbox Pattern)
* **Design Philosophy & Epistemology**:
  Recording message processing in an external cache (Redis) while mutating business state in a relational database (PostgreSQL) is the distributed dual-write fallacy all over again. If Redis updates but PostgreSQL crashes, the event is permanently lost; if PostgreSQL commits but Redis fails, the event will be reprocessed. The philosophy is Co-Locality of Deduplication: the inbox ledger and the business mutation must share the exact same local ACID transaction.
* **Threat Vector Defeated**: Silent message loss or duplicate execution caused by split-brain states between external deduplication caches and primary databases.
* **Storage & State Invariant**:
```text
Insert(InboxRecord) AND Mutate(DomainState) committed within ONE Database Transaction
```
* **Implementation Mechanism**:
  The consumer opens a single SQL transaction against PostgreSQL: it inserts the `event_id` into the local `inbox` table and executes the business mutation on the domain tables simultaneously. They commit or abort atomically.
* **Non-Negotiable Trade-Off**: Database connection pool utilization and lock contention on high-throughput consumers.

---

#### Principle 3: Commutative and Associative State Transformers
* **Design Philosophy & Epistemology**:
  The most robust idempotency is mathematical, not procedural. If applying an operation multiple times or in different orders yields the exact same state, the need for complex deduplication locks disappears. The philosophy is Algebraic Convergence: design state transitions to be naturally idempotent (A * A = A) and commutative (A * B = B * A).
* **Threat Vector Defeated**: Cascading locking deadlocks and race conditions caused by concurrent consumer instances attempting to lock the same deduplication row.
* **Storage & State Invariant**:
```text
f(f(State, Event)) == f(State, Event) (Idempotence)
f(f(State, Event_A), Event_B) == f(f(State, Event_B), Event_A) (Commutativity)
```
* **Implementation Mechanism**:
  State updates are structured as set additions (`INSERT ... ON CONFLICT DO NOTHING`), absolute state assertions (`UPDATE ... SET status = 'CANCELLED' WHERE status != 'CANCELLED'`), or delta accumulators with epoch tracking rather than relative increments (`balance = balance + 10` is converted to transactional ledger entries).
* **Non-Negotiable Trade-Off**: Requires reimagining domain business operations as algebraic operations rather than simple imperative mutations.

---

#### Principle 4: Pruning Lifecycles & Deduplication Horizon
* **Design Philosophy & Epistemology**:
  An inbox table that grows forever without retention management will eventually exhaust database disk space and degrade index lookups. The philosophy is Bounded Deduplication Horizons: deduplication guarantees are valid within a defined temporal window ($T_{\text{dedupe}}$), which must be strictly larger than the message broker's maximum message retention.
* **Threat Vector Defeated**: PostgreSQL performance collapse and disk exhaustion caused by millions of historical inbox records accumulating indefinitely.
* **Storage & State Invariant**:
```text
Retention(InboxRecords) > Retention(KafkaTopic) >= MaxRetryWindow
Purge(InboxRecords where created_at < NOW() - RetentionInterval)
```
* **Implementation Mechanism**:
  The `inbox` table is partitioned by date/week. A scheduled reaper drops partitions older than the retention horizon (e.g., 14 days), guaranteeing that no message within Kafka's 7-day retention window can be erroneously reprocessed.
* **Non-Negotiable Trade-Off**: Any event replayed from cold storage older than the 14-day deduplication window must be treated as a manual administrative action rather than an automated idempotent consume.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is Redis INCR or SETNX insufficient as a standalone idempotency guard for relational database updates?**
   * *Answer*: Because Redis and the relational database do not share a transaction boundary. If Redis records the key but the relational database rolls back due to a constraint violation, subsequent retries of the valid message will be rejected as duplicates, causing permanent silent data loss.
2. **What happens when an event arrives with aggregate_version = 7, but the local database is currently at aggregate_version = 5?**
   * *Answer*: The consumer detects a gap hole (version 6 is missing). It must not process version 7, as doing so would corrupt state. The consumer halts or routes version 7 to a delayed retry topic, awaiting the arrival of version 6.
3. **How does an idempotent consumer handle non-idempotent third-party side effects (e.g., sending an email or charging Stripe)?**
   * *Answer*: It uses an Outbox pattern or passes a deterministic `idempotency_key` (derived from the event ID) to the external API (e.g., Stripe's `Idempotency-Key` header). If the call is retried, the third-party provider recognizes the key and returns the existing receipt without double-charging.

---
## Authoritative References & Production Citations

* **[Pat Helland: Idempotence Is Not a Free Lunch (ACM Queue 2012)](https://queue.acm.org/detail.cfm?id=2187821)**
  - *Architectural Rationale & Implementation Grounding*: Deep architectural dive into side-effects, message redelivery mechanics, and building robust idempotent software services.
* **[Gregor Hohpe: Idempotent Receiver Pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html)**
  - *Architectural Rationale & Implementation Grounding*: Authoritative pattern on designing receivers capable of safely processing duplicate messages without unintended side-effects.
* **[Apache Kafka KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98)**
  - *Architectural Rationale & Implementation Grounding*: Seminal Kafka enhancement specification detailing idempotent producers, transactional coordinators, and consumer read-committed modes.
* **[Martin Kleppmann: Designing Data-Intensive Applications (Chapter 11: Idempotence)](https://dataintensive.net/)**
  - *Architectural Rationale & Implementation Grounding*: Definitive analysis of deduplication mechanisms, unique constraint guards, and distributed transaction atomicity.

## Amendment 2026-11-07: Inbox event validation (eventId UUID, semanticClass enum)

### Problem observed
- `POST /api/v1/consumers/inbox` with a non-UUID `eventId` returned 500.
- An unrecognised `semanticClass` (e.g. `BOGUS`) was silently treated as `DELTA`, applying the gap-hold version rule to a stream the caller intended otherwise.

### Root cause
- RC2: `eventId` format was enforced only by the Postgres UUID column, so the failure surfaced as a 500.
- Handler mapping: `idempotent-consumers.handler.go: Consume` set `semantic = DELTA` and switched to `STATE` only when the string equalled the STATE constant; any other value fell into DELTA. The service-level contract did not validate the enum.
- RC1: the handler used a bare `err.(*appErrors.ApplicationError)` assertion, which misses wrapped errors.

### Decision / fix
- `ValidateInboundConsumerEvent` rejects a non-empty `eventId` that is not a UUID and any `semanticClass` other than `STATE` or `DELTA`, with 400 field-level details.
- The handler passes the caller's `semanticClass` through unchanged; only an empty value defaults to `DELTA` (preserving the previous default for omitted fields).
- The handler uses `errors.As`.

### API-visible behavior change
- Non-UUID `eventId`: 500 becomes 400.
- Unknown `semanticClass`: silent DELTA becomes 400. **Breaking** for clients sending unrecognised values; empty still defaults to `DELTA`.
- Existing rule tests were updated to UUID fixtures.

### Verification
Re-tested live: both cases return 400; empty `semanticClass` still accepted. Unit test `TestValidateInboundConsumerEvent_EventIDAndSemanticClass` added (`tests/idempotent_consumers_validation_test.go`).

### Remaining gaps
- No HTTP-level test; the default-to-`DELTA` for empty `semanticClass` remains an implicit choice that could mask producer omissions.

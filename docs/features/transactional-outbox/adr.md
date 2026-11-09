# Transactional Outbox: Architectural Decision Record & Production Reference

* **Status:** Accepted
* **Date:** 2026-11-04
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `src/features/transactional-outbox/`, `src/api/`, `messaging/engine/`

---

## 1. Context and Problem Statement

When domain entities are modified in a relational database and corresponding events must be emitted to a message broker (e.g. Apache Kafka), performing a direct "dual-write" (writing to SQL, then calling `producer.Publish()`) guarantees data corruption:
- If the database transaction commits but the network call to the broker fails or times out, the event is permanently lost, causing downstream projections and sagas to become inconsistent.
- If the event is published first and the database transaction fails (e.g. constraint violation or crash), downstream consumers act on phantom events that never existed in the database.

Distributed transactions (2PC / XA) across Kafka and PostgreSQL are not supported, scale poorly, and introduce latency and operational fragility.

---

## 2. Invariants (System-Wide Truths)

* **INV-01 Atomicity**: Business mutation and outbox insert share ONE connection, ONE transaction, ONE database.
* **INV-02 Delivery Semantics**: At-least-once end to end; exactly-once exists only as an idempotent consumer effect.
* **INV-03 Per-Aggregate Ordering**:
  - Message key = `aggregate_id`.
  - All event types of one aggregate go to ONE topic named after `aggregate_type`.
  - Partition count never changes.
  - Relay preserves commit order per key and never sends $k+1$ before $k$ is acked.
* **INV-04 Version Monotonicity**: Each event carries `aggregate_version = previous + 1`, enforced via unique constraint `(tenant_id, aggregate_type, aggregate_id, aggregate_version)`.
* **INV-05 Event Identity**: `event_id` generated once via RFC 9562 UUIDv7, immutable across retries and replays.
* **INV-06 Single Writer**: `source_service` header guarantees a single service writes an aggregate type.
* **INV-07 No Ghost Events**: Never publish an event whose transaction can still be lost (async commit, async failover).
* **INV-09 Business Idempotency**: Step 1 claims `x-idempotency-key` in `outbox_idempotency_keys` table; retry with identical key is a no-op.
* **INV-11 Retention Chain**: Source-of-truth retention $\ge$ Kafka retention $\ge$ consumer dedupe window $\ge$ provider idempotency window.
* **INV-12 Payload Fidelity**: Financial numbers strictly forbidden as float; integer minor units or decimal strings only. Payloads > 1MB require claim-check.
* **INV-14 Observability Completeness**: Absence of metrics is an alert condition, never a healthy state.

---

## 3. End-to-End Architectural Diagrams

### 3.1 Write Path & Poller Relay with Fencing Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Service as Business Domain Service
    participant DB as PostgreSQL (ACID)
    participant Relay as Outbox Relay Worker (Worker A)
    participant RelayB as Outbox Relay Worker (Worker B)
    participant Broker as Apache Kafka Broker
    participant Consumer as Downstream Consumer

    rect rgb(240, 248, 255)
    Note over Client,DB: Section 1: Ingress Write Path (Atomic Transaction)
    Client->>Service: POST /api/v1/outbox (x-idempotency-key, payload)
    activate Service
    Service->>DB: 1. Claim Idempotency Key (INSERT INTO outbox_idempotency_keys ON CONFLICT)
    alt Idempotency Key Already Claimed (F1.2)
        DB-->>Service: Return existing record_id
        Service-->>Client: 200 OK (Return existing OutboxRecord - no duplicate aggregate)
    else First Command Execution
        Service->>DB: 2. Mutate Aggregate (version = version + 1 under row lock)
        Service->>DB: 3. Insert OutboxRecord (status='PENDING', aggregate_version)
        Service->>DB: 4. COMMIT Transaction
        Service-->>Client: 201 Created (Staged OutboxRecord)
    end
    deactivate Service
    end

    rect rgb(255, 250, 240)
    Note over Relay,Broker: Section 4: Poller Relay Lease & Ownership Fencing
    Relay->>DB: ClaimInFlight(worker_id='worker-A', lease=30s, limit=50)
    DB-->>Relay: Return claimed batch (status='IN_FLIGHT', worker_id='worker-A')
    
    alt Happy Path (Broker Ack before Lease Expiry)
        Relay->>Broker: Publish to topic = aggregate_type (Key = aggregate_id)
        Broker-->>Relay: Broker ACK (all ISR replicas)
        Relay->>DB: MarkPublishedByWorker(id, worker_id='worker-A', lease_expires_at >= NOW())
        DB-->>Relay: Confirmed PUBLISHED
    else Lease Expiry & Race Condition (F4.6 Fencing)
        Note over Relay: Network delay stalls Worker A past 30s lease
        RelayB->>DB: ClaimInFlight(worker_id='worker-B', lease=30s)
        DB-->>RelayB: Reclaims record (worker_id='worker-B')
        Relay->>DB: Worker A attempts MarkPublishedByWorker('worker-A')
        DB--xRelay: 412 Precondition Failed / Worker Ownership Lost (Fenced Out)
        RelayB->>Broker: Worker B Publishes (INV-02 At-Least-Once)
        RelayB->>DB: Confirmed PUBLISHED by Worker B
    else Poison Pill Failure (F4.7 Aggregate Parking)
        Relay->>Broker: Publish Attempt
        Broker--xRelay: Permanent Broker Rejection (Bad Header / Payload)
        Relay->>DB: MarkFailed(retry_count = retry_count + 1)
        Note over DB: On retry_count >= 5 -> status='PARKED'
        Note over DB: Aggregate k+1 queries suppressed by NOT EXISTS (PARKED)
    end
    end

    rect rgb(245, 255, 245)
    Note over Broker,Consumer: Section 6: Consumer Tier & Idempotent Processing
    Broker->>Consumer: Deliver Event (aggregate_id, aggregate_version, event_id)
    activate Consumer
    alt Incoming version <= last_applied
        Note over Consumer: Duplicate detected -> Acknowledge and skip
    else Incoming version == last_applied + 1
        Note over Consumer: Apply state mutation and record event_id in consumer DB tx
    else Incoming version > last_applied + 1
        Note over Consumer: Gap detected (upstream loss) -> Hold key, do not apply, page on-call
    end
    deactivate Consumer
    end
```

### 3.2 Outbox Record State Machine Diagram

```mermaid
stateDiagram-v2
    [*] --> PENDING: StageEvent (Normal Poller Path)
    [*] --> DELETED: StageEvent (CDC Mode Step 5)
    
    PENDING --> IN_FLIGHT: ClaimInFlight (Worker ID + Lease Assigned)
    
    IN_FLIGHT --> PUBLISHED: Broker ACK + Active Lease Verified
    IN_FLIGHT --> FAILED: Broker Publish Error
    IN_FLIGHT --> PENDING: Lease Expired Without Broker Ack
    
    FAILED --> PENDING: Retry with Backoff (Retry Count < 5)
    FAILED --> PARKED: Retry Count >= 5 (Poison Event Isolated)
    
    PARKED --> PENDING: Manual Triage / Unpark (R3 Runbook)
    
    PUBLISHED --> PURGED: PurgeExpired (Age > 48h Retention)
    PURGED --> [*]
    DELETED --> [*]
```

---

## 4. Specific Edge Cases and Deep Failure Handling

### 4.1 Write Path Edge Cases
1. **F1.2 Ambiguous Commit (Connection drops during COMMIT)**:
   - *Impact*: Client does not know if the transaction committed. Naive retries could double-charge or create two aggregates.
   - *Mitigation*: Step 1 claims `x-idempotency-key` in `outbox_idempotency_keys` table with primary key `(tenant_id, idempotency_key)`. The retry hits `ON CONFLICT DO NOTHING`, detects the collision, and returns the existing `OutboxRecord` without creating duplicate aggregates or outbox rows.
2. **F1.5 Concurrent Mutations on Same Aggregate**:
   - *Impact*: Two concurrent business transactions assign the same version or produce an out-of-order WAL commit sequence.
   - *Mitigation*: DB unique constraint `UNIQUE (tenant_id, aggregate_type, aggregate_id, aggregate_version)` causes the slower concurrent transaction to abort with a SQL unique violation, mapped to `ConflictError` (HTTP 409).
3. **F1.7 Oversized Payloads**:
   - *Impact*: Payloads exceeding broker request size limit (e.g., 1MB) become permanent poison events that stall partition processing.
   - *Mitigation*: `rules.IsPayloadSizeValid` rejects payloads $> 1\text{MB}$ at ingress before database persistence.
4. **F1.9 Floating-Point Precision Loss**:
   - *Impact*: `float64` floating point parsing drifts currency cents over time and rounds 64-bit integers $> 2^{53}$.
   - *Mitigation*: `rules.HasFloatingPointNumbers` parses numeric tokens and rejects any JSON payload containing `.`, `e`, or `E`.

### 4.2 Relay & Poller Edge Cases
1. **F4.0 / F4.1 Lock Holding Across Network**:
   - *Impact*: Holding database row locks open while awaiting Kafka network acks pins PostgreSQL vacuum horizons, accumulates table bloat, and exhausts connection pools.
   - *Mitigation*: Lease-based claim model. Poller marks records `IN_FLIGHT` with `worker_id` and `lease_expires_at` in a short transaction, releases the lock, performs the network publish asynchronously, and marks `PUBLISHED` in a separate short transaction.
2. **F4.3 Commit-Order Visibility Gap**:
   - *Impact*: A transaction with an earlier timestamp commits *after* a poller has read past that timestamp. Watermark/cursor-based querying skips the committed event forever.
   - *Mitigation*: Queries never filter on `id > last_seen_id` or `created_at > last_seen_timestamp`. Queries select strictly by `status = 'PENDING' OR (status = 'IN_FLIGHT' AND lease_expires_at < NOW())`.
3. **F4.6 Lease Expiration Fencing**:
   - *Impact*: Worker A experiences a GC pause or network stall exceeding its 30s lease. Worker B reclaims the batch and publishes. Worker A awakens and attempts to mark the records published, overwriting subsequent state.
   - *Mitigation*: `MarkPublishedByWorker` queries `WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 AND (lease_expires_at IS NULL OR lease_expires_at >= NOW())`. If 0 rows are affected, Worker A is fenced out and returns `ErrWorkerOwnershipLost`.
4. **F4.7 Poison Event / Poison Aggregate Parking**:
   - *Impact*: A record with an unroutable schema or broker-rejected payload fails permanently. Skipping it and publishing version $k+1$ violates INV-03 per-aggregate ordering.
   - *Mitigation*: After 5 consecutive publish errors, the record transitions to `PARKED`. The relay query includes `AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.aggregate_id = o.aggregate_id AND p.status = 'PARKED')`, safely halting all subsequent versions for that specific entity while allowing sibling aggregates to proceed without partition stalls.
5. **F4.11 Cleanup Deletes Unpublished Rows**:
   - *Impact*: Purge queries inadvertently truncate pending or stalled events.
   - *Mitigation*: `FlowPurgeExpiredPublishedRecords` explicitly filters `WHERE status = 'PUBLISHED' AND created_at < NOW() - INTERVAL '48 hours'`. It is mathematically impossible for a `PENDING`, `IN_FLIGHT`, or `PARKED` row to be deleted by cleanup jobs.

---

## 5. Comprehensive Failure Mode Reference Matrix

| Code | Failure Scenario | Impact | Code Mitigation | Live Detection |
|---|---|---|---|---|
| **F1.1** | Dual-write attempt | Event loss or ghost event | Zero Kafka imports in business services; mandatory outbox insertion | CI dependency scanner |
| **F1.2** | Ambiguous commit | Duplicate aggregate & event | `outbox_idempotency_keys` table claims key first; retries return existing record | Idempotency conflict rate |
| **F1.5** | Concurrent aggregate write | Version collision | `UNIQUE (tenant_id, aggregate_type, aggregate_id, aggregate_version)` | Unique constraint violation metric |
| **F1.7** | Oversized payload | Broker rejection & partition block | Hard write-time check $\le 1\text{MB}$ in `rules.IsPayloadSizeValid` | Payload size histogram |
| **F1.9** | Float in JSON contract | Financial cents drift | `rules.HasFloatingPointNumbers` rejects floats in JSON numbers | Contract validation error counter |
| **F1.14** | Null key or aggregate ID | Skipping / unroutable event | `rules.ValidateOutboxInput` enforces non-empty strings on all identifiers | Validation failure metric |
| **F2.1** | Kafka down / broker halt | Postgres disk bloat | Records stay safely `PENDING`; backpressure monitored | Slot lag bytes & WAL free disk % |
| **F4.0** | Row lock held over network | DB connection exhaustion | Lease model: records claimed as `IN_FLIGHT`, send without open DB tx | Oldest active transaction age |
| **F4.2** | Multiple workers reorder versions | Downstream corruption | Batch ordered by `aggregate_version ASC`; hash partition affinity on `aggregate_id` | Consumer gap detector |
| **F4.6** | Worker lease expires mid-send | Overwrite / race condition | `MarkPublishedByWorker` fences updates on worker ownership & active lease | Reclaim counter spike |
| **F4.7** | Poison event | Partition outage | Retry count $\ge 5 \to \text{PARKED}$; queries block subsequent versions $k+1$ | Alert when `count(status='PARKED') > 0` |
| **F4.11** | Retention purge deletes pending | Silent data loss | Purge query strictly filters `WHERE status = 'PUBLISHED'` | Purge count vs published count |
| **F9.1** | Silent pipeline stoppage | Undetected loss | Synthetic canary emitted every ~30s (`EmitCanaryHeartbeat`) | Canary consumer latency SLO |

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

When an application updates a database and publishes an event to a message broker, it faces the distributed dual-write problem. If the database commits but the broker connection times out, downstream systems miss the event forever. If the broker write succeeds but the database rolls back, downstream systems process phantom data. Two-Phase Commit (2PC) is slow, fragile, and unsupported by modern cloud message brokers.

### 2. Internal Algorithmic Mechanics & State Invariants

The service writes its domain entity mutations and its outgoing events into a local database outbox table inside the exact same relational ACID transaction. If the transaction rolls back, both are rolled back; if it commits, both commit atomically. An asynchronous CDC tailer (Debezium reading PostgreSQL's logical replication stream) streams records from the outbox table directly into Kafka, guaranteeing at-least-once delivery with zero application latency overhead.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Locality of Invariant Enforcement (The Dual-Write Impossibility)
* **Design Philosophy & Epistemology**:
  Distributed consistency cannot span heterogeneous storage boundaries without heavy consensus protocols (2PC/XA). Because modern message brokers intentionally reject distributed transactions, any application thread attempting to write to a relational database and then emit to Kafka operates under an unresolvable race condition. The core philosophy is Locality of Invariant Enforcement: enforce atomic consistency exclusively within the single storage engine capable of ACID guarantees, deferring cross-network communication to asynchronous observation.
* **Threat Vector Defeated**: Split-brain state divergence. Specifically: 1) Silent Loss: Database commits balance deduction, application dies before network send; downstream systems never receive notice. 2) Phantom Events: Application emits event to Kafka, database transaction rolls back due to constraint violation; downstream systems fulfill orders that never existed.
* **Storage & State Invariant**:
```text
State(DomainEntity) AND State(OutboxRecord) IN Transaction(ACID)
Pr(DomainEntity_Committed AND NOT OutboxRecord_Committed) == 0
```
* **Implementation Mechanism**:
  The business service acquires a database connection from the pool. A single SQL transaction (BEGIN ... COMMIT) wraps both the aggregate state mutation and the append-only outbox insert. Zero network I/O to Kafka occurs inside the application request-response thread.
* **Non-Negotiable Trade-Off**: Database write amplification. Every business event duplicates storage space inside the relational database prior to publication, increasing write IOPS and table churn.

---

#### Principle 2: Temporal De-coupling & WAL-Level Event Extraction
* **Design Philosophy & Epistemology**:
  Application-level polling (SELECT * FROM outbox WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED) is an anti-pattern. Pollers induce relational table contention, pollute buffer pools, and suffer from commit-order visibility holes (where transaction T1 with an earlier timestamp commits after transaction T2, causing pollers to skip T1 permanently). The mature philosophy embraces Storage Log Tailing: the database's internal Write-Ahead Log (WAL) is the ultimate, immutable chronological record of truth. The application mutates state; the infrastructure observes the write log.
* **Threat Vector Defeated**: Database CPU exhaustion, lock queue starvation, and skipped-transaction visibility gaps.
* **Storage & State Invariant**:
```text
LSN(tx[i]) < LSN(tx[i+1]) ==> PublishOrder(tx[i]) < PublishOrder(tx[i+1])
LSN(ReplicationSlotFlush) <= LSN(KafkaAcknowledgedAck)
```
* **Implementation Mechanism**:
  PostgreSQL logical replication using the `pgoutput` plugin decodes outbox table row commits directly from disk WAL pages into binary logical changes. Debezium / CDC tailer extracts rows, routes them by `aggregate_type` to Kafka topics, and only advances the PostgreSQL replication slot watermark (`confirmed_flush_lsn`) when Kafka confirms an `acks=all` ISR write.
* **Non-Negotiable Trade-Off**: Disk volume starvation vulnerability. If Kafka becomes unreachable, PostgreSQL holds unconsumed WAL segments on disk. If unmonitored, WAL accumulation exhausts storage and forces the database into read-only emergency mode.

---

#### Principle 3: Honest Delivery Semantics (The Fallacy of Exactly-Once)
* **Design Philosophy & Epistemology**:
  Exactly-Once Delivery over an uncoordinated network is physically impossible (The Two Generals' Problem). The design philosophy is Honest Delivery Semantics: the producer guarantees At-Least-Once delivery, and intentionally delegates deduplication to the consumer. Striving for broker-side exactly-once across foreign storage engines produces fragile, complex distributed state machines.
* **Threat Vector Defeated**: Message loss during transient network partitions, broker leader re-elections, or worker crash-recovery cycles.
* **Storage & State Invariant**:
```text
DeliveryCount(Event[k]) >= 1 for all k in CommittedTransactions
State(Consumer, History) == State(Consumer, History + DuplicateEvent)
```
* **Implementation Mechanism**:
  The relay worker sends events with `acks=all` and exponential backoff retry. If the broker accepts the event but the network drops the ACK before reaching the relay worker, the worker retries, producing duplicate events on Kafka. Every outbox event includes an immutable, monotonic UUIDv7 `event_id` and an integer `aggregate_version` in its headers.
* **Non-Negotiable Trade-Off**: Network and consumer CPU amplification. Every downstream consumer must implement an inbox ledger or monotonic version guard to discard duplicate deliveries.

---

#### Principle 4: Causal Ordering vs System Gridlock (Per-Aggregate Poison Parking)
* **Design Philosophy & Epistemology**:
  In an event-driven system, Per-Entity Invariants Trump Global Throughput, but Never Halt Unrelated Entities. If event k for Customer X is malformed or rejected by the broker, halting the entire outbox pipeline halts operations for 100,000 unaffected customers (total system blackout). Skipping event k and publishing k+1 for Customer X violates causal consistency (destroying Customer X's financial balance). The philosophical resolution is Per-Aggregate Poison Parking: quarantine the single failing entity while letting the remaining universe proceed.
* **Threat Vector Defeated**: Complete system gridlock caused by poison pill messages, and out-of-order state corruption caused by naive retry-skip algorithms.
* **Storage & State Invariant**:
```text
Failed(Event[A, k]) ==> Blocked(Event[A, k+n]) for all n >= 1
Failed(Event[A, k]) does NOT block Event[B, m] for any B != A
```
* **Implementation Mechanism**:
  If publishing an event fails 5 consecutive times, its status transitions to `PARKED`. The relay query enforces causal continuity via relational anti-join: `SELECT * FROM outbox o WHERE o.status = 'PENDING' AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.aggregate_id = o.aggregate_id AND p.status = 'PARKED') ORDER BY o.aggregate_version ASC LIMIT 50`.
* **Non-Negotiable Trade-Off**: Operational complexity. Parked aggregates require human intervention or automated dead-letter repair workflows to unblock the quarantined entity.

---

#### Principle 5: Disposable Temporal Epochs (Zero-Lock Table Lifecycle)
* **Design Philosophy & Epistemology**:
  High-throughput event systems inevitably drown in their own historical waste if data lifecycle is an afterthought. Executing `DELETE FROM outbox WHERE status = 'PUBLISHED'` on a high-concurrency table generates massive PostgreSQL dead-tuple bloat, triggers aggressive table-locking autovacuum runs, and eventually brings transaction throughput to zero. The philosophy is Destruction via Partition Dropping: treat physical storage as disposable temporal epochs.
* **Threat Vector Defeated**: Database performance degradation, transaction wraparound vacuum stalls, and index fragmentation caused by relational dead tuples.
* **Storage & State Invariant**:
```text
OutboxTable == Union(DailyPartitions)
Cost(Drop(ExpiredPartition)) == O(1) instantaneous metadata operation
```
* **Implementation Mechanism**:
  The `outbox` table is declaratively partitioned by `created_at` (daily partitions). A scheduled background reaper verifies that all events in partition `P_{d-7}` have `status = 'PUBLISHED'` and that the CDC replication slot LSN has passed the partition boundary. The reaper executes `DROP TABLE outbox_y2026m11d02;`, reclaiming disk blocks immediately with zero lock contention.
* **Non-Negotiable Trade-Off**: Partition management overhead and static query requirements. Partition keys must be included in primary keys and unique constraints (`(created_at, id)`).

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is Change Data Capture (CDC) via WAL tailing philosophically superior to poller queries (`SELECT ... FOR UPDATE SKIP LOCKED`)?**
   * *Answer*: Poller queries place read-locks on transaction tables, compete with user-facing write traffic, and fail to see uncommitted transactions with earlier timestamps that commit late (commit-order hole). CDC reads the physical binary transaction log asynchronously without querying the relational engine, separating the transactional write plane from the event streaming plane.
2. **What occurs if the broker is unavailable for 6 hours? Does the system fail fast or absorb the shock?**
   * *Answer*: The system absorbs the shock. Because domain mutations and outbox records commit locally inside PostgreSQL, client writes continue uninterrupted at line rate. The outbox table accumulates records safely. The only limiting factor is PostgreSQL disk space for WAL retention and outbox rows.
3. **If event k for Account #123 fails due to a schema incompatibility, why must event k+1 for Account #123 be strictly blocked from publishing?**
   * *Answer*: If event k is AccountCreated and event k+1 is AccountDebited, publishing k+1 while k is stuck will cause downstream consumers to reject or corrupt the account state. In an aggregate stream, causal order is absolute: version k+1 cannot exist in reality without version k.
4. **Why is it a catastrophic design flaw to publish events directly from an application thread without the outbox table?**
   * *Answer*: It violates the Two Generals' theorem. Either the application commits to the database first and risks dying before network transmission, or it publishes to the broker first and risks database rollback. In both scenarios, the system enters an unrecoverable, undetected split-brain state.

---
## Authoritative References & Production Citations

* **[Chris Richardson: Microservices Pattern: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)**
  - *Architectural Rationale & Implementation Grounding*: The canonical architectural pattern defining how to publish events reliably without dual-write inconsistency.
* **[Gunnar Morling: Reliable Event Sourcing with the Transactional Outbox Pattern (Devoxx)](https://www.youtube.com/watch?v=hsw_Zpfdffw)**
  - *Architectural Rationale & Implementation Grounding*: Detailed lecture by Debezium project lead on WAL-based outbox routing, CDC connectors, and production failure modes.
* **[Debezium Outbox Event Router SMT Documentation](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)**
  - *Architectural Rationale & Implementation Grounding*: Standard Single Message Transformation (SMT) for routing outbox table mutations directly to partitioned Kafka topics.
* **[Martin Kleppmann: Transactions - Myths, Truths, and What's Next](https://www.youtube.com/watch?v=5ZjhNTM8XU8)**
  - *Architectural Rationale & Implementation Grounding*: Authoritative Strange Loop lecture on distributed consistency, dual-write impossibility, and log-centric architecture.

## Amendment 2026-11-07: Duplicate-externalId conflict mapping and Kafka relay headers

### Problem observed
1. `POST /api/v1/outbox` with a second request that reuses the same `externalId` (same tenant and source service) but no idempotency key returned `500 INTERNAL_ERROR`. A duplicate is a client-side condition and should be a conflict.
2. Messages published by the relay (`RelayBatch`) arrived on Kafka with no record headers. A console consumer showed `NO_HEADERS`, although the outbox row stores `event_type`, `schema_version` and `source_service` in its `headers` column. Consumers therefore could not route or version-check without parsing the payload.

### Root cause
1. `transactional-outbox.postgres.repository.adapter.go: Save / SaveAndImmediateDelete` returned the raw `*pq.Error` for unique violations (`outbox_external_uk`, `outbox_aggregate_version_uk`). The service wrapped it with `%w` and the handler never matched it to a 409 (RC1, RC2). The in-memory adapter did not enforce the idempotency key, so unit tests on it could not see the defect (RC5).
2. `ProducerPort.Publish(ctx, topic, key, payload)` has no headers parameter, and `RelayBatch` called it directly, so the stored headers were dropped at the last hop (RC4-style gap: the stored contract was never exercised end to end).

### Decision / fix
- `mapInsertError` in the Postgres adapter converts SQLSTATE 23505 into an `ApplicationError` conflict: `outbox_external_uk` -> "an outbox event with the same externalId already exists for this tenant and source service"; `outbox_aggregate_version_uk` -> "an outbox event for this aggregate version already exists"; any other unique violation -> "duplicate outbox event". `OutboxService.StageEvent` now uses `errors.As` and returns the `ApplicationError` unwrapped, and the handler uses `errors.As`. The memory adapter now enforces the idempotency key so both adapters behave the same.
- Headers: a new optional interface `producers.HeaderedProducerPort` (`PublishWithHeaders`) and helper `producers.PublishRecord`. The Kafka adapter sends headers in sorted key order; the memory producer records them. `ProducerPort` itself is unchanged, so producers that do not support headers keep working (the helper falls back to plain `Publish`). We chose an optional extension over changing `ProducerPort` to avoid breaking every implementation and mock.
- Contract now enforced on relay (verified in `outbox.service.go`):
  - Kafka topic == the row's `aggregate_type`; message key == `aggregate_id`; value == `payload`.
  - Headers `event_type`, `schema_version`, `source_service` are always set at staging from the request, and any custom `headers` supplied in the request are carried too. They are sent as Kafka record headers.

### API-visible behavior change
- `POST /api/v1/outbox` duplicate `externalId` without idempotency key: `500` -> `409 CONFLICT` with a domain message. Clients that treated 500 as retryable will now see a non-retryable 409. Marked as a behavior change, arguably breaking for clients that special-cased 500.
- Kafka consumers now see record headers; additive.

### Verification
- Live (image `chiefj/event-driven-platform:fixes`, compose stack): duplicate externalId returned 409; the relayed Kafka record showed `event_type:E,schema_version:1,source_service:s`.
- Unit tests added in `src/features/transactional-outbox/tests/outbox_headers_conflict_test.go`: `TestOutboxDuplicateExternalIDIsConflict`, `TestOutboxRelayCarriesHeaders`.

### Remaining gaps
- The unique-violation mapping in the Postgres adapter has no Postgres-backed test in CI (only the live check above); Postgres-dependent integration tests skip silently (RC5).
- Topic provisioning is an operational prerequisite, not enforced by code (see RC6 below).

### Operational prerequisite (RC6): topic must exist before relay
The relay publishes to the Kafka topic named by `aggregate_type`, and the broker has topic auto-creation disabled. If the topic does not exist, publishing fails with "Unknown Topic Or Partition", the row is marked failed on each attempt, and after 5 retries it is PARKED. This is not a code defect. Create topics (with the intended partition count) before staging events for a new `aggregate_type`; see `failure-cases.md` for the recovery procedure.

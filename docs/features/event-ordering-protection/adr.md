# Architectural Decision Record: Event Ordering Protection Engine (WP-4)

## 1. Title & Metadata
* **Status**: APPROVED
* **Date**: 2026-11-04
* **Decision Makers**: Platform Architecture Guild, Chief Distributed Systems Architect
* **Governing Specification**: [Event Ordering Protection Decision Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/event-ordering-protection/business-logic.md)

---

## 2. Context & Problem Statement
In distributed event-driven and event-sourced topologies operating across cloud infrastructure, event streams are vulnerable to causal disruptions. Upstream multi-threaded pollers, out-of-order network retries, Kafka partition rebalances, and database transaction timing skews break sequential guarantees.

When an event stream delivers version $V=3$ before $V=2$:
1. **Silent State Corruption (OR-05 / OR-06)**: Blindly executing delta mutations out-of-order computes corrupt aggregate balances and illegal state transitions. Applying $V=2$ after $V=3$ can silently revert business intent or produce invalid financial records.
2. **Partition Starvation vs. Head-of-Line Blocking (OR-08)**: Naive ordering engines halt the entire Kafka consumer partition when an out-of-order message is encountered. Because hundreds of unrelated aggregates share that partition, stopping consumer progress freezes healthy tenants, transforming a local gap into a systemic cascading platform outage.
3. **Ghost Sequence Gaps from Non-Transactional ID Sequences (F3.2)**: Relying on database `SEQUENCE` objects or monotonic auto-increments causes permanent unfillable gaps when transactions roll back or sequence blocks are evicted from cache. Downstream consumers stall permanently waiting for an event version that was never committed.
4. **Zombie Consumers & Split-Brain Writes (OR-10)**: During partition rebalances, a slow consumer thread executing a long garbage collection pause may wake up and commit state updates after a newly assigned consumer has already taken over partition processing, causing out-of-order state mutations and duplicate processing.
5. **Payload Mutation on Re-delivery (OR-06)**: Transport-level deduplication that matches only `event_id` or `version` ignores payload corruption or malicious payload tampering during retry storms.

---

## 3. Decision Drivers & Invariants (OR-01 to OR-13)

* **OR-01: Gapless Per-Aggregate Sequence at Source**: Versions $V=1, 2, 3 \dots$ must be gapless per aggregate instance. Versions are assigned via conditional update on the aggregate row (`SET version = expected + 1 WHERE version = expected`) within the business database transaction. Database sequence objects are strictly prohibited.
* **OR-02: Commit Order Equals Version Order**: Row locks acquired during aggregate mutation are held until the database transaction commits, guaranteeing that version $N+1$ cannot commit before version $N$.
* **OR-03: Single Ordered Stream per Aggregate**: All events for a specific aggregate instance must route to the exact same Kafka topic with message key equal to `aggregate_id`. Partition count alterations must use keyed migration envelopes.
* **OR-04: Non-Overflowing 64-Bit Versions**: Sequence numbers are strictly typed as signed 64-bit integers (`BIGINT`) end-to-end to prevent integer overflow truncation.
* **OR-05: Exact-Next Version Application**: A consumer worker applies event $E$ if and only if $E.version == last\_applied\_version + 1$. Every other relationship is classified and rejected, skipped, or held.
* **OR-06: Same Version Implies Same Content**: Every applied event persists its SHA-256 payload digest alongside `last_applied_version`. An incoming event with $E.version \le last\_applied\_version$ must have an identical SHA-256 hash (idempotent skip). Any hash divergence is treated as a critical data integrity incident.
* **OR-07: Durable Held Events**: Events that arrive ahead of sequence ($E.version > last\_applied\_version + 1$) are durably persisted to PostgreSQL table `sequence_held_event` within the same transaction that advances the Kafka consumer offset. In-memory buffering is prohibited to eliminate message loss across container restarts.
* **OR-08: Park the Key, Not the Partition**: When an aggregate encounters a gap, only that specific `aggregate_id` is marked `is_held = true`. The consumer immediately commits the Kafka offset and continues consuming subsequent messages for other healthy aggregates on that partition.
* **OR-09: Gap Resolution Never Fabricates**: Gaps can only be filled by the original missing event (via producer re-emit API) or by an authoritative versioned snapshot from the producer. Fabricating synthetic no-op events without audit provenance is prohibited.
* **OR-10: Single Writer per Key via Epoch Fencing**: Every state modification verifies that the consumer's `owner_epoch` matches or exceeds the stored partition epoch. Writes from stale epochs are rejected with conflict errors.
* **OR-11: Explicit Bootstrap**: The first-seen event for an aggregate must have $V=1$, unless the consumer explicitly bootstraps state using an authoritative snapshot up to version $N$.
* **OR-12: Aggregate Scope Only**: Ordering guarantees apply strictly within individual aggregate boundaries. Cross-aggregate ordering is neither guaranteed nor assumed.
* **OR-13: Zero Gap Rate SLO**: Any confirmed sequence gap exceeding the holding TTL triggers immediate operator escalation and automated re-emit requests.

---

## 4. Ordering Break Map (H1 to H8)

The architecture isolates and mitigates every point in the pipeline where ordering guarantees can break:

| Hop Code | Pipeline Stage | Vulnerability | Engineering Mitigation |
| :--- | :--- | :--- | :--- |
| **H1** | Producer Sequence Assignment | Race conditions or non-transactional sequence generation | Conditional update on aggregate row in business transaction; no DB sequence objects |
| **H2** | Producer Transaction Commit | Transaction $N+1$ committing before transaction $N$ | Aggregate row lock held through transaction commit |
| **H3** | Outbox Storage | Selective filtering or skipping of internal events | Zero filtering on aggregate stream outbox; every internal event published with stream flag |
| **H4** | Outbox Relay / CDC | Multi-worker poller races or checkpoint loss | Key-hashed worker lanes (`hash(aggregate_id) % workers`); status-based cursor polling |
| **H5** | Kafka Transport | Unclean leader election, retries across producer sessions | Idempotent producer enabled (`enable.idempotence=true`, `acks=all`, `max.in.flight=5`); topic partition count frozen |
| **H6** | Consumer Ingestion | Multi-threaded thread pool reordering within partition | Key-hashed worker lanes; transactional consumer DB offset tracking |
| **H7** | Consumer Sequence Validation | Accepting $V \ge last + 1$ instead of strict equality | Domain Rule validation: exact-next check ($V == last + 1$) only |
| **H8** | Topology & Resharding | Resizing partitions or re-routing topics | Never resize active ordered topics; execute stream migration cutover per [EV-09] |

---

## 5. Entity-Relationship Data Model & Database Writing Rules

The storage gate complies with [05-db-writing-rules.md](file:///home/btpl-lap-22/live/System-Architect/llm-obs-policies/rules/folder-structure/05-db-writing-rules.md), enforcing the mandatory 5-role identifiers and immutability triggers.

### 5.1 The 5 Identifier Roles Mapping
1. **Role 1 (Surrogate PK)**: UUIDv7 generated internally (`id`).
2. **Role 2 (Public ID)**: Regex-constrained external strings (`public_id` matching `^seq_[0-9a-zA-Z]{26}$` for trackers and `^hld_[0-9a-zA-Z]{26}$` for held records).
3. **Role 3 (Tenant Scope)**: `tenant_id` leading compound primary keys.
4. **Role 4 (Natural Business Key)**: Composite unique constraint on `(tenant_id, source_service, external_id)`.
5. **Role 5 (Correlation Trace)**: `correlation_id` UUID propagated from upstream OpenTelemetry trace context.

### 5.2 Entity-Relationship Diagram

```mermaid
erDiagram
    aggregate_sequence_tracker {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (seq_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        varchar aggregate_id "Business aggregate identifier"
        varchar stream_name "Topic or stream identifier"
        bigint last_applied_version "Latest sequentially applied version"
        varchar last_payload_hash "SHA-256 digest of last applied event"
        boolean is_held "Flag indicating whether aggregate is parked"
        varchar hold_reason "Reason for hold (GAP, CHECKSUM_MISMATCH)"
        bigint owner_epoch "Consumer partition generation fencing epoch"
        timestamptz created_at "Record creation timestamp"
        timestamptz updated_at "Record last update timestamp"
    }

    sequence_held_event {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (hld_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        varchar aggregate_id "Target aggregate identifier"
        bigint aggregate_version "Held version waiting for predecessors"
        varchar event_id "Original event identifier"
        varchar payload_hash "SHA-256 digest of held payload"
        bytea payload "Serialized payload bytes"
        varchar partition_key "Kafka message partition key"
        bigint kafka_offset "Committed message offset"
        timestamptz held_at "Timestamp event was placed on hold"
        timestamptz expires_at "Holding TTL expiration timestamp"
    }

    aggregate_sequence_tracker ||--o{ sequence_held_event : "buffers future versions for"
```

---

## 6. System Execution Architecture & Sequence Diagrams

### 6.1 Out-of-Order Gap Detection & Per-Key Parking (OR-07, OR-08)
When version $V=3$ arrives before $V=2$, only aggregate `Order-101` is parked in `aggregate_sequence_tracker`. Subsequent events for healthy aggregates (e.g. `Order-102`) process without delay.

```mermaid
sequenceDiagram
    autonumber
    actor Producer as Upstream Producer
    participant Log as Kafka Partition
    participant Worker as Consumer Worker
    participant State as aggregate_sequence_tracker
    participant Store as sequence_held_event
    participant DLQ as Dead Letter Quarantine

    Producer->>Log: Publish Order-101 V=3 (Offset 50)
    Note over Log: V=2 delayed by upstream network retry
    Worker->>Log: Fetch batch (Offset 50)
    Worker->>State: SELECT tracker FOR UPDATE WHERE aggregate_id = 'Order-101'
    State-->>Worker: last_applied_version = 1, is_held = false
    
    rect rgb(255, 235, 235)
        Note over Worker: Gap detected: Expected V=2, Received V=3
        Worker->>Store: INSERT sequence_held_event (V=3, payload, offset=50)
        Worker->>State: UPDATE tracker SET is_held = true, hold_reason = 'GAP'
        Worker->>Log: Commit offset 50 (Partition does NOT stall)
    end

    Producer->>Log: Publish Order-102 V=1 (Offset 51)
    Worker->>Log: Fetch batch (Offset 51)
    Worker->>State: SELECT tracker FOR UPDATE WHERE aggregate_id = 'Order-102'
    State-->>Worker: last_applied_version = 0, is_held = false
    Worker->>State: UPDATE tracker SET last_applied_version = 1
    Note over Worker: Order-102 executes uninterrupted
```

### 6.2 Sequential Drain & Auto-Unparking Workflow
When the delayed missing event $V=2$ finally arrives, it is applied directly to the aggregate, which then triggers a Hoare loop draining all consecutive held versions ($V=3, V=4 \dots$) from storage.

```mermaid
sequenceDiagram
    autonumber
    actor Producer as Upstream Producer
    participant Worker as Consumer Worker
    participant State as aggregate_sequence_tracker
    participant Store as sequence_held_event
    participant Domain as Domain Business Logic

    Producer->>Worker: Delayed Order-101 V=2 arrives
    Worker->>State: SELECT tracker FOR UPDATE WHERE aggregate_id = 'Order-101'
    State-->>Worker: last_applied_version = 1, is_held = true
    
    rect rgb(235, 255, 235)
        Note over Worker: Exact-next match: V=2 == 1 + 1
        Worker->>Domain: Apply V=2 State Mutation
        Worker->>State: UPDATE tracker SET last_applied_version = 2
        
        loop Drain Held Events
            Worker->>Store: SELECT held events WHERE aggregate_id = 'Order-101' ORDER BY version ASC
            Store-->>Worker: Returns [Held Event V=3]
            Note over Worker: CanDrainSequence(2, 3) is TRUE
            Worker->>Domain: Apply V=3 State Mutation
            Worker->>Store: DELETE FROM sequence_held_event WHERE id = held_v3.id
            Worker->>State: UPDATE tracker SET last_applied_version = 3
        end
        
        Worker->>State: UPDATE tracker SET is_held = false, hold_reason = NULL
    end
    Note over Worker: Order-101 fully caught up and unparked
```

### 6.3 Duplicate vs. Integrity Violation Handling (OR-06)
Redelivered events with identical SHA-256 hashes are acknowledged and skipped. If an incoming duplicate has a different SHA-256 hash, execution immediately halts, the aggregate is quarantined, and a security alert is emitted.

```mermaid
sequenceDiagram
    autonumber
    actor Producer as Upstream Producer
    participant Worker as Consumer Worker
    participant State as aggregate_sequence_tracker
    participant DLQ as Quarantine Manager

    Producer->>Worker: Order-101 V=2 arrives (Duplicate)
    Worker->>State: SELECT tracker FOR UPDATE
    State-->>Worker: last_applied_version = 2, last_payload_hash = 'sha256_hash_abc'
    
    alt Payload SHA256 matches stored hash
        Note over Worker: Benign duplicate: acknowledge and drop
    else Payload SHA256 != stored hash
        rect rgb(255, 220, 220)
            Note over Worker: Integrity violation! Mutated payload on same version
            Worker->>State: UPDATE tracker SET is_held = true, hold_reason = 'CHECKSUM_MISMATCH'
            Worker->>DLQ: Route to Quarantine with INCIDENT alert
        end
    end
```

---

## 7. Real-World Failure Handling & Edge Case Analysis

### 7.1 Matrix of Failure Modes

| Scenario Code | Root Cause | Impact | Detection | Autonomous Recovery Action |
| :--- | :--- | :--- | :--- | :--- |
| **FL-ORD-01** | Duplicate event arrival with identical SHA-256 | Redelivery on producer retry | `version <= last_applied && hash == stored_hash` | Idempotent skip; log trace; commit offset. Zero business mutation. |
| **FL-ORD-02** | Duplicate event with conflicting SHA-256 digest | Upstream code bug mutating historical payloads or MITM tampering | `version <= last_applied && hash != stored_hash` | Immediately park key (`is_held = true, reason = CHECKSUM_MISMATCH`), route to quarantine DLQ, alert security guild. |
| **FL-ORD-03** | Gap detected ($V > last\_applied + 1$) | Producer multi-thread outbox race, packet drop, or rebalance | `version > last_applied + 1` | Persist in `sequence_held_event`, mark tracker `is_held = true, reason = GAP`, advance Kafka offset without stopping partition. |
| **FL-ORD-04** | Holding TTL expiration | Missing predecessor permanently lost at source | `now() > expires_at` on held event | Trigger automated request to Producer Re-emit API. If unresolvable within SLA, request authoritative snapshot resync. |
| **FL-ORD-05** | Zombie consumer thread post-rebalance | Stale consumer commits out-of-order write to store | `incoming.owner_epoch < tracker.owner_epoch` | Reject write with epoch fence conflict error. Zombie worker terminates transaction. |
| **FL-ORD-06** | Consumer crash during cascade drain | Partial application of held queue | Crash recovery during transaction commit | PostgreSQL transaction rollback restores held events. On restart, drain loop safely resumes from last committed version. |
| **FL-ORD-07** | Partial drain cascade | Interleaved future events buffered (e.g. $V=3, V=5$ held, $V=2$ arrives) | CanDrainSequence fails on second item ($5 \ne 3 + 1$) | Apply $V=2$ and $V=3$; stop drain loop; remain `is_held = true, reason = GAP`. |
| **FL-ORD-08** | Ghost gap from DB sequence usage | Rollbacks at producer burn numbers | Producer re-emit confirms version was never written | Alert on non-recoverable sequence gap; trigger snapshot resync. |
| **FL-ORD-09** | Truncated topic bootstrap | New consumer deployed on stream where $V=1$ was aged out | First-seen event has $V > 1$ with clean tracker | Reject immediate apply; trigger `SNAPSHOT_BOOTSTRAP` routine from snapshot store. |
| **FL-ORD-10** | Held buffer overflow | Massive burst of out-of-order events exceeding quota | Count of held records exceeds buffer capacity (e.g. $> 100$) | Reject additional holds; route excess to cold storage dead-letter quarantine. |

### 7.2 Epoch Fencing Against Zombie Consumers
When a Kafka rebalance occurs, consumer instance $C_1$ may pause (e.g. Stop-The-World GC). The coordinator revokes partition ownership and assigns it to $C_2$, incrementing the partition epoch from $E=4$ to $E=5$.
When $C_1$ awakens, its write to `aggregate_sequence_tracker` contains `owner_epoch = 4`. The database update executes:
```sql
UPDATE aggregate_sequence_tracker 
SET last_applied_version = $new_version, owner_epoch = $incoming_epoch 
WHERE tenant_id = $tenant_id AND aggregate_id = $aggregate_id AND owner_epoch <= $incoming_epoch;
```
Because $C_2$ already bumped `owner_epoch = 5`, $C_1$'s update modifies zero rows, triggering a transaction rollback and aborting the stale write.

---

## 8. OpenTelemetry Tracing & Observability Architecture

Every sequence transition and hold decision exports standard OpenTelemetry span attributes:

| Metric / Span Attribute | Type | Description |
| :--- | :--- | :--- |
| `event.ordering.action` | Span Attribute | `APPLIED`, `SKIPPED_DUPLICATE`, `HELD_GAP`, `DRAINED`, `HOLD_CHECKSUM_MISMATCH` |
| `event.aggregate.id` | Span Attribute | Unique identifier of the business aggregate |
| `event.aggregate.version` | Span Attribute | Monotonic sequence number of the processed event |
| `event.aggregate.expected_version` | Span Attribute | Expected next version ($last\_applied + 1$) |
| `event.partition.epoch` | Span Attribute | Fencing epoch of the consumer worker |
| `event_ordering_held_keys_gauge` | Metric Gauge | Number of currently parked aggregates per tenant and stream |
| `event_ordering_gap_duration_seconds` | Metric Histogram | Duration between when an aggregate was parked and when missing events arrived |
| `event_ordering_checksum_mismatch_total` | Metric Counter | Total number of integrity violations detected (Alert threshold: $> 0$) |

Trace context propagation is preserved end-to-end:
```
Producer Span [traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01]
  └── Relay Span
        └── Kafka Record Header
              └── Consumer Worker Span
                    ├── Store Held Event Span (if gap detected)
                    └── Evaluate Sequence Span [correlation_id preserved]
```

---

## 9. Verification & Test Plan

1. **Exact-Next Application**: Event $V=1$ applies; $V=2$ applies; aggregate sequence reaches $2$.
2. **Duplicate Idempotency**: Event $V=2$ re-delivered with identical SHA-256 payload digest returns `SKIPPED_DUPLICATE`; state unchanged.
3. **Payload Tampering Detection**: Event $V=2$ re-delivered with modified payload bytes transitions aggregate to `is_held = true`, sets reason to `CHECKSUM_MISMATCH`, and rejects mutation.
4. **Out-of-Order Gap Parking**: When aggregate is at $V=1$, incoming $V=3$ is persisted to `sequence_held_event`; tracker transitions to `is_held = true, reason = GAP`.
5. **Sequential Auto-Drain**: While parked at $V=1$ with held $V=3$, incoming $V=2$ applies immediately, drains held $V=3$, advances sequence to $3$, and unparks aggregate.
6. **Multi-Version Drain**: Aggregate at $V=1$ with held $V=3, V=4, V=5$. Arrival of $V=2$ sequentially applies $V=2 \to V=3 \to V=4 \to V=5$, emptying held store.
7. **Epoch Fencing Enforcement**: Consumer with stale `owner_epoch` receives conflict error when attempting to evaluate or drain sequences.


## Amendment 2026-11-07 (second pass): tenant scope required

`GET /events/held` returns 400 when `tenant_id` is missing (previously 200 with `data:null`).

Verified live on the compose-deployed image (`chiefj/event-driven-platform:v2`) and by unit tests; see ADR-0002 "Closed in a second pass".

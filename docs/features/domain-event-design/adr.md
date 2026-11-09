# ADR: Standardized Domain Event Envelope, Monotonic UUIDv7, and Causality Governance

* **Status:** Accepted
* **Date:** 2026-11-06
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `src/features/domain-event-design/`, `src/shared/types/`, and `messaging/engine/client/`

---

## 1. Context and Problem Statement

Downstream consumers in an asynchronous event-driven system depend on strict invariants to preserve causal ordering, avoid duplicate effects, prevent infinite causal loops, and maintain distributed traces across service boundaries. When domain events are defined with ad-hoc payloads or unformatted timestamps:
- Timezones mismatch and clock drift leads to erroneous out-of-order evaluations.
- Causation and correlation IDs are lost, rendering root-cause analysis and loop detection impossible.
- Producers use CRUD verbs (`OrderUpdated`, `ItemDeleted`), turning events into anemic state notifications that leak internal persistence models instead of capturing immutable domain facts.
- Multi-aggregate commands create race conditions where downstream consumers react to partial business transactions.

---

## 2. Decision Drivers

* **Monotonic Chronological Ordering & Identity**: Event identifiers must be time-ordered at the boundary using UUIDv7 without distributed lock overhead.
* **Gapless Aggregate Versioning (DE-02)**: Aggregate versions must increment monotonically ($v \to v+1$ or $v \to v+K$) enforced by an authoritative OCC lock ledger.
* **Semantics Class Governance (DE-03)**: Every event type explicitly declares its semantics class (`STATE`, `DELTA`, `NOTIFICATION`).
* **Fact-Time Reference Data (DE-04)**: Historical domain facts must include snapshot reference data at the time of the event (unit price, applied tax rate, currency) so consumers never look up mutable reference data.
* **Causal Loop Detection**: Envelopes must enforce `causation_depth` and causal lineage tracking to abort recursive loops ($A \to B \to A$) at a bounded depth ($\le 16$).
* **Multi-Aggregate Completeness Barrier**: Events emitted across multiple aggregates in one command declare `transaction_group_id` and `group_size` so consumers can wait for group completeness.

---

## 3. Considered Options

* **Option 1: Raw JSON Event Payloads**
  - *Cons:* No standardization; every consumer parses different metadata keys. Trace context and tenant isolation are routinely omitted.
* **Option 2: CloudEvents Specification without Validation**
  - *Cons:* Generic attribute bag lacking strict platform rules (no aggregate sequence numbers, no enforced UUIDv7 validation, loose timestamp formats).
* **Option 3: Hexagonal Event Envelope (`EventEnvelope[T any]`) with Strict Validation**
  - *Pros:* Strongly typed generic envelope; validates UUIDv7, RFC 3339 UTC, gapless aggregate sequence, past-tense naming syntax, causation depth, and multi-aggregate groups at serialization time.

---

## 4. Decision Outcome

We selected **Option 3: Hexagonal Event Envelope (`EventEnvelope[T any]`) with Strict Validation**.

### Envelope Structure (`src/features/domain-event-design/contracts/domain-event-design.types.go`)

```json
{
  "event_id": "0194857b-7b2a-7963-8a9d-5a8b2413e001",
  "event_type": "commerce.order.placed.v1",
  "schema_version": "1.0.0",
  "aggregate_type": "commerce.order",
  "aggregate_id": "ord_99812",
  "aggregate_version": 1,
  "tenant_id": "tenant_enterprise_01",
  "effective_at": "2026-11-06T16:00:00.000000000Z",
  "recorded_at": "2026-11-06T16:00:00.000000000Z",
  "causation_id": "cmd_81238",
  "correlation_id": "req_019283",
  "causation_depth": 1,
  "transaction_group_id": "txg_0194857b",
  "group_size": 1,
  "semantics_class": "STATE",
  "visibility": "PRIVATE",
  "pii_classification": "NONE",
  "source_service": "order-service",
  "trace_context": {
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7"
  },
  "payload": { ... }
}
```

### Invariants Enforced
1. **Event Identifier (`event_id`)**: Must parse as an RFC 9562 UUIDv7. Non-v7 UUIDs and arbitrary strings are rejected.
2. **Type Naming Convention (`event_type`)**: Must match regex `^[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.v[0-9]+$`. CRUD verbs are disallowed.
3. **Timestamps (`effective_at`, `recorded_at`)**: Strictly RFC 3339 UTC ending with `Z`.
4. **Aggregate Version (`aggregate_version`)**: Strictly $\ge 1$ and gapless.
5. **Causation Depth (`causation_depth`)**: Strictly $\ge 0$ and $\le 16$.
6. **Terminal Aggregate Lock**: If aggregate has emitted a terminal event (`is_terminal = true`), all further version reservations fail with 422 Unprocessable Entity.

---

## 5. Consequences

### Positive
* Single source of truth for all events across the fleet.
* Zero index fragmentation when persisting events in B-Tree or LSM database tables.
* Decoupled domain payload via Go generics.
* Bounded loop detection and multi-aggregate completeness guards.

### Negative / Tradeoffs
* Wire overhead of metadata attributes (~200-250 bytes per event).
* Strict deserialization rejects events from non-compliant external producers (requires an anti-corruption layer).

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

In distributed architectures, services that communicate via database sharing or direct CRUD RPCs become tightly coupled. When service A dictates the database schema or internal entity models of service B, neither service can deploy independently. Domain Events decouple systems by communicating what happened in domain terms rather than how data is stored.

### 2. Internal Algorithmic Mechanics & State Invariants

Domain events represent immutable historical facts emitted by aggregate roots. Every event payload adheres to a standardized envelope (CloudEvents v1.0.2) containing event_id (UUIDv7), tenant_id, correlation_id, causation_id, timestamp, and a strictly versioned payload schema. Aggregate roots record events during state mutation and emit them exclusively through the transactional outbox.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Past-Tense Historical Facthood
* **Design Philosophy & Epistemology**:
  Language dictates thought. An event is not a command, a request, or a speculative suggestion; it is a declaration of an incontrovertible historical fact that has already occurred in reality. The philosophy is Past-Tense Immutability: events are named with passive, past-tense verbs (OrderPlaced, PaymentCaptured). You cannot cancel an event; you can only record a subsequent event (OrderCancelled).
* **Threat Vector Defeated**: Semantic confusion where consumers treat events as requests that can be rejected, leading to inconsistent distributed state.
* **Storage & State Invariant**:
```text
EventName ends with PastTenseVerb (e.g., EntityActioned)
Event cannot be rejected by consumer; consumer must adapt its model to reality
```
* **Implementation Mechanism**:
  Platform domain models require event naming conventions enforced by schema linters in CI. Events contain timestamps, entity IDs, and the exact state delta resulting from the completed transaction.
* **Non-Negotiable Trade-Off**: Requires compensation logic in domain workflows rather than simple synchronous error rollbacks.

---

#### Principle 2: Self-Contained Domain Payloads (Event-Carried State Transfer)
* **Design Philosophy & Epistemology**:
  An event that contains only an ID (`OrderCreated: {id: 123}`) forces every downstream consumer to make a synchronous RPC query back to the originating service to fetch details. Under high load, this triggers catastrophic 'Thundering Herd' RPC storms that crash the core database. The philosophy is Self-Sufficiency: events carry sufficient domain state for downstream consumers to perform their functions without calling back.
* **Threat Vector Defeated**: Thundering herd query storms and tight operational coupling where an event burst overloads the producer's read database with RPC lookups.
* **Storage & State Invariant**:
```text
Payload contains core entity state snapshot required for 90% of downstream projection logic
CallbackRPCQueries == 0 for standard workflows
```
* **Implementation Mechanism**:
  Domain event schemas include entity attributes (e.g., customer details, line items, prices) alongside identity fields, enabling downstream consumers to build materialized views autonomously.
* **Non-Negotiable Trade-Off**: Larger message payload size and potential distribution of sensitive data requiring field-level encryption.

---

#### Principle 3: Schema Decoupling & Semantic Versioning
* **Design Philosophy & Epistemology**:
  Producers and consumers deploy on independent schedules. If a producer modifies an event schema without backward compatibility, downstream consumers crash simultaneously. The philosophy is Expand and Contract: event schemas evolve through additive, non-breaking modifications; fields are never deleted or repurposed in-place.
* **Threat Vector Defeated**: Platform-wide consumer deserialization failures caused by uncoordinated producer schema modifications.
* **Storage & State Invariant**:
```text
Schema(Event[v2]) is Backward Compatible with Schema(Event[v1])
Required fields can never be added without defaults; existing fields can never be removed in minor versions
```
* **Implementation Mechanism**:
  Schemas are versioned according to Semantic Versioning and validated in CI against the Schema Registry using BACKWARD compatibility modes before PR merges are permitted.
* **Non-Negotiable Trade-Off**: Long-term accumulation of deprecated fields in schemas requiring structured deprecation lifecycles.

---

#### Principle 4: Explicit Aggregate Boundary Identity
* **Design Philosophy & Epistemology**:
  Events originating from a domain aggregate must clearly identify the sovereign aggregate root that produced them. Without explicit aggregate identifiers and versions, downstream systems cannot detect out-of-order deliveries or enforce single-writer consistency. The philosophy is Aggregate Sovereignty: every event is anchored to its originating aggregate root ID and monotonic version number.
* **Threat Vector Defeated**: Out-of-order state corruption and concurrent update race conditions in downstream read models.
* **Storage & State Invariant**:
```text
Event.Headers.AggregateID != NULL
Event.Headers.AggregateType != NULL
Event.Headers.AggregateVersion == PreviousVersion + 1
```
* **Implementation Mechanism**:
  Platform event envelopes mandate standard metadata headers: `aggregate_id`, `aggregate_type`, `aggregate_version`, `tenant_id`, and `occurred_at`. These headers are populated directly by the domain repository layer during event generation.
* **Non-Negotiable Trade-Off**: Strict requirement that domain services be organized around Domain-Driven Design (DDD) aggregate boundaries.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **What is the architectural difference between a Domain Event and an Integration Event?**
   * *Answer*: A Domain Event reflects fine-grained domain state changes within a single bounded context (e.g., OrderLineAdded). An Integration Event is a curated, stabilized external contract emitted across organizational boundaries for external consumption (e.g., OrderPlaced).
2. **Why should floating-point numbers be strictly prohibited in financial domain events?**
   * *Answer*: Floating-point numbers (IEEE 754) suffer from binary rounding errors during serialization across different programming languages (e.g., Go vs Java vs JavaScript), leading to cents drift in financial calculations. Financial events must use integer minor units (cents) or fixed-precision string decimals.
3. **When is Event Notification (thin event with ID only) preferred over Event-Carried State Transfer?**
   * *Answer*: Event Notification is preferred when the payload is massive (e.g., gigabyte video files, massive datasets) or contains highly sensitive classified data that cannot be broadcast onto shared pub/sub buses.

---
## Authoritative References & Production Citations

* **[Eric Evans: Domain-Driven Design: Tackling Complexity in the Heart of Software](https://www.oreilly.com/library/view/domain-driven-design-tackling/0321125215/)**
  - *Architectural Rationale & Implementation Grounding*: The seminal work establishing Domain Events, Aggregate Roots, Bounded Contexts, and ubiquitous language.
* **[CNCF CloudEvents Specification v1.0.2](https://github.com/cloudevents/spec)**
  - *Architectural Rationale & Implementation Grounding*: Industry-standard specification for describing event data in common, interoperable formats across cloud ecosystems.
* **[Vaughn Vernon: Implementing Domain-Driven Design (The Red Book)](https://kalele.io/books/implementing-domain-driven-design/)**
  - *Architectural Rationale & Implementation Grounding*: In-depth guide on domain event modeling, event-driven architecture integration, and event storming methodology.
* **[Martin Fowler: What do you mean by Event-Driven?](https://martinfowler.com/articles/201701-event-driven.html)**
  - *Architectural Rationale & Implementation Grounding*: Essential taxonomy clarifying Event Notification, Event-Carried State Transfer, Event Sourcing, and CQRS.

## Amendment 2026-11-07: Transaction-group persistence in envelope build

### Problem observed
`POST /api/v1/domain-events/envelope/build` with a `transaction_group_id` returned `200` but no row appeared in `transaction_group_ledger`.

### Root cause
`UpsertTransactionGroup` used parameter `$9` both as a bare value and inside `CASE WHEN ... THEN $9`, so Postgres failed with "inconsistent types deduced for parameter $9". `BuildCanonicalEnvelope` discarded that error (`_, _ =`, RC3), and also discarded the causal-chain write error, hence a 200 with nothing persisted. Unit tests on the in-memory adapter could not hit the SQL typing problem (RC5).

### Decision / fix
- Explicit `$9::timestamptz` casts in `UpsertTransactionGroup` (insert value, CASE branches, and the `ON CONFLICT` update), with `'1970-01-01 00:00:00+00'::timestamptz` as the placeholder.
- `BuildCanonicalEnvelope` now returns `ErrPersistence` (wrapped, mapped to a generic 500) if either the causal-chain entry or the transaction-group upsert fails. We fail the request rather than return an envelope whose group bookkeeping was lost, because group completion tracking depends on those rows.
- A group requires `group_size > 0` (stated in the verified fixes; the enforcement site was not re-read for this amendment).

### API-visible behavior change
Persistence failures now surface as `500` instead of a false `200`. Requests with a `transaction_group_id` now create/update the ledger row.

### Verification
Live: envelope build with a `transaction_group_id` created a `transaction_group_ledger` row. Unit tests updated in `src/features/domain-event-design/tests/domain_event_design_unit_test.go`.

### Remaining gaps
No Postgres-backed test in CI covers the upsert SQL; the cast fix is verified only by the live run.

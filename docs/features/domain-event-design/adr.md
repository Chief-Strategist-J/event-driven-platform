# ADR: Standardized Domain Event Envelope, Monotonic UUIDv7, and Wire Formatting

* **Status:** Accepted
* **Date:** 2026-11-04
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `src/features/domain-event-design/`, `src/shared/types/`, and `messaging/engine/client/`

---

## 1. Context and Problem Statement

Downstream consumers in an asynchronous event-driven system depend on strict invariants to preserve causal ordering, avoid duplicate effects, and maintain distributed traces across service boundaries. When domain events are defined with ad-hoc payloads or unformatted timestamps:
- Timezones mismatch and clock drift leads to erroneous out-of-order evaluations.
- Causation and correlation IDs are lost, rendering root-cause analysis impossible.
- Producers use CRUD verbs (`OrderUpdated`, `ItemDeleted`), turning events into anemic state notifications that leak internal persistence models instead of capturing immutable domain facts.

---

## 2. Decision Drivers

* **Monotonic Chronological Ordering**: Event identifiers must be time-ordered at the boundary without distributed lock overhead.
* **Strict Temporal Integrity**: Timestamps must be standardized in UTC RFC 3339 format to avoid timezone translation errors.
* **Trace Context Propagation**: W3C traceparent and span identifiers must be embedded directly in the message envelope.
* **Causality & Gap Detection**: Envelopes must enforce causation ID, correlation ID, tenant ID, and monotonic aggregate sequence numbers.

---

## 3. Considered Options

* **Option 1: Raw JSON Event Payloads**
  - *Cons:* No standardization; every consumer parses different metadata keys. Trace context and tenant isolation are routinely omitted.
* **Option 2: CloudEvents Specification without Validation**
  - *Cons:* Generic attribute bag lacking strict platform rules (no aggregate sequence numbers, no enforced UUIDv7 validation, loose timestamp formats).
* **Option 3: Hexagonal Event Envelope (`EventEnvelope[T any]`) with Strict Validation**
  - *Pros:* Strongly typed generic envelope; validates UUIDv7, RFC 3339 UTC, positive aggregate sequence, and `<domain>.<aggregate>.<fact>.v<N>` naming syntax at deserialization time.

---

## 4. Decision Outcome

We selected **Option 3: Hexagonal Event Envelope (`EventEnvelope[T any]`) with Strict Validation**.

### Envelope Structure (`src/shared/types/envelope.types.go`)

```json
{
  "event_id": "0194857b-7b2a-7963-8a9d-5a8b2413e001",
  "type": "commerce.order.placed.v1",
  "schema_version": "1.0.0",
  "aggregate_id": "ord_99812",
  "aggregate_seq": 1,
  "occurred_at": "2026-11-04T10:00:00.000000000Z",
  "causation_id": "cmd_81238",
  "correlation_id": "req_019283",
  "tenant_id": "tenant_enterprise_01",
  "trace_context": {
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "spanId": "00f067aa0ba902b7"
  },
  "payload": { ... }
}
```

### Invariants Enforced (`messaging/engine/client/`)
1. **Event Identifier (`event_id`)**: Must parse as an RFC 9562 UUIDv7. Non-v7 UUIDs and arbitrary strings are rejected.
2. **Type Naming Convention (`type`)**: Must match regex `^[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.v[0-9]+$`. CRUD verbs are disallowed.
3. **Timestamp (`occurred_at`)**: Strictly RFC 3339 UTC ending with `Z`.
4. **Aggregate Sequence (`aggregate_seq`)**: Strictly `>= 1` to permit out-of-order and gap detection in projection rebuilds.

---

## 5. Consequences

### Positive
* Single source of truth for all events across the fleet.
* Zero index fragmentation when persisting events in B-Tree or LSM database tables.
* Decoupled domain payload via Go generics (`EventEnvelope[T any]`).

### Negative / Tradeoffs
* Wire overhead of metadata attributes (~150-200 bytes per event).
* Strict deserialization rejects events from non-compliant external producers (requires an anti-corruption layer).


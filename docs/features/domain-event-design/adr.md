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

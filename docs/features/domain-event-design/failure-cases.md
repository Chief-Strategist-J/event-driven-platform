# Domain Event Design — Failure Cases & Resilience Playbook

This document details failure modes, severity ratings, edge-node scenarios, automated mitigations, and detection triggers for the **Domain Event Design** subsystem.

---

## 1. Message Classification & Authority Failures (§2)

### FC-DED-01: Command Disguised as Domain Event Broadcast
* **Scenario:** Upstream service publishes an action request disguised as a past-tense domain fact (e.g., `SendEmailRequested` broadcasted on an event topic). Multiple consumer group instances consume the message and trigger duplicate side-effects.
* **Severity:** CORRUPT / LOSS
* **Handling:**
  1. Envelope validation engine rejects commands published to aggregate domain topics.
  2. Event catalog strictly restricts command messages to point-to-point command topics with single consumer group bindings and explicit reply channels.
* **Detection:** Alert on domain event topics receiving event types ending in `Requested` or `Command` without an registered catalog classification.

### FC-DED-02: Usage/Billing Events Routed to Telemetry Pipeline
* **Scenario:** Revenue-critical usage and quota events are emitted via OpenTelemetry logs or metrics collectors instead of domain aggregate topics. Sampling or transient network drops cause dropped records and severe under-billing.
* **Severity:** LOSS (Direct Revenue)
* **Handling:**
  1. Business metric classification gate strictly classifies billing and usage records as domain events.
  2. Static lint checks and C-ENV block emission of usage events through telemetry sinks.
* **Detection:** Daily automated reconciliation comparing billed meter counts against transactional outbox domain records.

### FC-DED-03: Fact Published by Non-Owner Aggregate
* **Scenario:** Two distinct services or aggregates publish conflicting facts for the same domain occurrence (e.g., both `payment-service` and `order-service` publish `OrderShipped`).
* **Severity:** CORRUPT
* **Handling:**
  1. Event catalog enforces a strict single-owner rule per event type (`owning_aggregate`).
  2. Producer-side authorization and schema registration checks reject publishing if caller aggregate does not match registered owner.
* **Detection:** Schema registration rejection and outbox validation alarm on mismatched aggregate ownership.

---

## 2. Private Domain vs Public Integration Boundary (§3)

### FC-DED-04: Internal Domain Event Leaked to Public Topics
* **Scenario:** Fine-grained private domain events containing internal database column names, transient status enums, or security flags (e.g., `PENDING_FRAUD_REVIEW`) are published directly to external consumer topics.
* **Severity:** SECURITY / OUTAGE
* **Handling:**
  1. Anti-Corruption Layer (C-ACL) translates private domain events into coarse-grained, documented public integration events.
  2. Public event topics reject events whose catalog entry specifies `visibility: PRIVATE`.
* **Detection:** CI lint checks on topic routing and ACL validation alarms when private event types appear on public ingress topics.

### FC-DED-05: Defective Integration Translator Corrupts Public Facts
* **Scenario:** An asynchronous translation daemon derives public integration events from private streams with projection bugs, broadcasting inaccurate facts to third-party integrations.
* **Severity:** CORRUPT
* **Handling:**
  1. For Tier-0 critical streams, integration translation executes atomically within the same outbox transaction or is verified against golden-path contract test fixtures.
  2. Public events undergo strict integration contract schema tests before deployment.
* **Detection:** Contract validation errors on consumer side; diff monitors between private outbox payload and public integration topic.

---

## 3. Scope, Granularity & Aggregate Group Failures (§5)

### FC-DED-06: Partial Multi-Aggregate Group Reaction
* **Scenario:** A command emits events across multiple aggregates in the same database transaction. Downstream consumers consume the event for Aggregate B before Aggregate A's event arrives, triggering premature business logic.
* **Severity:** CORRUPT
* **Handling:**
  1. Envelopes include `transaction_group_id` and `expected_group_size`.
  2. Consumers requiring completeness register arrival in `transaction_group_ledger` and block processing until all parts arrive or a bounded timeout expires.
* **Detection:** Gauge on `transaction_group_incomplete_duration_ms` exceeding 5000ms.

### FC-DED-07: Micro-Event Storm from Excessively Fine Granularity
* **Scenario:** A producer emits individual events for every field changed on a web form (`EmailUpdated`, `PhoneUpdated`, `AddressLineUpdated`), creating event storms and exposing intermediate inconsistent states.
* **Severity:** DEGRADE
* **Handling:**
  1. Design review checklist enforces "one event per business decision".
  2. Field mutations within one operation are collapsed into a single semantic event (`ProfileDetailsUpdated`).
* **Detection:** Anomaly detection on events/sec per aggregate instance exceeding 50 within a single transaction.

---

## 4. Payload Semantics & Fact-Time Reference Failures (§6)

### FC-DED-08: Historical Fact Omits Point-in-Time Reference Data
* **Scenario:** An `OrderPlaced` event carries only `product_id`. Downstream revenue consumers query the current product catalog, computing revenue using today's price instead of the historical price at the moment the order occurred.
* **Severity:** CORRUPT
* **Handling:**
  1. Invariant DE-04 mandates that domain facts carry the reference data defining them at fact time (unit price, currency, discount rule ID, applied tax rate).
  2. Contract tests verify that all financial events are self-contained without callback dependencies.
* **Detection:** Consumer invariant check failure when validating historical order totals against line items.

### FC-DED-09: Unchanged Fields Omitted from STATE Events
* **Scenario:** A producer emits a `STATE` semantics class event but omits fields that were unchanged. Downstream projection stores apply the event as a complete state snapshot and overwrite omitted fields with null values.
* **Severity:** LOSS (Data Loss)
* **Handling:**
  1. `STATE` class events are validated to ensure full entity completeness.
  2. Partial updates must be explicitly typed as `DELTA` semantics class with explicit patch rules.
* **Detection:** Schema validation flags missing mandatory fields on events declared with `semantics_class = 'STATE'`.

---

## 5. Temporal & Causality Invariants (§7, §11)

### FC-DED-10: Effective Time Conflated with Commit Time
* **Scenario:** A backdated accounting adjustment or scheduled rate change is timestamped only with `recorded_at`. Materialized views and periodic financial reporting assign the adjustment to the wrong reporting period.
* **Severity:** CORRUPT
* **Handling:**
  1. Canonical envelopes mandate both `effective_at` (business fact effective time) and `recorded_at` (system commit time).
  2. Financial and billing materialized views strictly partition by `effective_at`.
* **Detection:** Invariant check flags any event where `effective_at` is null or identical to `recorded_at` on adjustment event types.

### FC-DED-11: Causal Cycle Infinite Loop
* **Scenario:** Service A emits `EventA`, which triggers Service B to emit `EventB`, which in turn triggers Service A to emit `EventA`, creating an infinite recursive cycle.
* **Severity:** OUTAGE
* **Handling:**
  1. Envelope embeds `causation_depth` and tracks causal path lineage.
  2. C-ENV and consumer middleware reject any event where `causation_depth` exceeds the maximum allowed threshold (default: 16) or where the same `(aggregate, event_type)` is repeated in the causation path.
* **Detection:** Alert triggered immediately on `CausalCycleDetectedError`.

---

## 6. Aggregate Versioning & Concurrency Conflicts (§1, §10)

### FC-DED-12: Optimistic Concurrency Conflict on Aggregate Version
* **Scenario:** Two concurrent requests attempt to mutate the same aggregate root from version $N$.
* **Severity:** CORRUPT (if unchecked) / RECOVERABLE
* **Handling:**
  1. OCC lock on `aggregate_root_version_ledger` requires `current_version == expected_version`.
  2. Second transaction fails with `409 Conflict`, rolling back the command and preventing outbox insertion.
* **Detection:** Metric counter `domain_event_occ_conflict_total`.

### FC-DED-13: Event Emitted for Terminal / Closed Aggregate
* **Scenario:** A command attempts to execute against an aggregate that has already emitted a terminal event (`AccountClosed`, `OrderCancelled`).
* **Severity:** CORRUPT
* **Handling:**
  1. Aggregate version ledger marks `is_terminal = TRUE` upon consumption of terminal events.
  2. Further version reservations are permanently rejected with `422 Unprocessable Entity`.
* **Detection:** Alert on `TerminalAggregateMutationAttempted`.


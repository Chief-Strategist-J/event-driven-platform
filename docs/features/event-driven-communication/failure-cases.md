# Event-Driven Communication — Failure Cases & Resilience Playbook

This document details failure modes, impact ratings, automated mitigations, and detection triggers for the **Event-Driven Communication** subsystem.

---

## 1. Inter-Service Communication Failures (§1)

### FC-EDC-01: Temporal Coupling & Cascading Exhaustion
* **Scenario:** Service A uses synchronous HTTP/REST calls to notify Service B of an event. Service B experiences high latency or downtime. Service A's HTTP client threads block until thread pools and database connections are exhausted, cascading the outage upstream.
* **Severity:** OUTAGE
* **Handling:**
  1. Invariant EDC-02 bans synchronous RPC for notifications or state-mutation side-effects.
  2. Asynchronous notifications must be routed exclusively through transactional outbox tables and Kafka topics.
  3. Where synchronous RPC is strictly required for reads, circuit breakers (trip after 5 timeouts) and 500ms timeouts are mandatory.
* **Detection:** Prometheus alert on `http_client_in_flight_requests` exceeding 80% pool capacity and circuit breaker state changes to `OPEN`.

### FC-EDC-02: Dual-Write Data Drift
* **Scenario:** A service attempts to update its database and publish directly to a Kafka broker in two uncoordinated steps. The database transaction commits, but the network drops before the Kafka message is acknowledged. Downstream services never receive the event.
* **Severity:** CORRUPT / LOSS
* **Handling:**
  1. Invariant EDC-01 strictly prohibits business services from creating direct Kafka producer clients.
  2. Every domain event is persisted atomically to the `transactional_outbox` table within the same ACID database transaction as the aggregate mutation.
  3. Independent outbox relay daemons (or CDC Debezium connectors) publish events to Kafka with guaranteed at-least-once delivery.
* **Detection:** Automated outbox consistency checker flags rows with `status = 'PENDING'` older than 60 seconds.

---

## 2. Distributed Saga & Compensation Failures (§1)

### FC-EDC-03: Orphaned Saga from Missing Compensating Transaction
* **Scenario:** In a multi-step choreographed or orchestrated saga (e.g., Order $\to$ Payment $\to$ Inventory), Step 3 (Inventory Allocation) fails permanently, but Step 2 lacks a compensating event (Payment Refund). Customer is charged without receiving goods.
* **Severity:** LOSS (Financial)
* **Handling:**
  1. Saga coordinator mandates bidirectional contract symmetry: every forward action must register a compensating rollback action.
  2. If a forward step fails, the saga state machine deterministically emits compensating events in reverse order.
  3. Unrecoverable compensations transition the saga to `MANUAL_INTERVENTION_REQUIRED` and trigger high-priority paging alerts.
* **Detection:** Alert triggered when saga instance status remains `COMPENSATION_FAILED` or `STALLED` for more than 5 minutes.

### FC-EDC-04: Causal Trace Break at Saga Step Boundaries
* **Scenario:** A saga consumer processing Step 1 generates a new random `correlation_id` before emitting Step 2, decoupling the trace context and blinding observability waterfalls in Jaeger/Tempo.
* **Severity:** DEGRADE (Observability)
* **Handling:**
  1. Middleware enforces end-to-end correlation propagation: `correlation_id` is preserved through every hop of the saga.
  2. Immediate triggering event ID becomes the `causation_id` for the subsequent action.
* **Detection:** Lint checks and gateway header injectors verify `correlation_id` presence on all outgoing events.

---

## 3. Producer Topology & Delivery Failures (§2)

### FC-EDC-05: Silent Message Loss via Asynchronous Send Without Acks
* **Scenario:** Producer sends messages with `acks=0` or `acks=1`. A broker leader fails before replicating to in-sync replicas (ISR), causing unrecoverable data loss during leader re-election.
* **Severity:** LOSS
* **Handling:**
  1. Outbox relay producer configuration strictly enforces `acks=all` (or `-1`).
  2. Broker topic configuration mandates `min.insync.replicas=2` with replication factor of 3.
  3. Producer enables idempotence (`enable.idempotence=true`) to eliminate duplicate retries on transient network timeouts.
* **Detection:** Kafka broker metric `UnderReplicatedPartitions > 0` or producer alert on `RetriesExhaustedException`.

---

## 4. Consumer Pipeline & Error Resolution Failures (§3)

### FC-EDC-06: Phantom Processing via Premature Auto-Commit
* **Scenario:** A consumer enables Kafka `enable.auto.commit=true`. The consumer polls a batch of 100 records and automatically commits offsets before database processing completes. A node crash or `OutOfMemoryError` drops the uncommitted records.
* **Severity:** LOSS
* **Handling:**
  1. Invariant EDC-04 bans auto-commit across all consumer groups.
  2. Offsets are manually committed synchronously or asynchronously *only* after local database transactions succeed and side-effects are completed.
* **Detection:** Consumer logs audit checking manual commit invocations against batch completion events.

### FC-EDC-07: Poison Pill Head-of-Line Partition Block
* **Scenario:** A malformed payload or unhandled business validation panic occurs on record offset $N$. The consumer retries indefinitely, halting processing for all subsequent healthy records in the partition.
* **Severity:** OUTAGE
* **Handling:**
  1. Error classifier distinguishes transient failures (retryable DB timeouts) from permanent validation failures (poison pills).
  2. Transient failures retry with bounded exponential backoff and jitter up to a max attempt count (e.g., 3).
  3. Permanent poison pills are emitted directly to the Dead-Letter Queue (DLQ) with exception stacktraces, original headers, and failure metadata, allowing partition consumption to resume.
* **Detection:** Prometheus alert on `dlq_messages_published_total` rate spike and consumer lag growth.


## 5. Amendment 2026-11-07: Failure Modes Added

### FC-EDC-A1: Command dispatch silently dropped (fixed)
* **Scenario:** `commands/dispatch` returned success without persisting anything.
* **Severity:** LOSS
* **Detection:** Status was `DISPATCHED_TO_OUTBOX` with no outbox row. Now an outbox row (PENDING) must exist for every `QUEUED_IN_OUTBOX` response.
* **Handling:** Persisted via `SaveOutboundCommand`; failure to persist returns an error instead of success.

### FC-EDC-A2: Invalid dispatch request
* **Trigger:** missing `target_topic`, `reply_to_topic`, `tenant_id`, `source_service` or `command_name`; non-JSON `command_payload`.
* **Response:** `400` (`ErrInvalidCommand`).
* **Operator action:** Fix the request; `command_payload` is a JSON string.

### FC-EDC-A3: Duplicate command external_id
* **Trigger:** same `external_id` re-sent for the same tenant/source service.
* **Response:** `409 CONFLICT` (`ErrDuplicateCommand`).
* **Operator action:** Treat as already queued.

### FC-EDC-A4: Queued command never delivered (topic missing)
* **Trigger:** `target_topic` does not exist on the broker (auto-create disabled).
* **Detection:** outbox row PARKED after 5 retries with "Unknown Topic Or Partition".
* **Operator action:** create the topic, then reset the row (see transactional-outbox failure cases F-OBX-A3).

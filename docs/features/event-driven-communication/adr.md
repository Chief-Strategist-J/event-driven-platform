# ADR: Multi-Protocol Ingress, Asynchronous Publish/Subscribe Boundaries, and Saga Coordination

* **Status:** Proposed
* **Date:** 2026-11-06
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `src/features/event-driven-communication/`, `src/infra/`, and `src/api/rest/v1/`

---

## 1. Context and Problem Statement

Microservices frequently face the dilemma of choosing between synchronous RPC (ConnectRPC/REST) and asynchronous event streams (Apache Kafka). When teams apply these patterns haphazardly:
1. **Temporal Coupling & Cascading Outages**: Synchronous HTTP/REST calls used for cross-domain notifications create brittle distributed call chains. If downstream Service C suffers a latency spike or outage, upstream Service A hangs and exhausts its connection pool.
2. **Dual-Write Inconsistency**: Applications attempt to write to local databases and publish to Kafka in two separate uncoordinated operations, resulting in data drift when network drops occur between the database commit and the broker produce call.
3. **Uncompensated Distributed Sagas**: Long-running multi-step transactions (e.g., Order $\to$ Payment $\to$ Inventory) fail halfway through without deterministic compensation workflows, leaving customer accounts debited while inventory remains unallocated.
4. **Protocol Mismatch at the Edge**: Web and mobile clients communicating via HTTP/gRPC/WebSockets lack direct access to Kafka wire protocols, requiring a managed ingress gateway that bridges synchronous client requests into durable transactional outbox records.

---

## 2. Decision Drivers

* **Zero Temporal Coupling for Side-Effects**: All cross-boundary state changes and side-effects must be communicated asynchronously via Kafka events. Synchronous RPC is strictly restricted to read-only queries with bounded timeouts.
* **Guaranteed Delivery without Dual Writes**: Producers must publish through a Transactional Outbox pattern (`C-OBX`) using atomic ACID transactions. Direct client produce calls from service command handlers to Kafka brokers are banned.
* **Deterministic Saga Orchestration & Compensation**: Multi-step distributed transactions must enforce forward progress and backward compensation via explicit saga state machines.
* **Protocol Ingress Gateway**: Synchronous HTTP/gRPC ingress commands must translate into durable outbox events at the ingress boundary, returning immediate acknowledgment (`202 Accepted` with correlation ID) to callers.
* **Distributed W3C Trace Continuity**: Trace contexts (`traceparent`, `tracestate`) must propagate across protocol boundaries (HTTP header $\to$ Kafka record header $\to$ downstream consumer).

---

## 3. Considered Options

* **Option 1: Direct Synchronous REST/gRPC for All Inter-Service Calls**
  - *Cons:* Severe temporal coupling, cascading blast radiuses, circuit-breaker trips during partial network partitions, and zero durability when receivers are restarting.
* **Option 2: Direct Kafka Broker Client Inside Business Services**
  - *Cons:* Introduces the classic dual-write failure mode; if Kafka is temporarily unavailable, local business transactions either hang or commit without publishing events.
* **Option 3: Hybrid Architecture: ConnectRPC for Synchronous Queries + Transactional Outbox Relay for Asynchronous Facts + Managed Saga Orchestration**
  - *Pros:* Decouples availability; synchronous callers obtain real-time reads with circuit breakers, while state mutations are guaranteed at-least-once via PostgreSQL outbox tables and Kafka brokers.

---

## 4. Decision Outcome

We selected **Option 3: Hybrid Architecture with ConnectRPC Reads, Outbox-Backed Pub/Sub Facts, and Saga Orchestration**.

### Communication Pattern Selection Matrix

| Pattern | Interaction Type | Transport Protocol | Consistency Model | Failure Strategy |
| :--- | :--- | :--- | :--- | :--- |
| **Real-Time State Query** | Synchronous Read | ConnectRPC over HTTP/2 | Strong / Read-Your-Writes | Circuit breaker trips after 5 timeouts; return fallback or `504` |
| **Domain State Mutation** | Asynchronous Fact | Outbox $\to$ Kafka Pub/Sub | Eventual Consistency | At-least-once outbox delivery; consumer idempotency barrier |
| **Distributed Multi-Step Flow**| Orchestrated Saga | Kafka Topics + Saga Log | Eventual Compensating | Forward retry or reverse compensating events (`OrderCancelled`) |

### Invariants Enforced
1. **EDC-01 (Single Publication Gateway)**: No service business layer communicates directly with Kafka. Publication occurs exclusively through local transactional outbox tables.
2. **EDC-02 (RPC Notification Ban)**: Synchronous REST/gRPC is banned for triggering downstream reactions or notifications. If a caller does not block for the response payload, the call must be an event.
3. **EDC-03 (Producer Delivery Guarantees)**: Outbox relays must produce with `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection <= 5`, and `compression.type=zstd`.
4. **EDC-04 (Manual Offset Commitment)**: Consumers must commit Kafka offsets manually *only* after their local database transaction or side-effect completes successfully. Auto-commit is strictly forbidden.
5. **EDC-05 (Saga Compensation Symmetry)**: For every forward action event (e.g., `ReserveInventory`), an aggregate must define and handle a compensating action event (e.g., `ReleaseInventory`) referencing the original `saga_id`.

---

## 5. Consequences

### Positive
* Total decoupling of service availability: upstream services continue processing traffic even during downstream outages.
* Elimination of distributed dual-write inconsistency bugs.
* Complete traceability of multi-step business transactions through correlation and causation IDs.

### Negative / Tradeoffs
* Increased architectural complexity (requires outbox pollers, deduplication tables, and saga coordinator ledgers).
* Eventual consistency requires UI/UX paradigms that accommodate asynchronous completion (e.g., polling, WebSockets, optimistic UI).

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Synchronous HTTP/RPC communication between microservices creates tightly coupled dependency chains (distributed monoliths). If service A calls B, which calls C, which calls D, the system's availability is the mathematical product of all four services ($99.9\%^4 = 99.6\%$), and latency cascades linearly. An outage in service D cascades upstream, halting the entire platform.

### 2. Internal Algorithmic Mechanics & State Invariants

Services communicate exclusively via asynchronous event streams managed by partitioned commit logs. Producers publish events without awaiting downstream consumer execution. Subscribers independently pull events at their own processing pace. Temporal decoupling ensures that even if subscriber services are offline for hours during deployments or crashes, zero messages are lost and producers operate at full performance.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Asynchronous Temporal Independence
* **Design Philosophy & Epistemology**:
  Synchronous HTTP/gRPC architectures require every microservice in a call chain to be alive, healthy, and fast simultaneously. A single slow dependency causes thread starvation and cascades into a global outage. The philosophy is Temporal Independence: producers emit events and immediately resume work; consumers process events at their own pace. Services exist in complete temporal isolation.
* **Threat Vector Defeated**: Cascading outages and global platform collapse caused by downstream service latency spikes propagating back up synchronous call chains.
* **Storage & State Invariant**:
```text
ProducerAvailability != ConsumerAvailability
ProducerLatency != ConsumerProcessingTime
```
* **Implementation Mechanism**:
  Services communicate exclusively through partitioned, persistent message brokers (Kafka). Producers write to the local outbox or broker cluster with sub-10ms latency; downstream outages do not impair the producer's ability to accept client traffic.
* **Non-Negotiable Trade-Off**: Loss of immediate synchronous response semantics. Clients must adopt polling, webhooks, or WebSocket subscriptions to observe eventual outcomes.

---

#### Principle 2: Dumb Pipes, Smart Endpoints
* **Design Philosophy & Epistemology**:
  Embedding business routing logic, transformations, and orchestrations inside the messaging broker (e.g., enterprise service buses) creates an unmaintainable, centralized bottleneck that is impossible to test. The philosophy is Smart Endpoints, Dumb Pipes: the broker is simply a fast, highly available append-only log. All business logic, filtering, and transformations live exclusively in the endpoints.
* **Threat Vector Defeated**: Enterprise Service Bus (ESB) gridlock where centralized routing rules and transformations create a fragile, unversioned monolithic broker.
* **Storage & State Invariant**:
```text
BrokerLogic == AppendOnlyLog + Partitioning + Replicating
BusinessTransformations executed strictly within application domain boundaries
```
* **Implementation Mechanism**:
  Kafka brokers perform zero payload inspection or business transformation. Filtering and schema translations are implemented inside consumer applications using clean domain services.
* **Non-Negotiable Trade-Off**: Duplication of filtering logic across multiple consumer codebases if multiple teams consume the same raw topic.

---

#### Principle 3: Bounded Ephemeral Log Retention
* **Design Philosophy & Epistemology**:
  Treating a message broker as an infinite data lake results in massive disk costs, slow broker rebalance recovery times, and operational fragility. The philosophy is Ephemeral In-Flight Buffering: message brokers are high-speed transit channels; durable, infinite history belongs in data lakes, object storage, and event-sourced databases.
* **Threat Vector Defeated**: Broker disk exhaustion, runaway cloud storage bills, and multi-hour broker restart times caused by infinite message retention.
* **Storage & State Invariant**:
```text
BrokerRetentionTime == MaxExpectedConsumerOutage + SafetyBuffer (e.g., 7 days)
LongTermHistory archived into S3 / Iceberg / Parquet
```
* **Implementation Mechanism**:
  Kafka topics enforce strict segment retention policies (`retention.ms = 604800000`, 7 days). A background sink connector (Kafka Connect S3) continuously streams committed events into immutable Parquet files on S3 for permanent storage and analytics.
* **Non-Negotiable Trade-Off**: Historical replays past the retention window require reading from cold object storage rather than directly from Kafka brokers.

---

#### Principle 4: Standardized CloudEvents Envelope Specification
* **Design Philosophy & Epistemology**:
  Ad-hoc message formats across different teams create integration chaos, breaking generic middleware, observability tools, and routing pipelines. The philosophy is Universal Envelope Standardization: every event across the enterprise must conform to the CNCF CloudEvents standard envelope.
* **Threat Vector Defeated**: Integration friction, broken tracing middleware, and unparseable payloads caused by teams inventing inconsistent JSON wrapping structures.
* **Storage & State Invariant**:
```text
Message conforms to CNCF CloudEvents 1.0 Specification
Envelope contains: id, source, specversion, type, datacontenttype, time, data
```
* **Implementation Mechanism**:
  Platform SDKs wrap all outgoing domain events into standardized CloudEvents JSON or Protobuf structures. Generic platform tools (routers, DLQs, metrics monitors) inspect envelope metadata without coupling to the internal domain payload.
* **Non-Negotiable Trade-Off**: Slight message size overhead from standardized envelope metadata headers.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is an event-driven architecture fundamentally more resilient than a synchronous microservices mesh?**
   * *Answer*: Synchronous microservices mesh architectures have multiplicative availability: if 5 services each have 99% availability, total system availability is 0.99^5 = 95.1%. Event-driven architectures have additive resilience: if downstream consumers are down, the producer continues operating at 99.99% availability while events buffer safely on the broker.
2. **How does an application handle workflows requiring immediate synchronous confirmation (e.g., user password change)?**
   * *Answer*: Immediate personal security state changes remain synchronous within their local aggregate boundary. Event-driven communication is used for cross-boundary side effects (e.g., sending security alert emails, invalidating cross-region sessions).
3. **What is the primary operational metric that indicates health in an event-driven communication backbone?**
   * *Answer*: Consumer Group Lag (number of unconsumed messages per partition) and End-to-End Latency ($T_{\text{consumed}} - T_{\text{produced}}$). A rising lag metric indicates downstream saturation or processing failure.

---
## Authoritative References & Production Citations

* **[Gregor Hohpe & Bobby Woolf: Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/)**
  - *Architectural Rationale & Implementation Grounding*: The canonical work establishing asynchronous messaging patterns, channel architectures, and message translation models.
* **[Martin Fowler: What do you mean by Event-Driven?](https://martinfowler.com/articles/201701-event-driven.html)**
  - *Architectural Rationale & Implementation Grounding*: Essential taxonomy clarifying Event Notification, Event-Carried State Transfer, Event Sourcing, and CQRS.
* **[Sam Newman: Building Microservices: Designing Fine-Grained Systems](https://samnewman.io/books/building_microservices_2nd_edition/)**
  - *Architectural Rationale & Implementation Grounding*: Authoritative patterns on asynchronous event-driven collaboration, service boundaries, and choreography vs orchestration.
* **[Jay Kreps: Questioning the Lambda Architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/)**
  - *Architectural Rationale & Implementation Grounding*: Seminal essay advocating for unified event-log stream architectures over fragile hybrid batch/stream systems.

## Amendment 2026-11-07: Command dispatch now persists to the transactional outbox

### Problem observed
`POST /api/v1/communication/commands/dispatch` returned `202` with status `DISPATCHED_TO_OUTBOX`, but no outbox row existed and nothing was ever published. (Note: `/api/v1/commands/dispatch` is a different route belonging to CQRS.)

### Root cause
RC4: `EventCommunicationService.DispatchAsyncCommand` was a scaffolded stub that generated a `cmd_...` id and returned a success status without any repository call. The status string described a side effect that never happened.

### Decision / fix
- `DispatchAsyncCommand` now builds a JSON envelope (`command_id`, `command_name`, `reply_to_topic`, `timeout_seconds`, `payload`) and persists one outbox row through the new repository method `SaveOutboundCommand` (query `InsertOutboundCommandOutbox`). Row shape: `aggregate_type` = `target_topic` (the relay uses `aggregate_type` as the Kafka topic), `aggregate_id` = command id, `event_type` = `command_name`, `schema_version` = v1, state PENDING, headers `{reply_to_topic: ...}`.
- Status renamed to `QUEUED_IN_OUTBOX`: the service only guarantees durable queuing; delivery is asynchronous via the relay. We chose to reuse the outbox rather than publish directly to Kafka so the command shares the dual-write protection and retry/parking behavior of every other event.
- Validation (`ErrInvalidCommand`): non-empty `target_topic`, `reply_to_topic`, `tenant_id`, `source_service`, `command_name`; `command_payload` must be valid JSON (empty means `{}`). `external_id` defaults to the command id when absent; a non-UUID `correlation_id` is replaced by a generated UUID. A unique violation maps to `ports.ErrDuplicateCommand` (409).

### API-visible behavior change
- Response `status`: `DISPATCHED_TO_OUTBOX` -> `QUEUED_IN_OUTBOX` (**breaking** for clients matching the old string).
- New `400` for missing/invalid fields; new `409` for duplicate `external_id`.
- `command_payload` must be a JSON string containing valid JSON.

### Verification
- Live: dispatch returned QUEUED_IN_OUTBOX, an outbox row was created and the relay moved it to PUBLISHED.
- Unit tests updated in `src/features/event-driven-communication/tests/event_driven_communication_unit_test.go`.

### Remaining gaps
- Because `target_topic` becomes the Kafka topic, it must exist beforehand or the row is PARKED (see transactional-outbox RC6).
- The relay publishes only the stored headers for these rows (`reply_to_topic`); `event_type`/`schema_version`/`source_service` headers are set by `OutboxService.StageEvent`, which this path bypasses (verified from the code; the live header output for this path was not checked).
- No reply correlation/timeout enforcement is implemented in this path (not verified beyond the envelope fields).

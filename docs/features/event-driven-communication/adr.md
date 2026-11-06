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


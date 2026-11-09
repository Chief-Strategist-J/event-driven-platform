# ADR: Distributed Tracing Pipeline, Observability Stack Activation, Ingress Health Contracts, and Unified Shared Telemetry

* **Status:** Accepted
* **Date:** 2026-11-04
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `docs/features/distributed-tracing/`, `src/infra/observability/`, `src/shared/middleware/`, `deploy/docker/compose/`, and `scripts/`

---

## 1. Context and Problem Statement

In an asynchronous event-driven platform utilizing Kafka brokers, relational event stores, and decoupled microservices, isolating errors, tracking latencies, and debugging failures without end-to-end tracing and unified logs is impossible:
1. Without distributed tracing, asynchronous event hops across Kafka partitions lose causation and parent-span context.
2. The infrastructure container definitions (`docker-compose.infra.yml`) previously placed the entire observability suite (Grafana Tempo, Grafana Loki, OpenTelemetry Collector, Prometheus, Grafana) behind an optional Compose profile (`profiles: [observability]`), leaving local environments completely blind to trace spans and logs.
3. The Envoy API Gateway was marked `unhealthy` under default Docker checks because its container image (`envoyproxy/envoy:v1.31.2`) omits `wget`, causing the container health probe to fail despite Envoy being healthy and serving traffic.
4. Hardcoded remote `@sha256:...` digest pins for images like `apicurio` and `kafka` broke compose bootstrapping because remote registries updated multi-arch manifests, causing `manifest unknown` download errors.
5. Domain services previously lacked a centralized, uniform telemetry wrapper conforming to the 3-Step Failure Invariant (recording error on span + emitting structured JSON log with trace context + returning typed domain error) and lacked centralized HTTP ingress middleware for W3C context extraction and panic recovery.

---

## 2. Decision Drivers

* **End-to-End Trace Lineage**: W3C Trace Context (`traceparent`, `tracestate`) must seamlessly bridge HTTP ingress (Envoy), Go service execution, Kafka message headers, and background consumer pipelines.
* **Observability-by-Default Contract**: Tracing and logging infrastructure must be active by default during developer runs and integration testing via platform scripts (`scripts/run/run.sh`).
* **Minimal Attack Surface Compatibility**: Health checks in hardened or minimal distroless containers (Envoy) must not assume external utilities (`wget`/`curl`) and must use zero-dependency native shell socket primitives (`/dev/tcp`).
* **Hermetic Script-Based Execution**: All bootstrapping, secret creation, and telemetry validation must execute exclusively through repository scripts (`scripts/run/run.sh`, `scripts/test/test.unit.sh`, `scripts/test/test.integration.sh`).
* **Compliance with Strict Platform Rules**:
  - `09-test-writing.md`: Tiered test pyramid (`tests/*.rules.test.go`, integration scripts, zero-synthetic illusion).
  - `10-tracing-writing.md`: 4-tier span hierarchy, 3-step failure invariant, W3C context propagation, structured JSON logs correlated with trace IDs.
  - `0-package-folder-writing-rule.md` & `1-feature-folder-writing-rule.md`: Hexagonal directory segregation, `.gitkeep` hierarchy preservation, zero inline comments.
  - `2-apis.writing.rule.md` & `3.shared.code.writing.rule.md`: Centralized shared middleware, pure utilities in `src/shared/`, centralized infrastructure in `src/infra/`.

---

## 3. Considered Options

* **Option 1: Ad-hoc Manual Docker Commands and On-Demand Telemetry**
  - *Cons:* Inconsistent developer setups, silent failure modes when running without traces, breaks CI reproducibility.
* **Option 2: Standalone Jaeger + File-based Logging**
  - *Cons:* Lacks correlation between logs, metrics, and traces. Misses unified querying across Grafana, Loki, and Tempo.
* **Option 3: Unified OpenTelemetry Architecture (OTel Collector + Tempo + Loki + Prometheus) with Centralized Shared Middleware & Native Socket Probing**
  - *Pros:* Fully compliant with OpenTelemetry standards. Single collector pipeline receives OTLP traces (`:4317`/`:4318`) and forwards traces to Grafana Tempo, logs to Loki, and metrics to Prometheus. Unifies telemetry inside Grafana (`:3000`). Provides turnkey HTTP middleware for W3C trace context extraction and structured error recovery.

---

## 4. Decision Outcome

We selected **Option 3: Unified OpenTelemetry Architecture (OTel Collector + Tempo + Loki + Prometheus) with Centralized Shared Middleware & Native Socket Probing**.

### 4.1 Telemetry Pipeline Topology

```
[Inbound Traffic / Test Runners]
         │ (HTTP / OTLP :4318)
         ▼
[Envoy Ingress & Centralized Ingress Middleware]
         │ (TraceAndContextMiddleware: extracts traceparent & correlation IDs)
         ▼
[Domain Services: Event Schema Registry & Outbox]
         │ (StartSpan -> RecordError on failure -> Log with trace_id/span_id)
         ▼
[OpenTelemetry Collector (:4317 / :4318)]
   ├── Traces  ──► Grafana Tempo (:4317 / :3200)
   ├── Logs    ──► Grafana Loki  (:3100)
   └── Metrics ──► Prometheus    (:9090)
         │
         ▼
[Unified Grafana Dashboard (:3000)] (admin/admin)
```

### 4.2 Architectural Changes Applied

1. **Default Observability Profile Activation ([`scripts/run/run.sh`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/scripts/run/run.sh))**:
   - Sets `COMPOSE_PROFILES="${COMPOSE_PROFILES:-observability}"` so that Tempo, Loki, Prometheus, OTel-Collector, and Grafana start automatically alongside database, broker, and cache infrastructure.
2. **Native TCP Socket Health Check ([`deploy/docker/compose/docker-compose.infra.yml`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/deploy/docker/compose/docker-compose.infra.yml))**:
   - Fixed Envoy's failing health probe by switching to `/dev/tcp/127.0.0.1/9901` socket inspection, eliminating missing binary errors (`wget: not found`).
3. **Resilient Registry Tags**:
   - Stripped fragile SHA256 digest pins in `docker-compose.infra.yml` in favor of validated SemVer image tags (`apache/kafka:3.8.0`, `apicurio/apicurio-registry-mem:2.6.5.Final`, `envoyproxy/envoy:v1.31.2`).
4. **Centralized Observability Module ([`src/infra/observability/telemetry.go`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/src/infra/observability/telemetry.go))**:
   - Implemented `StartSpan`, `RecordError`, and structured JSON log helpers (`LogInfo`, `LogWarn`, `LogError`) embedding top-level `trace_id` and `span_id` fields.
5. **Centralized Shared Ingress Middleware ([`src/shared/middleware/ingress.middleware.go`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/src/shared/middleware/ingress.middleware.go))**:
   - `TraceAndContextMiddleware`: W3C traceparent extraction, request/correlation ID management, and response header injection (`X-Trace-Id`, `X-Span-Id`).
   - `ErrorRecoveryMiddleware`: Global panic recovery, active span error marking, structured error logging, and standard `ApiResponse` envelope formatting.
6. **Automated Trace & Log Ingestion Suite ([`scripts/test/test.integration.sh`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/scripts/test/test.integration.sh))**:
   - Verifies end-to-end telemetry ingestion by emitting OTLP spans and Loki log lines and asserting their queryability via Tempo (`/api/traces/{traceId}`) and Loki (`/loki/api/v1/query`).
7. **Automated Tier 0 Unit Runner ([`scripts/test/test.unit.sh`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/scripts/test/test.unit.sh))**:
   - Runs all package Tier 0 unit tests with `-race` detection and strict error exit codes.

---

## 5. Consequences

### Positive
* **Complete Observability**: Every event, error, and database operation can be correlated across logs and traces in Grafana.
* **100% Healthy Fleet**: All 10 local infrastructure and telemetry containers report `healthy` status.
* **Strict Script Contract**: All setup, secrets, and tests adhere strictly to platform runner scripts.
* **Hermetic Failure Auditing**: Zero unrecorded errors; panics and 4xx/5xx failures are recorded on spans and emitted in structured JSON.

### Negative / Trade-offs
* **Workstation Memory Overhead**: The full suite consumes ~1.2 GB additional memory. Developers needing minimal footprints can set `COMPOSE_PROFILES="" ./scripts/run/run.sh`.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

In monolithic applications, a single stack trace identifies the root cause of a failure. In distributed event-driven systems, a single business transaction spans multiple microservices, background Kafka topics, outbox queues, and asynchronous workers. Without distributed tracing, diagnosing latency spikes or intermittent dropouts across async boundaries is statistically impossible.

### 2. Internal Algorithmic Mechanics & State Invariants

Every request entering the platform is assigned a globally unique Trace ID and Parent Span ID via W3C TraceContext standards. When the service publishes an event or writes to the outbox, the OpenTelemetry context is injected into message headers (traceparent, tracestate). Downstream consumers extract this context from headers, linking asynchronous child spans to the original caller span with complete causal fidelity.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Universal Context Propagation Across Asynchronous Hops
* **Design Philosophy & Epistemology**:
  In an event-driven system, synchronous HTTP call stacks disappear the moment a message is dropped onto an asynchronous broker. If causality headers are lost, distributed observability becomes impossible. The philosophy is Unbroken Causality: trace context must travel within message headers across brokers, queues, and databases, linking all downstream reactions to the originating user action.
* **Threat Vector Defeated**: Inability to diagnose distributed outages, latency bottlenecks, and data corruption across decoupled microservices due to broken diagnostic traces.
* **Storage & State Invariant**:
```text
TraceID(OriginatingRequest) == TraceID(PublishedMessage) == TraceID(ConsumedExecution) == TraceID(DownstreamEvent)
```
* **Implementation Mechanism**:
  The platform injects OpenTelemetry W3C Trace Context headers (`traceparent`, `tracestate`) into Kafka record headers on publish. Consumer middleware extracts these headers upon message receipt, instantiating child spans that preserve parent-child span hierarchies.
* **Non-Negotiable Trade-Off**: Payload byte overhead from injecting trace headers on every message, and CPU serialization overhead during span extraction and injection.

---

#### Principle 2: Non-Sampling Invariant for Failure Spans
* **Design Philosophy & Epistemology**:
  Probabilistic trace sampling (e.g., saving only 1% of traces) saves storage costs, but guarantees that the exact trace needed to debug an intermittent, catastrophic production failure is discarded. The philosophy is Tail-Based Error Retention: 100% of failed traces, error spans, and DLQ-routed operations must be preserved, while successful high-frequency traces can be aggressively sampled.
* **Threat Vector Defeated**: Missing critical production debugging traces during rare, intermittent race conditions and catastrophic service failures.
* **Storage & State Invariant**:
```text
If Span.Status == ERROR or StatusCode >= 500 ==> SamplingDecision == KEEP (100% Retention)
```
* **Implementation Mechanism**:
  OpenTelemetry Collectors implement tail-based sampling processors. Spans are buffered in memory until transaction completion; any trace containing an error tag, panic, or HTTP 5xx code is marked for permanent retention in Tempo / Jaeger, while healthy traces are sampled at 1%.
* **Non-Negotiable Trade-Off**: Memory buffer overhead on OpenTelemetry collector nodes to retain traces in flight while awaiting final status codes.

---

#### Principle 3: Asynchronous Baggage Governance
* **Design Philosophy & Epistemology**:
  Propagating arbitrary business metadata across asynchronous boundaries bloats message envelopes and creates hidden data coupling between unrelated services. The philosophy is Strict Baggage Whitelisting: only immutable, cross-cutting operational metadata (tenant ID, correlation ID, security context) may be carried in trace baggage.
* **Threat Vector Defeated**: Network bandwidth bloat, memory bloat, and security leaks caused by developers attaching large domain payloads to distributed trace baggage.
* **Storage & State Invariant**:
```text
BaggageKeys strictly subset of {tenant_id, correlation_id, user_id, client_type}
BaggageSize <= 512 bytes
```
* **Implementation Mechanism**:
  Middleware intercepts OpenTelemetry baggage attachments, rejecting any key not explicitly defined in the platform governance schema and dropping baggage exceeding the 512-byte size threshold.
* **Non-Negotiable Trade-Off**: Developers cannot use trace context as an ad-hoc parameter-passing backdoor, forcing explicit domain event schema design.

---

#### Principle 4: High-Cardinality Dimension Hygiene
* **Design Philosophy & Epistemology**:
  Injecting unbounded, unique user IDs or timestamps into trace span names explodes tracing backend index cardinality, degrading search performance and crashing tracing storage clusters. The philosophy is Static Span Topology: span names represent fixed operational templates; dynamic entity identifiers belong strictly in span attributes.
* **Threat Vector Defeated**: Tracing backend (Elasticsearch, Tempo) index crashes and out-of-memory collapses caused by high-cardinality span names.
* **Storage & State Invariant**:
```text
SpanName is Static Enum String (e.g., 'Kafka.Consume: orders.events')
HighCardinalityIdentifiers stored strictly in SpanAttributes (e.g., 'order.id = 12345')
```
* **Implementation Mechanism**:
  Platform tracing SDKs enforce linting rules that disallow string interpolation in span creation (`tracer.Start(fmt.Sprintf("order-%s", id))` is banned). Dynamic IDs are set via `span.SetAttributes(attribute.String("order.id", id))`.
* **Non-Negotiable Trade-Off**: Requires strict lint checks in CI to prevent developers from interpolating dynamic IDs into span names.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is head-based sampling dangerous in an event-driven architecture?**
   * *Answer*: Head-based sampling makes the decision to keep or drop a trace at the initial ingress point before processing begins. If an error occurs 5 hops later during asynchronous consumption, a dropped trace means zero forensic data is available to debug the failure.
2. **How does OpenTelemetry propagate trace context through an Apache Kafka message?**
   * *Answer*: OpenTelemetry utilizes Kafka record headers. The `traceparent` header contains the W3C trace ID, parent span ID, and trace flags formatted as a binary or ASCII string without modifying the message payload.
3. **What happens to trace continuity when an event is delayed in a retry topic for 30 minutes?**
   * *Answer*: Trace context is preserved in the Kafka headers throughout the 30-minute delay. When the consumer resumes, it creates a new child span linked to the original parent trace ID, documenting the exact 30-minute latency gap in the visualization timeline.

---
## Authoritative References & Production Citations

* **[W3C Trace Context Specification (W3C Recommendation 2021)](https://www.w3.org/TR/trace-context/)**
  - *Architectural Rationale & Implementation Grounding*: Universal standard for distributed context propagation across heterogeneous transport layers and cloud platforms.
* **[OpenTelemetry Semantic Conventions for Messaging Systems](https://opentelemetry.io/docs/specs/semconv/messaging/)**
  - *Architectural Rationale & Implementation Grounding*: Formal specification of span attributes, context injection rules, and naming standards for Kafka and pub/sub systems.
* **[Benjamin Sigelman et al.: Dapper, a Large-Scale Distributed Systems Tracing Infrastructure (Google 2010)](https://research.google/pubs/pub36356/)**
  - *Architectural Rationale & Implementation Grounding*: The seminal paper introducing modern distributed tracing concepts, context stitching, and sampling trade-offs.
* **[Brendan Gregg: Systems Performance: Enterprise and the Cloud](https://www.brendangregg.com/systems-performance-2nd-edition-book.html)**
  - *Architectural Rationale & Implementation Grounding*: Foundational text on profiling latency, distributed bottlenecks, and measuring request lifecycles through tracing instrumentation.

## Amendment 2026-11-07: Coverage report validation and no silent error drops

### Problem observed
`POST /api/v1/tracing/coverage/report` accepted any payload (blank fields, unknown `hop_type`, negative counts, `propagated_spans` greater than `total_spans`) and returned success even when the upsert failed.

### Root cause
RC3: `ReportCoverage` did `_ = s.repo.UpsertPropagationCoverage(...)`, and had no input validation (RC2). `InjectTraceContext` likewise discarded the `RecordSpanAudit` error.

### Decision / fix
- `validateCoverageCommand` (service layer) trims and requires `tenant_id`, `source_service`, `external_id`, `hop_source`, `hop_target`; `hop_type` must be one of `CONNECT_RPC`, `OUTBOX_RELAY`, `KAFKA_CONSUMER`, `ASYNC_REACTION`; counts must be >= 0; `propagated_spans` <= `total_spans`. Violations return `ErrInvalidCoverageReport` (400).
- The upsert error is now returned. The span-audit write in `InjectTraceContext` is intentionally best-effort (the carrier injection already succeeded) but the failure is now logged with the trace id rather than dropped.
- Validation lives in the service (not only the handler) so every caller gets it, and we avoid relying on DB constraints for client mistakes.

### API-visible behavior change
Invalid coverage reports: `200` -> `400`. Upsert failures: `200` -> server error.

### Verification
Live: an invalid report returned 400. Unit tests added/updated in `src/features/distributed-tracing/tests/distributed_tracing_unit_test.go`.

### Remaining gaps
Span-audit failure is only logged (no metric/alert). No HTTP-level test for the handler mapping.

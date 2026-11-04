# ADR: Distributed Tracing Pipeline, Observability Stack Activation, and Ingress Health Contracts

* **Status:** Accepted
* **Date:** 2026-11-04
* **Deciders:** Lead Systems Architect & Platform Core Engineering
* **Feature Scope:** `docs/features/distributed-tracing/`, `deploy/docker/compose/`, and `scripts/`

---

## 1. Context and Problem Statement

In an asynchronous event-driven platform utilizing Kafka brokers, relational event stores, and decoupled microservices, isolating errors, tracking latencies, and debugging failures without end-to-end tracing and unified logs is impossible:
1. Without distributed tracing, asynchronous event hops across Kafka partitions lose causation and parent-span context.
2. The infrastructure container definitions (`docker-compose.infra.yml`) previously placed the entire observability suite (Grafana Tempo, Grafana Loki, OpenTelemetry Collector, Prometheus, Grafana) behind an optional Compose profile (`profiles: [observability]`), leaving local environments completely blind to trace spans and logs.
3. The Envoy API Gateway was marked `unhealthy` under default Docker checks because its container image (`envoyproxy/envoy:v1.31.2`) omits `wget`, causing the container health probe to fail despite Envoy being healthy and serving traffic.
4. Hardcoded remote `@sha256:...` digest pins for images like `apicurio` and `kafka` broke compose bootstrapping because remote registries updated multi-arch manifests, causing `manifest unknown` download errors.

---

## 2. Decision Drivers

* **End-to-End Trace Lineage**: W3C Trace Context (`traceparent`, `tracestate`) must seamlessly bridge HTTP ingress (Envoy), Go service execution, Kafka message headers, and background consumer pipelines.
* **Observability-by-Default Contract**: Tracing and logging infrastructure must be active by default during developer runs and integration testing via platform scripts.
* **Minimal Attack Surface Compatibility**: Health checks in hardened or minimal distroless containers (Envoy) must not assume external utilities (`wget`/`curl`) and must use zero-dependency native shell socket primitives.
* **Hermetic Script-Based Execution**: All bootstrapping, secret creation, and telemetry validation must execute exclusively through repository scripts (`scripts/run/run.sh`, `scripts/test/test.integration.sh`).

---

## 3. Considered Options

* **Option 1: Ad-hoc Manual Docker Commands and On-Demand Telemetry**
  - *Cons:* Inconsistent developer setups, silent failure modes when running without traces, breaks CI reproducibility.
* **Option 2: Standalone Jaeger + File-based Logging**
  - *Cons:* Lacks correlation between logs, metrics, and traces. Misses unified querying across Grafana, Loki, and Tempo.
* **Option 3: Unified OpenTelemetry Architecture (OTel Collector + Tempo + Loki + Prometheus) with Native Socket Probing**
  - *Pros:* Fully compliant with OpenTelemetry standards. Single collector pipeline receives OTLP traces (`:4317`/`:4318`) and forwards traces to Grafana Tempo, logs to Loki, and metrics to Prometheus. Unifies telemetry inside Grafana (`:3000`).

---

## 4. Decision Outcome

We selected **Option 3: Unified OpenTelemetry Architecture (OTel Collector + Tempo + Loki + Prometheus) with Native Socket Probing**.

### 4.1 Telemetry Pipeline Topology

```
[Inbound Traffic / Test Runners]
         │ (HTTP / OTLP :4318)
         ▼
[Envoy Ingress & Service Producers]
         │ (Inject W3C traceparent into Kafka Headers / OTLP)
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
   - `scripts/run/run.sh` sets `COMPOSE_PROFILES="${COMPOSE_PROFILES:-observability}"` so that Tempo, Loki, Prometheus, OTel-Collector, and Grafana start automatically alongside database, broker, and cache infrastructure.
2. **Native TCP Socket Health Check ([`deploy/docker/compose/docker-compose.infra.yml`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/deploy/docker/compose/docker-compose.infra.yml))**:
   - Fixed Envoy's failing health probe by switching to `/dev/tcp/127.0.0.1/9901` socket inspection, avoiding missing binary errors (`wget: not found`).
3. **Resilient Registry Tags**:
   - Stripped fragile SHA256 digest pins in `docker-compose.infra.yml` in favor of validated SemVer image tags (`apache/kafka:3.8.0`, `apicurio/apicurio-registry-mem:2.6.5.Final`, `envoyproxy/envoy:v1.31.2`).
4. **Automated Trace & Log Ingestion Suite ([`scripts/test/test.integration.sh`](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/scripts/test/test.integration.sh))**:
   - The integration script verifies end-to-end telemetry ingestion by emitting OTLP spans and Loki log lines and asserting their queryability via Tempo (`/api/traces/{traceId}`) and Loki (`/loki/api/v1/query`).

---

## 5. Consequences

### Positive
* **Complete Observability**: Every event, error, and database operation can be correlated across logs and traces in Grafana.
* **100% Healthy Fleet**: All 10 local infrastructure and telemetry containers report `healthy` status.
* **Strict Script Contract**: All setup, secrets, and tests adhere strictly to platform runner scripts.

### Negative / Trade-offs
* **Workstation Memory Overhead**: The full suite consumes ~1.2 GB additional memory. Developers needing minimal footprints can set `COMPOSE_PROFILES="" ./scripts/run/run.sh`.

# Distributed Tracing — Failure Cases & Resilience Playbook

This document details failure modes, impact ratings, automated mitigations, and detection triggers for the **Distributed Tracing** subsystem.

---

## 1. Perimeter & Identity Failures (§3.1)

### FC-TR-01: Client Forces Unsampled High-Volume Tracing
* **Scenario:** An external API client or automated stress test injects `traceparent: ...-01` (Sampled flag = 1) across millions of requests, aiming to force 100% trace ingestion and exhaust collector storage/budget.
* **Severity:** OUTAGE / DEGRADE
* **Handling:**
  1. Invariant TR-04 mandates that external inbound trace context is untrusted.
  2. Edge Envoy gateway strips or overrides external sampling flags. The platform determines sampling independently via deterministic correlation hash or rate limiters.
* **Detection:** Spike in `otelcol_receiver_refused_spans` or edge rejected sampled flag metrics.

### FC-TR-02: Header Explosion via Malicious or Oversized Baggage
* **Scenario:** Upstream clients send massive `tracestate` or OpenTelemetry `baggage` headers containing multi-kilobyte payloads, causing downstream microservices to fail HTTP request header size limits (`431 Request Header Fields Too Large`).
* **Severity:** OUTAGE
* **Handling:**
  1. Envoy gateway truncates `tracestate` and strips untrusted `baggage` at the edge boundary.
  2. SDK baggage validator enforces a maximum of 16 key-value pairs and 256 bytes per entry.
* **Detection:** Ingress 431 status code counter alarms.

---

## 2. Asynchronous Propagation & Outbox Breakages (§3.4, §3.5)

### FC-TR-03: Context Omitted at Transactional Outbox Insert
* **Scenario:** A service command handler writes an event to the `transactional_outbox` table without serializing the active OpenTelemetry context into the table row.
* **Severity:** DEGRADE (Observability)
* **Handling:**
  1. Invariant TR-02 dictates that `C-OBXT` helper libraries require active `ctx` and automatically serialize `traceparent` and `tracestate` into dedicated columns at SQL insertion time.
  2. Downstream relay (Debezium/Poller) copies the stored context directly into Kafka record headers.
* **Detection:** Daily trace completeness auditor flags asynchronous consumer spans whose root is an orphan without a producer parent/link.

### FC-TR-04: Relay Usurps Business Parent Span
* **Scenario:** The outbox relay daemon starts its own processing span and sets it as the parent of the Kafka record, nesting downstream business logic inside the transport poller lifecycle.
* **Severity:** DEGRADE (Waterfall Distortions)
* **Handling:**
  1. Outbox relay creates independent spans with `SpanLink` to the captured outbox context, never adopting or replacing the business parent context.
* **Detection:** Linting and automated golden trace verification checking parent-child lineage.

---

## 3. Data Leakage & Compliance Violations (§3.3, §9)

### FC-TR-05: PII and Raw SQL Parameters Leaked in Span Attributes
* **Scenario:** Database instrumentation captures raw SQL queries containing cleartext user credentials, social security numbers, or payment card numbers in `db.statement`.
* **Severity:** SECURITY / COMPLIANCE
* **Handling:**
  1. Invariant TR-09 mandates query parameterization and statement sanitization (`SELECT ... WHERE id = $1`). Raw literal values are replaced with placeholders before span creation.
  2. Host-local collector agents run regex redaction processors (`redaction/pii`) scrubbing common patterns (credit cards, JWTs, bearer tokens) before forwarding spans.
* **Detection:** Automated scanner scanning Tempo trace storage for unmasked sensitive patterns.

---

## 4. Collector Pipeline & Tail Sampling Failures (§7, §10)

### FC-TR-06: Collector Memory Exhaustion Drops Live Spans
* **Scenario:** A sudden traffic burst or network partition causes spans to accumulate in collector agent queues until host memory is exhausted, triggering an OOM kill.
* **Severity:** DEGRADE
* **Handling:**
  1. Invariant TR-10 enforces that tracing never impairs application availability.
  2. Collectors configure the `memory_limiter` processor with hard drop thresholds (drop spans when memory reaches 80% soft limit).
  3. Applications use bounded non-blocking batch span processors (`max_queue_size=2048`, `drop_on_queue_full=true`).
* **Detection:** Prometheus alarm on `otelcol_processor_dropped_spans > 0`.

### FC-TR-07: Incomplete Traces in Tail Sampler via Broken Hashing
* **Scenario:** Spans belonging to the same `trace_id` are routed to different collector gateway instances. The tail sampler evaluates decisions on partial spans and prematurely drops error traces.
* **Severity:** LOSS (Diagnostic)
* **Handling:**
  1. Invariant TR-07 mandates trace-id-aware routing. Host agents use the `loadbalancingexporter` configured with `routing_key: trace_id` to guarantee all spans of a trace land on the same gateway replica.
* **Detection:** Monitor `otelcol_tail_sampling_early_drops` and orphan span metrics in Tempo.

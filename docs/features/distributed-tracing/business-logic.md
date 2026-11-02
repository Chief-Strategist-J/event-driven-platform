# Distributed Tracing — Business Logic Decision Tree

```
DISTRIBUTED TRACING (BUSINESS DECISION TREE)
│
├── 1. W3C CONTEXT INJECTION & TRACE CONTINUITY TIER
│   └── Trigger: Client Initiates Inbound Request at API Gateway (Envoy)
│       ├── Step 1: Trace Context Initialization
│       │   ├── Check: Does inbound request contain W3C `traceparent` header?
│       │   │   ├── YES ──> Extract `TraceID` and `ParentSpanID`; propagate context
│       │   │   └── NO ──> Generate new 128-bit `TraceID` and 64-bit root `SpanID`
│       │   └── Format: `traceparent: 00-<4a3...32chars>-<00f...16chars>-01` (Sampled flag = 01)
│       │
│       ├── Step 2: OpenTelemetry Span Lifecycle in Go Service
│       │   ├── Span Initialization: `ctx, span := tracer.Start(ctx, "OrderService.PlaceOrder")`
│       │   ├── Tag Enrichment:
│       │   │   ├── `peer.service = "kafka"`
│       │   │   ├── `messaging.system = "kafka"`
│       │   │   ├── `messaging.destination.name = "orders.order.placed.v1"`
│       │   │   ├── `app.tenant_id = tenantID`
│       │   │   └── `app.aggregate_id = orderID`
│       │   └── Invariant: NEVER log passwords, credit card numbers, or PII into span attributes
│       │
│       └── Step 3: Context Injection across Asynchronous Kafka Boundary
│           ├── Action: Inject OpenTelemetry propagation context into Kafka Record Headers
│           ├── Resulting Headers:
│           │   ├── Header `traceparent`: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
│           │   └── Header `tracestate`: Vendor-specific correlation metadata
│           └── Value: Preserves causality and end-to-end trace tree across decoupled consumer boundaries
│
├── 2. ASYNCHRONOUS CONSUMER CONTINUATION & SPAN LINKING
│   └── Trigger: Downstream Consumer Polls Kafka Message
│       ├── Trace Extraction Choice:
│       │   ├── Option A: Child Span Continuation (Sequential Processing)
│       │   │   ├── Action: `ctx := propagator.Extract(context.Background(), recordHeaders)`
│       │   │   ├── Create: Child span with `parent = record.TraceParent`
│       │   │   └── Result: Consumer span appears directly nested beneath producer publish span
│       │   │
│       │   └── Option B: Span Links (Batch / Fan-in Aggregation Processing)
│       │       ├── When: Consumer polls a batch of 100 records from multiple independent trace contexts
│       │       ├── Pattern: Create fresh root span for batch processing; attach `SpanLink` to each record's TraceID
│       │       └── Advantage: Avoids artificial single-parent nesting across unrelated customer transactions
│       │
│       └── Exception Recording:
│           ├── Error Occurs: `span.RecordError(err)` + `span.SetStatus(codes.Error, err.Error())`
│           └── Span Close: `span.End()` on deferred exit; flushes to OTel SDK buffer
│
└── 3. TELEMETRY PIPELINE ROUTING & VISUALIZATION TIER
    └── Trigger: OpenTelemetry SDK Exports Spans via OTLP/gRPC (:4317)
        ├── OTel Collector Processing Pipeline:
        │   ├── Receiver: `otlp` gRPC (:4317) & HTTP (:4318)
        │   ├── Processor: `batch` (Flushes every 1s or 1024 spans to optimize network calls)
        │   └── Exporters:
        │       ├── Exporter A: Export traces to Grafana Tempo / Jaeger on port 4317
        │       ├── Exporter B: Generate RED metrics (Rate, Errors, Duration) via `spanmetrics` processor
        │       └── Exporter C: Route span metrics to Prometheus (:8889)
        │
        └── Unified Observability in Grafana (:3000):
            ├── Trace-to-Metrics: Clicking error rate spike in Grafana jumps directly to exemplar trace in Tempo
            └── Trace-to-Logs: TraceID automatically correlated to structured JSON log lines in Loki
```

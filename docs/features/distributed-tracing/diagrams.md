# Distributed Tracing — Architecture & Context Propagation Diagrams

This document contains Mermaid diagrams illustrating end-to-end W3C trace context propagation, parent vs. link decisions across asynchronous messaging boundaries, OpenTelemetry collector pipeline topologies, and outbox context capture.

---

## 1. End-to-End Context Propagation Topology (Edge to Consumer)

```mermaid
flowchart TD
    subgraph EdgeBoundary ["Perimeter (Untrusted Client Boundary)"]
        CLIENT["External Client<br/>(Optional traceparent)"] -->|Untrusted Header| ENVOY["Envoy API Gateway"]
        ENVOY -->|TR-04: Start New Trace<br/>Link to Client Trace| SVC_A["Order Service (ConnectRPC Server)"]
    end

    subgraph ServiceATx ["Service A: Business Transaction (ACID)"]
        SVC_A -->|Active Span: PlaceOrder| DB_OP["Execute Business Mutex (Postgres)"]
        SVC_A -->|TR-02: Capture W3C Context| OUTBOX[("Transactional Outbox Row<br/>traceparent, tracestate, correlation_id")]
        DB_OP -.->|Atomic Commit| OUTBOX
    end

    subgraph TransportTier ["Asynchronous Relay & Messaging Tier"]
        OUTBOX -->|WAL Tail / Poller| RELAY["Outbox Relay / Debezium"]
        RELAY -->|Relay Span Links to Source<br/>Never becomes Parent| KAFKA["Kafka Topic: commerce.order.placed.v1<br/>(Record Headers: traceparent, tracestate)"]
    end

    subgraph ConsumerTier ["Downstream Asynchronous Consumer Tier"]
        KAFKA -->|Poll Record Batch| CONSUMER["Payment Service Consumer"]
        CONSUMER -->|TR-05: Parent vs Link Decision| C_SPAN["Consumer Child Span or Linked Span"]
        C_SPAN -->|Execute Effect| PAY_DB[("Payment DB / External Gateway")]
    end
```

---

## 2. Asynchronous Boundary: Parent vs. Link Decision Flow (§3.6)

```mermaid
flowchart TD
    MSG["Kafka Record Received with traceparent"] --> D_CHECK{"When did the message arrive?"}
    
    D_CHECK -- "Gaps > 5 Minutes<br/>(Batch / Backlog / Replay)" --> LINK_FLOW["Option B: Create New Root Trace<br/>Attach SpanLink to Producer Context"]
    LINK_FLOW --> RATIONALE1["Rationale: Prevents distorted, hours-long spans<br/>in APM waterfall visualizations"]

    D_CHECK -- "Sub-Minute Immediate Processing<br/>(Single Message)" --> PARENT_FLOW{"Is Consumer Processing a Batch?"}
    
    PARENT_FLOW -- "Single Record Processing" --> CHILD_SPAN["Option A: Direct Child Span<br/>parent_span_id = record.span_id"]
    CHILD_SPAN --> RATIONALE2["Rationale: Unbroken causality tree for<br/>real-time synchronous pipelines"]

    PARENT_FLOW -- "Batch Processing (Fan-in of 50+ records)" --> BATCH_LINK["Option C: Batch Root Span<br/>Attach SpanLinks to each unique traceparent"]
    BATCH_LINK --> RATIONALE3["Rationale: Prevents artificial single-parent nesting<br/>across unrelated customer transactions"]
```

---

## 3. OpenTelemetry Collector Pipeline & Tail Sampling Topology (§7)

```mermaid
flowchart LR
    subgraph HostAgents ["Node-Local DaemonSets"]
        APP1["App Pod A (Go)"] -->|OTLP / gRPC :4317| AGENT["OTel Collector Agent (Host-Local)"]
        APP2["App Pod B (Kotlin)"] -->|OTLP / gRPC :4317| AGENT
        AGENT -->|Memory Limiter + Redaction Filter| ROUTER["Trace-ID Aware Hash Load Balancer"]
    end

    subgraph GatewayCluster ["Collector Gateway Tier (Tail Sampling Cluster)"]
        ROUTER -->|Hash(TraceID) % N| GW1["Gateway Collector 1"]
        ROUTER -->|Hash(TraceID) % N| GW2["Gateway Collector 2"]
        
        GW1 --> METRICS1["TR-08: Pre-Sampling SpanMetrics Processor<br/>(Unsampled Rate, Error, Duration)"]
        METRICS1 --> SAMPLER1["TR-07: Tail-Sampling Processor<br/>(Keeps 100% Errors, 100% P99, 1% Healthy)"]
        
        GW2 --> METRICS2["TR-08: Pre-Sampling SpanMetrics Processor"]
        METRICS2 --> SAMPLER2["TR-07: Tail-Sampling Processor"]
    end

    subgraph Sinks ["Storage & Observability Backends"]
        METRICS1 -->|Prometheus Remote Write| PROM[("Prometheus / Mimir")]
        METRICS2 --> PROM
        SAMPLER1 -->|OTLP Export| TEMPO[("Grafana Tempo / Trace Storage")]
        SAMPLER2 --> TEMPO
    end
```

---

## 4. In-Transaction Outbox Capture & Relay Span Linking Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Client / Upstream Service
    participant Handler as Command Handler (OrderService)
    participant Tracer as OpenTelemetry Tracer (Go SDK)
    participant DB as Postgres (Order State + Outbox)
    participant Relay as Outbox Debezium Relay
    participant Broker as Apache Kafka Broker
    participant Consumer as Downstream Worker

    Caller->>Handler: Invoke ProcessCommand(ctx)
    activate Handler
    Handler->>Tracer: Start Span: "OrderService.SubmitOrder"
    activate Tracer
    Tracer-->>Handler: ctx with active SpanID (e.g. 00f067aa)
    
    Handler->>Tracer: Inject(ctx, carrier) -> W3C Header String
    Tracer-->>Handler: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    
    Handler->>DB: BEGIN TRANSACTION
    Handler->>DB: INSERT INTO orders VALUES (...)
    Handler->>DB: INSERT INTO transactional_outbox (trace_context, correlation_id, payload)
    Handler->>DB: COMMIT TRANSACTION
    Handler-->>Caller: 202 Accepted (Order ID, Correlation ID)
    Handler->>Tracer: End Span
    deactivate Tracer
    deactivate Handler

    Note over Relay,Broker: Relay runs asynchronously outside business span
    Relay->>DB: Poll PENDING outbox rows
    Relay->>Broker: Produce Record with Headers [traceparent = stored_trace_context]
    Broker-->>Relay: Broker ACK
    Relay->>DB: Mark Outbox Row PUBLISHED

    Note over Broker,Consumer: Consumer extracts trace without parent collision
    Broker->>Consumer: Poll message batch
    activate Consumer
    Consumer->>Tracer: Extract(record.Headers) -> parentContext
    Consumer->>Tracer: Start Span: "PaymentService.HandleOrderPlaced", Link(parentContext)
    Consumer->>Consumer: Execute Idempotent Processing
    Consumer->>Tracer: End Span
    deactivate Consumer
```


---

## 3. OpenTelemetry Collector Pipeline & Tail Sampling Topology (§7)

```mermaid
flowchart LR
    subgraph HostAgents ["Node-Local DaemonSets"]
        APP1["App Pod A (Go)"] -->|OTLP / gRPC :4317| AGENT["OTel Collector Agent (Host-Local)"]
        APP2["App Pod B (Kotlin)"] -->|OTLP / gRPC :4317| AGENT
        AGENT -->|Memory Limiter + Redaction Filter| ROUTER["Trace-ID Aware Hash Load Balancer"]
    end

    subgraph GatewayCluster ["Collector Gateway Tier (Tail Sampling Cluster)"]
        ROUTER -->|Hash(TraceID) % N| GW1["Gateway Collector 1"]
        ROUTER -->|Hash(TraceID) % N| GW2["Gateway Collector 2"]
        
        GW1 --> METRICS1["TR-08: Pre-Sampling SpanMetrics Processor<br/>(Unsampled Rate, Error, Duration)"]
        METRICS1 --> SAMPLER1["TR-07: Tail-Sampling Processor<br/>(Keeps 100% Errors, 100% P99, 1% Healthy)"]
        
        GW2 --> METRICS2["TR-08: Pre-Sampling SpanMetrics Processor"]
        METRICS2 --> SAMPLER2["TR-07: Tail-Sampling Processor"]
    end

    subgraph Sinks ["Storage & Observability Backends"]
        METRICS1 -->|Prometheus Remote Write| PROM[("Prometheus / Mimir")]
        METRICS2 --> PROM
        SAMPLER1 -->|OTLP Export| TEMPO[("Grafana Tempo / Trace Storage")]
        SAMPLER2 --> TEMPO
    end
```

---

## 4. In-Transaction Outbox Capture & Relay Span Linking Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Client / Upstream Service
    participant Handler as Command Handler (OrderService)
    participant Tracer as OpenTelemetry Tracer (Go SDK)
    participant DB as Postgres (Order State + Outbox)
    participant Relay as Outbox Debezium Relay
    participant Broker as Apache Kafka Broker
    participant Consumer as Downstream Worker

    Caller->>Handler: Invoke ProcessCommand(ctx)
    activate Handler
    Handler->>Tracer: Start Span: "OrderService.SubmitOrder"
    activate Tracer
    Tracer-->>Handler: ctx with active SpanID (e.g. 00f067aa)
    
    Handler->>Tracer: Inject(ctx, carrier) -> W3C Header String
    Tracer-->>Handler: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    
    Handler->>DB: BEGIN TRANSACTION
    Handler->>DB: INSERT INTO orders VALUES (...)
    Handler->>DB: INSERT INTO transactional_outbox (trace_context, correlation_id, payload)
    Handler->>DB: COMMIT TRANSACTION
    Handler-->>Caller: 202 Accepted (Order ID, Correlation ID)
    Handler->>Tracer: End Span
    deactivate Tracer
    deactivate Handler

    Note over Relay,Broker: Relay runs asynchronously outside business span
    Relay->>DB: Poll PENDING outbox rows
    Relay->>Broker: Produce Record with Headers [traceparent = stored_trace_context]
    Broker-->>Relay: Broker ACK
    Relay->>DB: Mark Outbox Row PUBLISHED

    Note over Broker,Consumer: Consumer extracts trace without parent collision
    Broker->>Consumer: Poll message batch
    activate Consumer
    Consumer->>Tracer: Extract(record.Headers) -> parentContext
    Consumer->>Tracer: Start Span: "PaymentService.HandleOrderPlaced", Link(parentContext)
    Consumer->>Consumer: Execute Idempotent Processing
    Consumer->>Tracer: End Span
    deactivate Consumer
```
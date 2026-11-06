# Distributed Tracing — Business Logic Decision Tree

```

DISTRIBUTED TRACING — PRODUCTION SYSTEM DESIGN TREE (v1)
Legend  : [C] Certain   [L] Likely (verify per SDK/collector/Debezium version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-SDK   OpenTelemetry SDKs — Go (explicit context), Kotlin/JVM (agent or manual; coroutine context integration)
│   ├── C-EDGE  Envoy gateway tracing — edge spans, inbound context policy, egress stripping
│   ├── C-RPCI  ConnectRPC instrumentation — client/server spans, retries as attempts
│   ├── C-DBI   DB/cache/search client instrumentation — sanitized statements, pool wait spans
│   ├── C-OBXT  Outbox trace capture — context stored in outbox row at insert
│   ├── C-RLYT  Relay propagation — copies stored context into Kafka headers (Debezium/outbox router) [L]
│   ├── C-MSGI  Messaging instrumentation — producer/consumer/process spans with links
│   ├── C-COLA  Collector agents (per node) — receive, memory-limit, redact, forward
│   ├── C-COLG  Collector gateways — trace-id-aware load balancing → tail sampling → span metrics → export
│   ├── C-TEMPO Trace backend (Tempo) — storage, search, retention
│   ├── C-CORR  Correlation fabric — trace id in logs, exemplars on metrics, correlation/event ids in spans
│   └── C-MON   Tracing health — drop counts, propagation coverage, sampling ratios
│
├── 1. INVARIANTS  (with enforcement)
│   ├── TR-01 Tracing is diagnostic, never a record or a workflow driver [C]
│   │   └── Enforced by: business flows tracked by envelope correlation/causation ids; lint forbids reading trace ids in domain code
│   ├── TR-02 Context is captured at the source of every async hop [C]
│   │   └── Enforced by: outbox library stores context at insert; reacting consumers capture their own span context into their outbox
│   ├── TR-03 One propagation format end to end (W3C trace context) [C]
│   │   └── Enforced by: SDK/Envoy/collector configuration; legacy formats translated only at defined boundaries
│   ├── TR-04 External inbound trace context is untrusted [C]
│   │   └── Enforced by: edge policy — start new trace, link to client context; ignore client sampled flag
│   ├── TR-05 Async boundaries follow a declared parent/link policy [C]
│   │   └── Enforced by: messaging instrumentation library (§3)
│   ├── TR-06 Sampling is consistent per business flow [C]
│   │   └── Enforced by: deterministic sampling on correlation id (or propagated decision) across all services (§4)
│   ├── TR-07 Tail sampling sees whole traces [C]
│   │   └── Enforced by: trace-id-aware load balancing to gateway collectors (§7)
│   ├── TR-08 Span-derived metrics are computed before sampling [C]
│   │   └── Enforced by: collector pipeline order: span metrics → tail sampling → export
│   ├── TR-09 No personal data, secrets, or payloads in spans or baggage [C]
│   │   └── Enforced by: attribute allow-lists, redaction processors, statement sanitization, scanners
│   ├── TR-10 Tracing never blocks or fails the application [C]
│   │   └── Enforced by: bounded exporter queues with drop policy, non-blocking export, timeouts
│   ├── TR-11 Bounded size and cardinality [C]
│   │   └── Enforced by: SDK limits (attributes, events, links), low-cardinality span names
│   ├── TR-12 Context is per unit of work, never ambient across records [C]
│   │   └── Enforced by: per-record context objects in lanes; no thread-local reuse across records (multi-tenant MT-05 analogue)
│   └── TR-13 Propagation coverage is measured per hop [C]
│       └── Enforced by: coverage metrics (§11) with alerts on regressions
│
├── 2. SCOPE DECISION — WHAT TRACING IS FOR
│   ├── Use tracing for
│   │   ├── Latency breakdown of synchronous requests (edge → services → DB)
│   │   ├── Diagnosing a single failing/slow processing of an event (consumer span with DB/RPC children)
│   │   ├── Async hop latency samples (producer commit → relay → consume) via linked spans
│   │   └── Dependency maps (service graph) from sampled data
│   ├── Do NOT use tracing for
│   │   ├── "Did order X complete?" → flow monitor by correlation id (communication tree C-MON)
│   │   ├── Audit or compliance evidence → audit trail tree
│   │   ├── Exact counts / SLO numerators → metrics (unsampled)
│   │   └── Business routing, idempotency, or correlation in code → envelope fields (TR-01)
│   └── E2.1 Business process relies on trace lookup to find related events — sampling drops it — CORRUPT (ops decisions) [C]
│
├── 3. PROPAGATION — BOUNDARY BY BOUNDARY
│   │
│   ├── 3.1 External client → Edge (Envoy)
│   │   ├── Check: trusted partner with contractual tracing?
│   │   │   ├── YES → continue trace (still ignore sampled flag for cost control) [L]
│   │   │   └── NO → new root trace; link to client-supplied context (if present) for support cases
│   │   ├── tracestate: size-limited, untrusted entries dropped
│   │   └── Edge cases
│   │       ├── E3.1a Client forces sampled=1 on every request — 100% sampling — cost/collector OUTAGE [C]
│   │       ├── E3.1b Client-supplied trace id collides/reused across users — unrelated requests merged in one trace [C]
│   │       └── E3.1c Oversized tracestate/baggage headers — header limit errors — request failures [C]
│   │
│   ├── 3.2 Edge → services → services (ConnectRPC)
│   │   ├── Propagate W3C context; server span per call; client span per attempt (retries visible as separate attempts)
│   │   ├── Deadline propagation is separate from tracing (communication tree §3.1)
│   │   └── Edge cases
│   │       ├── E3.2a Envoy sidecar and app both create server spans with inconsistent parenting — confusing trees [C]
│   │       │   └── Handling: decide span ownership (proxy spans vs app spans) and parent correctly; test
│   │       ├── E3.2b Retries reuse one client span — retry storms invisible in traces [C]
│   │       └── E3.2c Mixed propagation formats between services — trace splits at that hop [C]
│   │
│   ├── 3.3 Service → datastores (PostgreSQL, OpenSearch, Redis, KMS)
│   │   ├── Client spans with sanitized statements (no parameter values) [C]
│   │   ├── Connection pool acquisition as its own span (pool waits are a top latency cause)
│   │   ├── Transaction span wrapping statements in business tx (commit duration visible)
│   │   └── E3.3a Full SQL with literal values captured — PII and secrets in traces — COMPLIANCE [C]
│   │
│   ├── 3.4 Business transaction → outbox (TR-02)
│   │   ├── At outbox insert: serialize current context (traceparent, tracestate) into outbox row columns
│   │   ├── Baggage NOT copied (or only allow-listed keys) — events outlive requests and cross trust boundaries
│   │   └── E3.4a Context omitted from outbox row — every async continuation becomes an orphan root — DEGRADE (diagnosis) [C]
│   │
│   ├── 3.5 Relay → Kafka
│   │   ├── Relay copies stored context to record headers (event router additional field placement as header) [L]
│   │   ├── Relay's own processing span (if any) LINKS to the stored context; never becomes its parent [C]
│   │   ├── Debezium tracing integrations exist but behavior varies by version — verify [L]
│   │   └── Edge cases
│   │       ├── E3.5a Relay extracts "current span" (its own) — all events appear caused by the relay — misleading [C]
│   │       ├── E3.5b Header names changed by connector upgrade — propagation silently breaks (schema evolution SE-09) [L]
│   │       └── E3.5c Raw table CDC (no context in rows) — consumers start new roots — expected; document [C]
│   │
│   ├── 3.6 Kafka → consumer (parent vs link policy, TR-05)
│   │   ├── Check: processing model
│   │   │   ├── Single record per processing span, consumed shortly after production
│   │   │   │   └── Option A: process span child of producer context (one trace end-to-end) [L]
│   │   │   ├── Single record, consumed after long delay (backlog, retry schedule, DLQ redrive, replay)
│   │   │   │   └── New trace; process span LINKS to producer context (avoid days-long traces)
│   │   │   └── Batch processing (many records in one span)
│   │   │       └── New trace; batch span with links to each record context (link limit applies) [L]
│   │   ├── Recommended default: always new trace + link for async; optional "child if lag < threshold" [L: threshold 10–30 s]
│   │   ├── Consumer span attributes: system, destination, partition, offset, consumer group, event id, event type,
│   │   │   aggregate type, decision outcome (APPLIED/DUPLICATE/STALE/GAP/HELD)
│   │   └── Edge cases
│   │       ├── E3.6a Batch span with 500 links exceeds SDK link limit — links silently dropped [C]
│   │       │   └── Handling: per-record child spans for sampled records; batch-level summary span
│   │       ├── E3.6b Child-of policy with multi-hour backlog — trace durations of hours — backend search noise [C]
│   │       └── E3.6c Lane threads reuse previous record's context — spans attached to wrong trace — CORRUPT (diagnosis) [C]
│   │           └── Handling: TR-12; Go: explicit context per record; JVM: context scoped per task; coroutine context propagation [L]
│   │
│   ├── 3.7 Consumer → follow-up events (reacting consumers)
│   │   ├── Capture consumer process span context into its outbox rows (TR-02)
│   │   └── Chain continues: each hop links/parents per §3.6 policy
│   │
│   ├── 3.8 Delayed paths
│   │   ├── Scheduled retry (minutes later) → new trace linked to original attempt's context (stored with held item)
│   │   ├── DLQ quarantine → store original traceparent in envelope; redrive creates new trace linked to it
│   │   ├── Replay/rebuild → spans marked replay; low sampling; no links into expired original traces (optional)
│   │   └── Sagas → each step's processing is its own trace; saga id + correlation id tie them (not one giant trace)
│   │
│   ├── 3.9 Fan-out
│   │   ├── One event → N consumer groups → N independent traces linked to same producer context
│   │   └── E3.9a Fan-out modeled as children of one trace — traces with thousands of spans — backend limits/drops [C]
│   │
│   ├── 3.10 Egress to third parties
│   │   ├── Strip internal trace context on calls to external partners (or send fresh ids) [L]
│   │   └── E3.10a Internal trace ids/tracestate sent to partners — leaks internal topology; partners can correlate users [C]
│   │
│   └── 3.11 Mirrored / DR clusters
│       └── Headers preserved by mirroring tool? verify [L]; DR consumers produce traces in DR region (residency)
│
├── 4. SAMPLING
│   ├── 4.1 Layers
│   │   ├── Head: parent-based at edge; default ratio per tier [G: e.g., T0 higher than T2]
│   │   ├── Tail: at gateway collectors with policies (keep errors, latency outliers, T0 flagged flows, canaries)
│   │   └── Consistent async sampling: decision derived deterministically from correlation id (same decision in every
│   │       service and every async hop) — keeps whole flows, not fragments (TR-06) [L: consistent probability sampling approaches]
│   │
│   ├── 4.2 Tail sampling mechanics
│   │   ├── Trace-id-aware load balancing so all spans of a trace reach one gateway instance (TR-07) [C]
│   │   ├── Decision wait window sized to typical trace completion [L: 10–30 s]
│   │   ├── Late spans after decision are dropped or kept per policy — async continuations are separate traces anyway
│   │   └── Memory bounded: traces in flight × spans per trace
│   │
│   ├── 4.3 Error semantics for sampling
│   │   ├── Business rejections (REJECTED, DUPLICATE, STALE, EXPIRED) are NOT span errors
│   │   ├── Errors = failures needing attention (exceptions, timeouts, dependency errors)
│   │   └── E4.3a Every duplicate/stale event marked error — error-keep policy keeps everything — cost OUTAGE [C]
│   │
│   ├── 4.4 Rare-but-important flows
│   │   ├── T0 flows, specific incident correlation ids (temporary "always sample" list via config, time-boxed)
│   │   └── E4.4a Debug "sample 100% for tenant X" left on — cost and privacy creep [C]
│   │       └── Handling: debug sampling rules expire automatically
│   │
│   └── Edge cases
│       ├── E4.1 Independent per-trace sampling across async hops — producer trace kept, consumer trace dropped — broken story [C]
│       ├── E4.2 Multiple gateway collectors without trace-id routing — tail decisions on partial traces — inconsistent keeps [C]
│       ├── E4.3 Span metrics computed after sampling — rates/latencies wrong by sampling factor — CORRUPT (ops) [C]
│       └── E4.4 Sampling rate changed during incident without annotation — dashboards misread [C]
│
├── 5. SPAN DESIGN & SEMANTIC CONVENTIONS
│   ├── 5.1 Names: low cardinality (operation + route template / destination), never ids [C]
│   ├── 5.2 Kinds: SERVER/CLIENT for RPC; PRODUCER for publish; CONSUMER/process for handling; INTERNAL for lanes/steps
│   ├── 5.3 Attributes: follow OpenTelemetry semantic conventions per area (rpc, db, messaging) consistently across Go and Kotlin [L: versions evolve]
│   ├── 5.4 Business attributes (allow-list): event_type, aggregate_type, tier, tenant id (opaque, allowed in traces not metrics),
│   │        correlation_id, causation_id, event_id, decision outcome, quarantine class, retry tier
│   ├── 5.5 Span events: retry attempt, breaker open, held, quarantined, version decision, upcast applied, decrypt outcome class
│   ├── 5.6 Status: error only for TR §4.3 failures; status message sanitized
│   ├── 5.7 Limits (TR-11): attribute count/length, events per span, links per span — set explicitly, monitored
│   └── Edge cases
│       ├── E5.1 Span name includes aggregate id — span-name cardinality explosion in backend indexes [C]
│       ├── E5.2 Exception stack traces with customer data recorded as span events — COMPLIANCE [C]
│       └── E5.3 Go and Kotlin services use different attribute names for the same concept — queries miss half the fleet [C]
│
├── 6. CORRELATION WITH LOGS & METRICS
│   ├── 6.1 Logs: trace id, span id, correlation id, event id in every structured log line
│   ├── 6.2 Metrics: exemplars (trace ids) on latency histograms — only for sampled traces (expected)
│   ├── 6.3 Span metrics (RED) computed pre-sampling (TR-08) — or rely on native app metrics instead
│   ├── 6.4 Navigation: alert → dashboard → exemplar trace → logs by trace id → event metadata (no payloads)
│   └── E6.1 Logs carry trace id only — async continuations unfindable when trace not sampled [C]
│       └── Handling: correlation id and event id in logs are the durable keys
│
├── 7. COLLECTOR & BACKEND ARCHITECTURE
│   ├── 7.1 SDK export
│   │   ├── Batch exporter, bounded queue, drop on full (TR-10), short export timeout
│   │   └── E7.1a Synchronous/blocking exporter — collector outage stalls request threads — OUTAGE [C]
│   ├── 7.2 Agents (per node)
│   │   ├── Memory limiter first in pipeline; redaction processor; batching; forward to gateways
│   │   └── E7.2a Agent without memory limiter — OOM under span burst — node pressure affects apps [C]
│   ├── 7.3 Gateways
│   │   ├── Trace-id-aware load balancing tier → tail sampling tier (TR-07)
│   │   ├── Pipeline order: receive → memory limit → redact → span metrics → tail sample → batch → export
│   │   └── Horizontal scaling changes trace-id routing → in-flight traces split during scale events [L]
│   ├── 7.4 Backend (Tempo)
│   │   ├── Retention per environment [G]; search over allow-listed attributes
│   │   ├── Ingestion limits per tenant/service (protect backend from one noisy service)
│   │   └── E7.4a Backend ingestion limit hit during incident — traces dropped exactly when needed [C]
│   │       └── Handling: priority for error/T0 traces; capacity headroom; drop-count alerts
│   ├── 7.5 Independence (dashboard tree OD-08): tracing pipeline not dependent on the Kafka/DBs it observes
│   └── 7.6 Multi-region: traces stored in-region for residency; cross-region flows linked by correlation id
│
├── 8. SECURITY & PRIVACY  (TR-09)
│   ├── Redaction: SQL parameters, URL query strings, authorization headers, cookies, tokens, request bodies
│   ├── Baggage: allow-list keys only; never PII; stripped at egress
│   ├── Access: trace UI scoped by role; tenant-scoped views for support; audit of trace queries for T0 services
│   ├── Retention aligned with privacy rules; erasure: traces must not contain personal data (so no erasure needed)
│   └── Edge cases
│       ├── E8.1 Authorization header captured by HTTP auto-instrumentation — credential leak — SECURITY [C]
│       ├── E8.2 User email set as baggage for "debugging" — propagated to every service and vendor — COMPLIANCE [C]
│       └── E8.3 Trace backend hosted in another region — residency violation — COMPLIANCE [L]
│
├── 9. TIMING & CLOCKS
│   ├── Durations from monotonic clocks; timestamps from wall clock (NTP-synced)
│   ├── Cross-host skew makes children appear before parents — expected small; alert on large offsets
│   └── E9.1 Async lag computed from span timestamps across hosts — skew distorts lag — use envelope recorded_at + consumer clock with skew monitoring [C]
│
├── 10. RUNTIME & LIBRARY SPECIFICS
│   ├── Go: context passed explicitly; goroutines started without ctx lose traces; lanes receive per-record ctx
│   ├── JVM/Kotlin: agent auto-instrumentation vs manual — avoid double instrumentation of Kafka/HTTP clients [L]
│   │   └── Coroutines need context propagation integration; thread pools need context wrapping [L]
│   ├── Kafka client auto-instrumentation creates producer/consumer spans that may conflict with outbox-based propagation
│   │   └── Decide: disable auto Kafka spans in relay, or align them to stored context
│   ├── Version pinning of SDKs/conventions across languages; upgrade tests for attribute names
│   └── Edge cases
│       ├── E10.1 Double instrumentation (agent + manual) — duplicate spans, inflated span metrics [C]
│       ├── E10.2 Goroutine fan-out without context — child work appears as separate roots [C]
│       └── E10.3 Instrumentation overhead on 10k events/s consumer — CPU regression — DEGRADE [L]
│           └── Handling: sampling before span creation where possible; measure overhead in load tests
│
├── 11. COVERAGE & QUALITY MEASUREMENT  (TR-13)
│   ├── Propagation coverage per hop: % consumer spans with link/parent to a producer context
│   ├── Orphan root ratio per service (roots not at edge/jobs/CDC → broken propagation)
│   ├── Sampled flow completeness: for sampled correlation ids, % of expected hops present
│   ├── Drops: SDK queue drops, agent/gateway drops, backend refusals
│   ├── Redaction effectiveness: scanner hits on stored spans (must be 0)
│   └── E11.1 Propagation silently broken by a library upgrade — discovered during next incident [C]
│       └── Handling: coverage alerts per hop; canary flows always sampled and asserted end to end
│
├── 12. OBSERVABILITY OF TRACING ITSELF
│   ├── Collector health: queue sizes, memory, CPU, refused spans, export errors
│   ├── Tail sampling: decisions per policy, traces dropped for memory, late spans
│   ├── Backend: ingestion rate vs limits, query latency
│   └── Absence alerts on all of the above (tracing failure must not be silent)
│
├── 13. RUNBOOKS
│   ├── RB-01 Broken traces at a hop → check context capture at outbox/consumer outbox → header presence → library versions
│   ├── RB-02 Tracing cost spike → find sampling override / error-status inflation / client-forced sampling → correct
│   ├── RB-03 Collector overload → memory limiter engaged → scale gateways with trace-id routing → verify sampling
│   ├── RB-04 PII found in spans → stop source attribute → purge backend data where possible → scanner rule → incident
│   └── RB-05 Incident needs full flow → use correlation id in flow monitor/logs; enable time-boxed always-sample for that flow
│
└── 14. TEST MATRIX
    ├── Request → command → outbox → relay → consumer → follow-up event → second consumer: all hops linked under one correlation id
    ├── Relay processing → does not become parent of business spans
    ├── Consumer backlog of 2 hours → new trace with link (no hours-long trace)
    ├── Batch consumer → per-record sampled spans; link limits respected
    ├── Scheduled retry and DLQ redrive → new traces linked to original context
    ├── Deterministic sampling by correlation id → producer and consumer traces kept or dropped together
    ├── Gateway scale-out → tail sampling still receives complete traces
    ├── Span metrics vs native metrics → counts match within tolerance (pre-sampling)
    ├── Client sends sampled=1 → ignored; client trace id linked, not adopted
    ├── Authorization header and SQL parameters → absent from stored spans
    ├── Baggage with email → stripped
    ├── Duplicate/stale events → not marked errors; error-keep policy unaffected
    ├── Collector down → applications unaffected; drop counters rise; alert fires
    ├── Library upgrade changes header names → coverage alert fires in staging
    └── Go goroutine lanes and Kotlin coroutines → no context bleed between records
```

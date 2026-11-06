# Event Operations Dashboard — Business Logic Decision Tree

```
EVENT OPERATIONS DASHBOARD — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (verify per tool version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-PROM  Metrics (Prometheus) — HA pair or remote-write to durable store; recording rules for SLOs
│   ├── C-TEMPO Traces (Tempo) — tail sampling keeps errors/slow/flagged flows
│   ├── C-LOKI  Logs (Loki) — structured, PII-redacted, bounded label cardinality
│   ├── C-OPSX  Ops metadata index (OpenSearch) — event/quarantine METADATA only (no payloads)
│   ├── C-GRAF  Grafana — dashboards as code, folder permissions, annotations
│   ├── C-ALRT  Alerting (Alertmanager) + external dead-man's switch (monitoring tree EC-15)
│   ├── C-CTL   Control plane — the ONLY place operational actions execute (pause, scale, redrive, offsets)
│   ├── C-TOPO  Topology/catalog — streams, hops, owners, tiers, runbooks (drives dashboard generation)
│   └── C-ANN   Annotation sources — deploys, config/ACL/schema changes, failovers, incidents
│
├── 1. INVARIANTS  (with enforcement)
│   ├── OD-01 Dashboards investigate; alerts page [C]
│   │   └── Enforced by: every page originates from C-ALRT rules; dashboards linked from alerts, not watched for incidents
│   ├── OD-02 "No data" is never displayed as healthy [C]
│   │   └── Enforced by: panel standards (no null-as-zero for health metrics); explicit staleness/absence panels
│   ├── OD-03 Worst case is shown, not averages [C]
│   │   └── Enforced by: max/percentile per partition/key/tenant; averages only as secondary
│   ├── OD-04 Lag is time; freshness is per hop; business staleness is shown next to lag [C]
│   │   └── Enforced by: panels from offset tree §7 and monitoring tree §3 definitions
│   ├── OD-05 Every panel maps to a hop, an owner, and a runbook [C]
│   │   └── Enforced by: dashboard generator from C-TOPO; CI rejects panels without runbook link
│   ├── OD-06 No operational action executes from an observability tool [C]
│   │   └── Enforced by: action links open C-CTL with prefilled context; Grafana has no write credentials anywhere
│   ├── OD-07 No customer payload data in observability [C]
│   │   └── Enforced by: metadata-only indexing; log/trace redaction; scanners (encryption tree EN-08)
│   ├── OD-08 Observability is independent of the systems it observes [C]
│   │   └── Enforced by: separate failure domain for T0 telemetry path; dead-man's switch external
│   ├── OD-09 Cardinality is budgeted [C]
│   │   └── Enforced by: label allow-lists; per-tenant/key detail via logs/traces/top-N, not labels
│   └── OD-10 Every change that can cause an incident is annotated [C]
│       └── Enforced by: C-ANN integrations (deploy, schema registration, ACL, connector config, failover, offset ops)
│
├── 2. INFORMATION ARCHITECTURE  (layers; each answers one question)
│   ├── L0 BUSINESS (are customers affected?)
│   │   ├── Flow SLOs per business flow (checkout, payment, payout): success rate, end-to-end latency
│   │   ├── Canary status per stream × partition × region (monitoring tree §22)
│   │   ├── Business staleness: oldest held key age per T0 handler
│   │   ├── Correctness: unexplained drift (must be 0), business invariant violations (must be 0)
│   │   └── Shown by tier (T0/T1/T2) — never a single global error rate
│   │
│   ├── L1 PIPELINE HOP MAP (where is it broken?)
│   │   ├── H1 Outbox/WAL: slot time-to-full, unpublished outbox age
│   │   ├── H2 Relay: task state, publish rate vs outbox insert rate
│   │   ├── H3 Kafka: produce latency, throttle time, under-min-ISR, offline partitions
│   │   ├── H4 Consumers: per-partition state (OK/LAGGING/STALLED/STOPPED/PAUSED-with-reason), time lag
│   │   ├── H5 Authoritative views: processing latency, zero-row/integrity failures
│   │   ├── H6 Derived stores: fan-out lag, bulk rejections, refresh lag
│   │   └── Each hop tile: green/amber/red/UNKNOWN (UNKNOWN when its metrics are stale)
│   │
│   ├── L2 COMPONENT (why is this hop broken?)
│   │   ├── Brokers, Connect, consumer groups, databases, search clusters, KMS, PKI
│   │   └── Saturation, errors, latency, config drift, recent changes (annotations)
│   │
│   ├── L3 DRILL-DOWN (which partition / key / tenant?)
│   │   ├── Top-N held keys, hot partitions, per-tenant budgets, quarantine fingerprints
│   │   └── Links to traces (by correlation id) and logs (by event id) — metadata only
│   │
│   └── L4 FORENSIC (what exactly happened?)
│       └── Masked, audited triage tools (DLQ ledger, audit query service) — outside Grafana
│
├── 3. SIGNAL CORRECTNESS RULES
│   ├── 3.1 Staleness (OD-02)
│   │   ├── Every panel group shows "last scrape age" of its sources
│   │   ├── Absence rules on key series (absent → UNKNOWN tile, alert)
│   │   └── E3.1 Scrape target down → lag panel shows nothing → read as healthy — incident missed [C]
│   ├── 3.2 Null handling
│   │   └── E3.2 Panel configured to treat null as zero — error rate "0%" during outage [C]
│   ├── 3.3 Aggregation (OD-03)
│   │   ├── Max over partitions for lag/state; p99/p99.9 for latency; per-tier splits
│   │   └── E3.3 Group-average lag hides one partition stuck for hours — T0 key held unnoticed [C]
│   ├── 3.4 Rate windows
│   │   ├── Short windows for detection, long for trends; both visible
│   │   └── E3.4 5-minute rate smooths 30-second total outage into a dip — missed [C]
│   ├── 3.5 Units
│   │   ├── Time lag (seconds), not records (offset tree OF-10); retention headroom (time)
│   │   └── E3.5 "Lag > 100,000 records" threshold — minutes on one topic, days on another [C]
│   ├── 3.6 Paused ≠ broken
│   │   ├── Paused partitions shown with reason (breaker, backpressure, tenant halt, operator) and duration
│   │   └── E3.6 Rate 0 due to open breaker interpreted as deadlock — restart triggers storm [C]
│   └── 3.7 Clocks & time zones
│       └── Dashboards in UTC with explicit zone; annotations aligned (incident timelines across regions)
│
├── 4. TRIAGE DECISION TREE  (replaces v1 §2)
│   └── Trigger: Alert fires (OD-01) or on-call investigates a report
│       │
│       ├── Step 0: Is observability itself healthy?
│       │   ├── Dead-man's switch OK? Scrape freshness OK? Canary readers reporting?
│       │   ├── NO → treat all dashboards as UNKNOWN; use independent signals (external probes, vendor status, logs on hosts)
│       │   └── YES → Step 1
│       │
│       ├── Step 1: Business impact (L0)
│       │   ├── Which flows/tiers breach SLO? Canary cells failing? Held T0 keys aging?
│       │   ├── Correctness signals (unexplained drift, invariants, TAMPERED, PLAINTEXT-UNEXPECTED, TENANT-LEAK)?
│       │   │   └── Any non-zero → security/correctness incident path (monitoring tree §7; encryption tree §7) in parallel
│       │   └── Set severity from impact, not from metric size
│       │
│       ├── Step 2: Localize the hop (L1) — first red/UNKNOWN hop from the source side
│       │   ├── H1 Outbox/WAL red → §4.5
│       │   ├── H2 Relay red → §4.6
│       │   ├── H3 Kafka red → §4.4
│       │   ├── H4 Consumer red → §4.1
│       │   ├── Quarantine/DLQ surge → §4.2
│       │   └── H5/H6 freshness red with H1–H4 green → §4.3
│       │
│       ├── 4.1 CONSUMER STALLED / LAGGING (per partition)
│       │   ├── Check A: Partition state reason
│       │   │   ├── PAUSED: breaker open → dependency incident (retry tree RB-01); do NOT restart; do NOT scale
│       │   │   ├── PAUSED: backpressure → which dependency saturated? (backpressure tree RB-01); freeze autoscaler
│       │   │   ├── PAUSED: operator/tenant halt → verify reason and expiry
│       │   │   ├── STALLED with held key at head → DLQ/ordering workflows (parked/gap); do NOT restart
│       │   │   ├── STALLED with crash loop → crash-loop poison? (persisted attempts → quarantine-at-pickup)
│       │   │   ├── STALLED, no reason, no errors → poll-thread/lane watchdog signals → hung handler path
│       │   │   └── STOPPED (no owner) → group membership, rebalance storm, failed pods, ACL failure
│       │   ├── Check B: Rebalance storm? (rebalances per 10 min > threshold)
│       │   │   └── YES → freeze autoscaler; find cause (probes, OOM, CPU throttling, evictions) (consumer groups RB-01)
│       │   ├── Check C: LAGGING with progress
│       │   │   ├── Hot partition only → classify hot key/collision (partitioning tree §4)
│       │   │   ├── All partitions, dependency latency normal, CPU saturated → capacity → scale (within partition and
│       │   │   │   connection budgets) — only after confirming no dependency saturation
│       │   │   ├── Dependency latency elevated → backpressure path (no scale-out)
│       │   │   └── Read-committed consumers + LSO stuck → long-open producer transaction (offset tree F6.4)
│       │   └── Check D: Retention headroom (AT-RISK?) → escalate regardless of cause
│       │
│       ├── 4.2 QUARANTINE / DLQ SURGE
│       │   ├── Check A: Systemic? (same fingerprint across many keys, short window)
│       │   │   ├── YES → consumer should already be halted (DLQ tree F4.4); if not → halt via C-CTL; find producer change (annotations)
│       │   │   └── NO → single-key triage in DLQ ledger (masked tools)
│       │   ├── Check B: Class distribution
│       │   │   ├── C1/C2 (unreadable/contract) → producer rollback (never registry hotfix)
│       │   │   ├── C4 (handler bug) → consumer rollback/fix → redrive by fingerprint via C-CTL preflight
│       │   │   ├── C5 (integrity) → incident
│       │   │   └── Any ENVIRONMENTAL class present → classifier defect → page owning team (DL-08 violated)
│       │   └── Check C: Parked key count and oldest age vs SLA → escalation
│       │
│       ├── 4.3 VIEW FRESHNESS / DRIFT (H5/H6)
│       │   ├── Decompose by hop (materialized views tree §9.2) before touching stores
│       │   ├── H5 slow: projection processing latency, DB locks/bloat, zero-row failures
│       │   ├── H6 slow: fan-out lag, bulk rejections, refresh interval, cluster health (partial results!)
│       │   └── Only then: scale/tune the store that is actually the bottleneck
│       │
│       ├── 4.4 KAFKA BROKER HEALTH (URP is one signal of many)
│       │   ├── Offline partitions > 0 → OUTAGE for those partitions → broker recovery
│       │   ├── Under-min-ISR > 0 → acks=all produces failing → producer/relay errors → WAL growth risk
│       │   ├── URP > 0 → replication lag / broker down → durability risk
│       │   ├── Request latency / request queue / network handler idle low → broker saturation
│       │   ├── Disk usage / disk latency → retention or capacity
│       │   ├── Throttle time > 0 for relays/consumers → quotas (backpressure tree §11.3)
│       │   ├── Controller/quorum health → metadata operations failing
│       │   └── Leader imbalance → hot broker
│       │
│       ├── 4.5 OUTBOX / WAL (H1)
│       │   ├── Slot time-to-full, slot active, wal status, oldest transaction age, prepared transactions
│       │   └── Actions per CDC tree RB-01 (never drop slot as first response)
│       │
│       ├── 4.6 RELAY (H2)
│       │   ├── Task state (not connector state), restart count, last error
│       │   ├── Canary missing while outbox inserts continue → router/config issue
│       │   └── Actions per CDC tree RB-03
│       │
│       └── Step 3: Stabilize → Step 4: Recover → Step 5: Reconcile (monitoring tree §16 / RC jobs) → Step 6: Close
│           └── E4.1 Incident closed when lag returns to zero, without reconciliation — silent loss unexamined [C]
│
├── 5. FORBIDDEN REFLEX ACTIONS  (shown on dashboards next to the panels that tempt them)
│   ├── Restarting consumers to "unstick" lag without checking partition reasons (4.1 Check A)
│   ├── Scaling out while a dependency is saturated (backpressure tree BP-05)
│   ├── Redriving DLQ before root cause fixed and preflight passed (DLQ tree §10.4)
│   ├── Editing schemas/compatibility rules in the registry to "fix" consumers (schema evolution SE-02/03)
│   ├── Skipping offsets to unblock (offset tree OF-08)
│   ├── Dropping replication slots to free disk without bounding the gap (CDC tree §13.1)
│   ├── Increasing retention blindly without disk headroom check
│   └── Silencing alerts without expiry (monitoring tree EC-19)
│
├── 6. ACTIONS FROM DASHBOARDS  (OD-06)
│   ├── 6.1 Action links open C-CTL with context (stream, group, partition, fingerprint, incident id)
│   ├── 6.2 C-CTL enforces: identity (personal), role, preconditions, approvals (T0), reason, expiry, audit
│   ├── 6.3 Specific actions
│   │   ├── Pause: per handler/partition set via consumer control channel; reason + max duration; persisted
│   │   │   across rebalances (consumer groups tree §8 Step 4)
│   │   ├── Scale override: freezes autoscaler with expiry; checks partition count and connection budget
│   │   ├── Redrive: DLQ tree §10.4 preflight; progressive scope; rate limits
│   │   ├── Halt group (systemic): records incident link; resume requires reason
│   │   └── Offset operations: offset tree §9 only
│   └── Edge cases
│       ├── E6.1 Grafana button calls admin API with shared service account — unattributable privileged actions — SECURITY [C]
│       ├── E6.2 "Pause group" assumed broker-side — instances keep consuming / pause lost on rebalance — no effect [C]
│       ├── E6.3 Manual scale override fights autoscaler — flapping replicas — rebalance storm [C]
│       └── E6.4 Dashboard links forgeable (crafted URL triggers action) — SECURITY [C]
│           └── Handling: actions require explicit confirmation inside C-CTL with re-authentication for T0
│
├── 7. DATA PROTECTION IN OBSERVABILITY  (OD-07)
│   ├── 7.1 Logs: structured fields; PII-tag-aware redaction; no payload dumps; event ids and metadata only
│   ├── 7.2 Traces: no payloads or PII in span attributes; baggage restrictions (communication tree §9.3)
│   ├── 7.3 Ops index (C-OPSX): metadata only — event id, type, aggregate id (opaque), tenant, offsets, class, fingerprint
│   ├── 7.4 Access: Grafana folders by role; Explore access to logs/traces restricted; tenant-scoped views for support
│   ├── 7.5 Retention: observability retention aligned to incident investigation needs and privacy limits
│   ├── 7.6 Vendors/regions: hosted observability respects residency (multi-tenant tree §8)
│   └── Edge cases
│       ├── E7.1 Payloads indexed into ops OpenSearch "for debugging" — broad PII exposure, erasure gap — COMPLIANCE [C]
│       ├── E7.2 Exception messages with customer data in logs — COMPLIANCE [C]
│       └── E7.3 Any engineer can query all tenants' logs — SECURITY [C]
│
├── 8. OBSERVABILITY PLATFORM RELIABILITY  (OD-08, OD-09)
│   ├── 8.1 Independence: T0 telemetry path not dependent on the Kafka cluster/DBs it monitors
│   ├── 8.2 HA: metrics HA or remote-write; alerting HA; dead-man's switch external
│   ├── 8.3 Cardinality budget
│   │   ├── Allowed labels per metric; per-partition labels only for bounded topic sets
│   │   ├── Per-tenant/per-key metrics only for top-N + VIP (multi-tenant tree F7.3)
│   │   └── E8.1 New label (tenant_id, event_id) added in a deploy — series explosion — metrics OOM mid-incident — OUTAGE [C]
│   ├── 8.4 Query performance: recording rules for heavy SLO queries; dashboards load within seconds under incident load
│   ├── 8.5 Retention tiers: high-resolution recent, downsampled long-term
│   └── E8.2 Everyone opens the same heavy dashboard in an incident — metrics backend overloaded — blind [C]
│
├── 9. TRACING & LOGGING SPECIFICS
│   ├── 9.1 Tail sampling: keep all errors, slow flows, T0 flows (by attribute), canaries; sample the rest [L]
│   ├── 9.2 Async continuity: links from consumer spans to producer context captured at outbox (communication tree §9)
│   ├── 9.3 Correlation: logs carry trace id, correlation id, event id; one click from trace to logs (metadata)
│   ├── 9.4 Log levels: no debug in production by default; dynamic level changes audited and time-boxed
│   └── E9.1 Head sampling drops the rare failing flow — no trace for the incident — DEGRADE (diagnosis) [C]
│
├── 10. ANNOTATIONS & CHANGE CONTEXT  (OD-10)
│   ├── Deploys (service, version), schema registrations, compatibility rule changes, ACL changes, connector config,
│   │   offset operations, DLQ redrives, replays/cutovers, DB failovers, broker restarts, KMS/PKI events
│   └── E10.1 Incident caused by config change without annotation — long time-to-diagnose [C]
│
├── 11. DASHBOARD LIFECYCLE
│   ├── Dashboards as code, generated from C-TOPO (new stream/handler → panels + alerts + runbooks automatically)
│   ├── Ownership per dashboard; review on change; deprecated panels removed
│   ├── Game days validate that dashboards lead to the right runbook under synthetic incidents
│   └── E11.1 New consumer group never added to dashboards — invisible stalls — LOSS [C]
│       └── Handling: generation from topology; coverage check (every group has panels and alerts)
│
├── 12. MULTI-TENANT & DR VIEWS
│   ├── Tenant views (top-N, VIP): held keys, lag, quotas, quarantine per tenant
│   ├── DR region dashboards with the same layers (monitoring tree F24.2)
│   └── Failover mode: dashboards switch targets via topology registry, not manual edits
│
├── 13. PANEL → RUNBOOK MAP  (OD-05; examples)
│   ├── Slot time-to-full → CDC RB-01           ├── Held T0 key aging → ordering/DLQ runbooks
│   ├── Relay task failed → CDC RB-03           ├── Rebalance storm → consumer groups RB-01
│   ├── Under-min-ISR → broker runbook          ├── Breaker open → retry RB-01
│   ├── Canary missing → monitoring RB-01       ├── Quarantine systemic → DLQ RB-02 (v3)
│   ├── Unexplained drift → monitoring RB-03    ├── Decrypt UNAVAILABLE spike → encryption RB-03
│   └── Cert expiry → encryption RB-01          └── Token/authz failures spike → authorization RB-04
│
└── 14. GAME-DAY TEST MATRIX
    ├── Kill metrics scrape for consumer service → dashboard shows UNKNOWN, alert fires, no false green
    ├── Open breaker on a dependency → partitions PAUSED with reason; triage path says "do not restart"
    ├── Poison on one T0 key → held key visible at L0; group-level lag stays normal yet incident detected
    ├── DB saturation → backpressure tiles red; autoscaler frozen; scale-out button refuses with reason
    ├── Broker under-min-ISR with URP = 0 → broker subtree flags it
    ├── Long-open producer transaction → LSO stall shown; not mistaken for consumer slowness
    ├── Systemic schema failure → consumer halted; quarantine panel shows single fingerprint; producer annotation visible
    ├── Environmental exception lands in DLQ → classifier-defect alert
    ├── Dashboard action link → C-CTL requires personal identity, reason, expiry; audit record created
    ├── Search for a customer email in logs/ops index → no results (redaction works)
    ├── New consumer group deployed → panels and alerts auto-generated
    ├── High-cardinality label introduced → CI/metrics guard rejects
    └── Region failover → dashboards follow topology; DR views populated
```

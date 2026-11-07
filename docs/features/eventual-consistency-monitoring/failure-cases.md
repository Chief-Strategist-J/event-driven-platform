# Failure Cases & Forensic Remediation Matrix: Eventual Consistency Monitoring (WP-14)

This failure catalog documents all critical failure modes across the eventual consistency monitoring lifecycle, covering detector blindness, false greens, representation divergence, heal storms, canary hazards, and incident-time degradation per the governing specification.

---

## 1. Failure Taxonomy Matrix

| Failure ID | Failure Mode | Severity | Invariant Violated | Detection Mechanism | Automated / Operational Remediation |
|---|---|---|---|---|---|
| **F16.1** | Alerting Pipeline Total Outage | OUTAGE | EC-15 | External dead-man's switch absence | Independent external monitoring service emits emergency page when invariant 60s heartbeat ceases. |
| **F16.2** | Correlated Failure Domain Dependency | OUTAGE | EC-15 | Cross-zone chaos simulation tests | Segregate canary readers and alerting egress into dedicated failure domains independent of primary Kafka/DB clusters. |
| **F16.3** | UNKNOWN Counted as MATCH (False Green) | LOSS | EC-12 | Comparator metric evaluation (`UNKNOWN` ratio) | Comparator evaluates errors/timeouts strictly to `UNKNOWN`; alert fires when `unknown_ratio > 0.05` or coverage drops. |
| **F16.4** | Sweep Execution Coverage Decay | DEGRADE | EC-13 | Sweep execution duration tracker | Alert if `last_full_sweep_completed_age > interval`; partition sweeps horizontally across keyspace workers. |
| **F16.5** | Stratified Sampling Skew / Missing Partition | DEGRADE | EC-07 | Per-partition/tenant coverage telemetry | Enforce stratified sampling distribution requiring at least 1 probe per partition, per tenant tier, per window. |
| **F16.6** | Load Shedding Biased Checks Under Peak | DEGRADE | EC-04 | Hourly coverage ratio distribution | Enqueue skipped peak-period keys into a deferred re-check priority queue; track peak coverage independently. |
| **F16.7** | Unmonitored View Registered in Cluster | DEGRADE | EC-14 | Registry reconciliation daemon | Continuous reconciler compares view registry with monitor configs; unmonitored view generates automated P1 ticket. |
| **F16.8** | Drift Tracker Memory Loss / Reset | OUTAGE | EC-06 | Sudden drop in `max_drift_age` | Persist drift state in durable PostgreSQL ledger; mark all active drift ages as `UNKNOWN` upon node restart. |
| **F17.1** | Frozen Source Replica Replication Lag | CORRUPT | EC-12 | Source replica LSN / replay timestamp probe | Probe checks replication lag before running comparison batch; if `lag > bound`, evaluations evaluate to `UNKNOWN`. |
| **F17.2** | OpenSearch Partial Shard Result Masking | CORRUPT | EC-12 | Shard failure count in query response | Reject responses with `_shards.failed > 0` as `UNKNOWN`; gate background sweeps behind OpenSearch green cluster health. |
| **F17.3** | Elastic/OpenSearch Search Replica Lag | DEGRADE | EC-08 | Read preference routing check | Enforce consistent routing preference (`_preference=_local` or primary shard) with tolerance window hysteresis. |
| **F17.4** | Store Representation & Precision Mismatch | CORRUPT | EC-11 | Canonical normalization parser | Normalize fields via canonical serializer: compare currency in integer minor units, timestamps in UTC microseconds, and lowercase UUIDs. |
| **F17.5** | XOR Checksum Cancellation on Duplicates | CORRUPT | EC-02 | Injected duplicate row test | Combine row hashes with 64-bit modular sum + exact record count + SHA-256; forbid pure XOR hashing. |
| **F17.6** | Schema Deployment Preceding Monitor Rules | DEGRADE | EC-11 | Monitor version mismatch correlator | Version comparison rules alongside view schema; monitor selects rule set based on view `schema_version`. |
| **F17.7** | False Drift from Interim Aggregate Deletion | LOW | EC-02 | Re-check against tombstone / erasure registry | Query deletion tombstone registry before promoting candidate mismatch to confirmed incident. |
| **F17.8** | Monotonic Clock Skew / NTP Jump | CORRUPT | EC-05 | Monotonic clock vs wall clock offset probe | Compute durations strictly via monotonic timers; host clock jump $> 100\text{ms}$ marks host freshness evaluations `UNKNOWN`. |
| **F17.9** | Active Blue/Green Table Swap Contention | LOW | EC-10 | View rebuild state subscriber | Monitor subscribes to cutover controller; shifts comparison target atomically during table pointer swap. |
| **F18.1** | Producer Domain Invariant Violation | CORRUPT | EC-16 | Independent domain invariant assertions | Run domain assertion rules ($\sum \text{debits} = \sum \text{credits}$) across both source and read models independently of event stream. |
| **F18.2** | Upcaster Semantic Drift (100x Unit Error) | CORRUPT | EC-16 | Value distribution anomaly detector | Monitor statistical distribution shifts across key business fields post-deployment. |
| **F19.1** | Unauthorized Direct DB Manual Modification | SECURITY | EC-17 | Database CDC audit trigger on human roles | Tag modifications originating from non-pipeline database roles with drift class `MANUAL_WRITE`; block auto-overwrite. |
| **F19.2** | Eternal Alert Silence Stagnation | DEGRADE | EC-19 | Silence registry TTL expiry scanner | Reject silences without owner; hard-expire silences after 7 days; trigger weekly unmonitored review. |
| **F19.3** | Flapping Drift Auto-Resolution Loop | DEGRADE | EC-08 | Alert flap counter and hysteresis timer | Require healthy evaluation status for sustained window ($N = 15\text{m}$) before resolving drift incident. |
| **F21.1** | Self-Healing Infinite Thrashing Loop | OUTAGE | EC-09 | Repeat-heal counter per aggregate key | Abort auto-healing and escalate to P1 incident if key undergoes $> 2$ heals within a 24-hour period. |
| **F21.2** | Heal Storm During Consumer DB Failover | OUTAGE | EC-09 | Consumer failover event bus subscription | Temporarily suspend auto-healing volume upon failover; require reconciliation sign-off before unfreezing. |
| **F21.3** | Stale Snapshot Injection During Outage | CORRUPT | EC-09 | Producer health gate & version guard | Gate heal executions on producer health; require write model snapshot response to carry version and checksum. |
| **F22.1** | Synthetic Canary Partition Blind Spot | OUTAGE | EC-18 | Partition coverage validator | Recompute synthetic canary key routing to ensure at least 1 canary key maps to every partition in topic. |
| **F22.3** | Canary Pollution in Business Reporting | COMPLIANCE | EC-18 | Downstream tenant ID filter validator | Quarantine canary keys with `tenant_id = 'canary_*'` and enforce exclusion filter across all analytics queries. |
| **F22.4** | Canary Triggering External Effect Gateway | LOSS | EC-18 | Effect gateway canary header interceptor | Route external side effects (emails, payments, webhooks) to sandbox mock providers when `is_canary = true`. |
| **F23.1** | Drift Tracker Cardinality Explosion | OUTAGE | EC-06 | Memory threshold monitor on tracker cache | Hard cap key tracking at 100,000 keys; fall back to bucketed range aggregation when limit is exceeded. |
| **F23.2** | Monitor Request Flooding Degraded Target | OUTAGE | EC-04 | Target system health & error rate probe | Shed monitoring sweeps and active probes when target latency degrades; emit `UNKNOWN` instead of retrying. |
| **F24.1** | Cross-Region Replication Lag False Alert | LOW | EC-10 | Regional topology registry subscriber | Route monitoring comparisons to same-region replicas; cross-region parity checks incorporate regional replication lag budget. |

---

## 2. Forensic Scenarios & Execution Blueprints

### Scenario 1: Three-Valued Logic Handling Under Partial Shard Outage (F17.2)
1. Sweeper initiates batch comparison for view `order_search_summary` against OpenSearch cluster.
2. OpenSearch node 3 crashes mid-sweep; query returns HTTP 200 with partial response:
   ```json
   {
     "_shards": {
       "total": 5,
       "successful": 4,
       "skipped": 0,
       "failed": 1
     }
   }
   ```
3. Naive checkers evaluate missing documents as `MISSING` and trigger false-alarm healing.
4. **Three-Valued Comparator Execution**:
   - Inspects `_shards.failed == 1`.
   - Maps entire evaluation batch to `UNKNOWN`.
   - Increments metric `consistency_evaluations_total{status="UNKNOWN", reason="PARTIAL_SHARD_FAILURE"}`.
   - Refuses to invoke auto-healing or alter drift log.
   - Triggers `CoverageDecay` warning if `UNKNOWN` ratio exceeds $5\%$.

### Scenario 2: Repeat-Heal Loop Prevention (F21.1)
1. Buggy projection code calculates order discount incorrectly on every update.
2. Monitor detects `ATTRIBUTE_MISMATCH` for `order_id = "ord_998811"`.
3. First heal executes: fetches PostgreSQL snapshot ($v=4$), rewrites OpenSearch read model ($v=4$).
4. Pipeline processes subsequent event ($v=5$) with faulty logic; view state diverges again.
5. Monitor detects `ATTRIBUTE_MISMATCH` for `ord_998811` within 2 hours.
6. **Repeat-Heal Guard Execution**:
   - Queries `consistency_drift_incident_log` for `ord_998811`.
   - Identifies 2 prior heal executions within last 24 hours.
   - Tripping threshold: aborts automated repair.
   - Transitions incident to `MANUAL_INTERVENTION_REQUIRED`.
   - P1 page dispatched with tag `REPEAT_HEAL_CIRCUIT_TRIPPED`.


## 3. Amendment 2026-11-07: Failure Modes Added

| Failure ID | Failure Mode | Severity | Trigger | Detection | Response / Operator Action |
|---|---|---|---|---|---|
| **F-ECM-A1** | Heal requested for nonexistent incident | MEDIUM | `POST .../incidents/{id}/heal` with unknown id (or wrong tenant) | `GetDriftIncident` / `UpdateDriftIncidentStatus` return `ErrIncidentNotFound` | `404`. Verify the incident id and tenant; nothing was modified. Previously a false `healed:true`. |
| **F-ECM-A2** | Heal limit reached | HIGH | `heal_attempts` >= max attempts (persisted counter now honored) | Heal returns not-healed; incident status `MANUAL_INTERVENTION` | Stop automated healing; operator investigates the drift root cause (see Scenario 2, F21.1). |
| **F-ECM-A3** | Heal status write fails | HIGH | DB error while updating incident status | Error returned (previously discarded) | Request fails with a server error instead of reporting success; retry after DB recovery. |
| **F-ECM-A4** | Canary delivery unproven | MEDIUM | `POST .../probes/canary` | Outcome `UNKNOWN` + `CANARY_PROBE` row in `consistency_check_run_history` (unknown_count 1, coverage 0) | Treat UNKNOWN as not-green (three-valued logic). The probe does not yet observe delivery; confirm arrival out-of-band until a reconciliation step exists. |
| **F-ECM-A5** | Canary record write fails | MEDIUM | DB error recording the probe | Error returned | Request fails; no false MATCH is reported. |

# Event Operations Dashboard Failure Cases and Forbidden Reflex Actions

## Forbidden Reflex Actions Matrix (Section 5)

| Forbidden Action | Common Mistake / Temptation | Underlying Failure Mode | Correct System Action (Triage Tree) |
| :--- | :--- | :--- | :--- |
| **Restarting Consumers to "Unstick" Lag** | Consumer lag growing; operator attempts container reboot. | Breaker is open or backpressure is asserted on saturated database; restart triggers rebalance storm and connection spikes. | Check partition pause reason (`PAUSED: BREAKER_OPEN` or `PAUSED: BACKPRESSURE`). Do NOT restart; resolve downstream dependency. |
| **Scaling Out During Dependency Saturation** | Throughput dipped, latency elevated; operator increases replicas. | Downstream DB/API connection pool exhausted; scale-out multiplies concurrent queries and accelerates deadlock/OOM. | Freeze autoscaler override via C-CTL; triage dependency metrics. |
| **Redriving DLQ Before Root-Cause Resolution** | DLQ quarantine queue filling up; operator executes mass redrive. | Poison schema violation or handler regression still live; records re-fail and flood DLQ retry topics. | Await producer rollback or consumer hotfix; execute C-CTL preflight before progressive redrive. |
| **Skipping Offsets to Unblock Consumer** | One partition stuck with held key; operator commits next offset. | Violates causal ordering and silent data loss occurs without audit. | Use aggregate parking store or dead-letter quarantine with causal branch tracking. |
| **Displaying Missing Data as Healthy** | Telemetry agent down or scrape network partition; metric query returns null. | Panel displays 0% error rate or 0s lag because null is coalesced to 0 (`OD-02` violation). | Absent or stale telemetry must immediately transition tile to `UNKNOWN` and trigger dead-man's switch alert. |

## Amendment 2026-11-07: Failure Modes Added

| Failure Mode | Trigger | Detection | Response | Operator Action |
| :--- | :--- | :--- | :--- | :--- |
| **Triage localize fails on healthy flow** (fixed) | `POST /api/v1/ops/triage/localize` where every hop is OK/unknown | Previously `400` from `ck_ops_incident_triage_link_btrim` (empty c_ctl link) | Now `200`, `localized_hop: NONE`, `c_ctl_action_link` = `/control-plane/consumers/status` | None. A `NONE` result means no degraded hop was found; it does not prove health if telemetry is absent (see OD-02: missing data must display as UNKNOWN, not healthy). |
| **Triage persistence regression** | A future rule returns an empty link | DB check `ck_ops_incident_triage_link_btrim` rejects the insert | Request fails | Ensure every `LocalizeBrokenHop` branch returns a non-empty link. |

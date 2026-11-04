# Consumer Groups — Failure Taxonomy & Edge Case Catalog

## 1. Overview
This catalog categorizes all failure scenarios for **Consumer Groups (WP-6)** under [Consumer Groups Decision Tree](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/consumer-groups/business-logic.md).

---

## 2. Failure Handling Matrix

| Code | Failure Scenario | System Impact | Detection Mechanism | Immediate Handling | Long-term Recovery |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **FL-CG-01** | Banned eager assignor configured | Stop-the-world rebalance freeze across entire cluster | Startup validator inspects `assignmentStrategy` | Reject startup immediately with `ErrEagerAssignorDisallowed` | Enforce `CooperativeStickyAssignor` via CI/CD manifest validation |
| **FL-CG-02** | Ephemeral pod restart triggers rebalance storm | Frequent partition migration causing high lag | Consumer connects without `group.instance.id` | Coordinator triggers full rebalance | Configure static membership `group.instance.id = ${POD_NAME}` in StatefulSets |
| **FL-CG-03** | Batch processing exceeds 80% of max poll interval | Coordinator evicts consumer assuming hung thread | Watchdog timer reaches 240s | Pause partition ingestion via `Pause()`, commit active offsets, then resume | Tune `max.poll.records` or batch concurrency |
| **FL-CG-04** | Zombie consumer thread post-revocation | Concurrent writes from two pods to same database projection | Stale generation ID in database update query | Reject write via generation epoch check; abort transaction | Rebalance epoch fencing prevents split-brain writes |
| **FL-CG-05** | Heartbeat thread starvation (>45s) | Pod evicted from consumer group by broker | `now - heartbeat_at > 45000ms` | Transition member to `DEAD` in ledger; trigger watchdog exit | Separate heartbeat thread from processing thread |

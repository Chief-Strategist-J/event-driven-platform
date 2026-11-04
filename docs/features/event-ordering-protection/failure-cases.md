# Event Ordering Protection — Failure Modes & Edge Cases Taxonomy

## 1. Overview
This document categorizes all failure modes, boundary edge cases, and recovery behaviors for **Event Ordering Protection (WP-4)** in compliance with [Event Ordering Protection Decision Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/event-ordering-protection/business-logic.md).

---

## 2. Failure Handling Matrix

| Code | Failure Scenario | System Impact | Detection Mechanism | Immediate Handling | Long-term Recovery |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **FL-ORD-01** | Duplicate event with matching payload hash | Benign network redelivery or retry | `incoming.version <= last_applied && SHA256(payload) == stored_hash` | Acknowledge and drop. Advance Kafka offset. | Zero intervention required. |
| **FL-ORD-02** | Duplicate event with conflicting payload hash | Code defect in upstream producer or payload tampering | `incoming.version <= last_applied && SHA256(payload) != stored_hash` | Transition aggregate to `is_held = true, hold_reason = 'CHECKSUM_MISMATCH'`. Route to DLQ. | Alert Security Guild. Forensic inspection of producer outbox mutation history. |
| **FL-ORD-03** | Future event arrives ahead of sequence ($V_n > V_{curr} + 1$) | Relay multi-worker race or temporary message delay | `incoming.version > last_applied + 1` | Insert into `sequence_held_event`. Set `is_held = true, hold_reason = 'GAP'`. Advance Kafka offset. | Wait for delayed predecessor or trigger re-emit on TTL expiry. |
| **FL-ORD-04** | Missing predecessor never arrives (TTL Expired) | Message permanently dropped at source or broker loss | Scheduled cron or sweep worker detects `expires_at < NOW()` | Emit high-severity page. Increment `event_ordering_missing_predecessor_total`. | Invoke Producer Re-emit API for missing versions. If unavailable, trigger snapshot resync. |
| **FL-ORD-05** | Zombie consumer post-rebalance | Stale worker commits state out-of-order after partition revoked | Database conditional update: `WHERE owner_epoch <= incoming_epoch` returns 0 rows | Rollback transaction. Stale consumer exits thread. | Fenced completely by database row update. New consumer epoch takes precedence. |
| **FL-ORD-06** | Consumer crash during cascade drain | Partial application of held queue | Process crash or container SIGKILL during drain loop | Database transaction rollback preserves uncommitted held events in storage. | When consumer restarts or rebalances, next worker re-fetches held events and resumes drain. |
| **FL-ORD-07** | Non-sequential held buffer ($V=3, V=5$ held, $V=2$ arrives) | Cascading gaps across multiple messages | Predecessor arrives ($V=2$), drains $V=3$, but $V=4$ is still missing | Drain $V=2 \to V=3$. Detect $V=4$ is missing. Re-park aggregate at $V=3$ with reason `GAP`. | Wait for $V=4$ or trigger re-emit. |
| **FL-ORD-08** | Database sequence object used at producer | Rollbacks burn sequence numbers creating permanent ghost gaps | Upstream database generates gap that never existed in business intent | Consumer parks aggregate on gap. Re-emit API reports version was never written to outbox. | Enforce [OR-01]: producers must use conditional row updates on aggregate, never DB sequence objects. |
| **FL-ORD-09** | First-seen event version is greater than 1 ($V > 1$ on clean DB) | Consumer deployed on pre-existing stream without snapshot | `tracker == nil && incoming.version > 1` | Mark tracker `is_held = true, hold_reason = 'GAP_FROM_BOOTSTRAP'`. | Invoke Producer Snapshot API at version $V-1$ to bootstrap aggregate state. |
| **FL-ORD-10** | Massive held buffer overload (Denial of Service) | Out-of-order burst exhausts storage gate disk | Count of held events for single aggregate exceeds buffer limit (e.g. $> 100$) | Reject additional held events. Mark aggregate `BUFFER_OVERFLOW` and route excess to cold storage DLQ. | Throttling producer stream or investigating burst packet loss. |

---

## 3. Detailed Edge Case Analysis & Invariants

### 3.1 Ghost Sequence Gaps (F3.2)
* **The Trap**: Using PostgreSQL `CREATE SEQUENCE` or MySQL `AUTO_INCREMENT` to generate event sequence numbers. When a transaction rolls back, the allocated sequence number is lost forever.
* **The Consequence**: Downstream consumers will observe a sequence gap (e.g. $V=1 \to V=3$), park the aggregate, and wait indefinitely for $V=2$. The producer re-emit API cannot fulfill the request because $V=2$ was never created.
* **Invariant Enforcement (OR-01)**: The version number MUST reside on the business aggregate record itself. Increments occur via:
  ```sql
  UPDATE aggregate_store 
  SET version = version + 1 
  WHERE id = $id AND version = $expected_version 
  RETURNING version;
  ```
  If 0 rows are updated, the command aborts and rolls back without burning any version number.

### 3.2 Non-Contiguous Cascading Drain (FL-ORD-07)
* **Scenario**:
  1. Aggregate starts at $V=1$.
  2. Events $V=3$ and $V=5$ arrive and are parked in `sequence_held_event`.
  3. Aggregate tracker is at $V=1, \text{is\_held} = \text{true}$.
  4. Missing event $V=2$ arrives.
* **Execution Flow**:
  1. Consumer acquires row lock on `aggregate_sequence_tracker`.
  2. $V=2$ is exact-next ($1 + 1$). It applies to business state, advancing tracker to $V=2$.
  3. Drain query retrieves held events: $[V=3, V=5]$.
  4. $V=3$ is evaluated: $3 == 2 + 1 \implies$ APPLIED. Held record $V=3$ deleted. Tracker advances to $V=3$.
  5. $V=5$ is evaluated: $5 \ne 3 + 1$ (missing $V=4$).
  6. **Hoare Invariant Guard**: The drain loop MUST terminate immediately.
  7. Tracker remains `is_held = true, hold_reason = 'GAP'`.
  8. Partition commits offset. Processing continues.

### 3.3 Epoch Fencing Against Split-Brain (FL-ORD-05)
* **Scenario**: Consumer $A$ reads partition 0 at epoch 1. Long GC pause occurs ($> 30$ seconds).
* **Kafka Reaction**: Consumer Group coordinator marks Consumer $A$ dead, initiates rebalance, assigns partition 0 to Consumer $B$ at epoch 2.
* **Race Condition**: Consumer $B$ processes message $M_{10}$ and updates tracker with `owner_epoch = 2`. Consumer $A$ wakes up from GC and attempts to commit out-of-date calculation for $M_9$ with `owner_epoch = 1`.
* **Database Resolution**:
  ```sql
  UPDATE aggregate_sequence_tracker
  SET last_applied_version = $new_v, owner_epoch = $worker_epoch
  WHERE aggregate_id = $agg_id AND owner_epoch <= $worker_epoch;
  ```
  The update modifies 0 rows because $2 > 1$. The worker checks `RowsAffected()`: if 0, it aborts the transaction immediately, preventing state corruption.


# Idempotent Consumer Failure Cases and Edge Analysis

Governed by:
- `llm-obs-policies/rules/database/problems/concurrent-state-changes-and-effect-integrity.md`
- `llm-obs-policies/rules/database/problems/change-safety.md`
- `llm-obs-policies/rules/folder-structure/05-db-writing-rules.md`

---

## 1. Matrix of Failure Cases

| Case ID | Failure Scenario | Trigger Condition | Consequence if Unhandled | Defensive Mechanism & Invariant |
|---------|------------------|-------------------|--------------------------|----------------------------------|
| **FC-IC-01** | Transport Redelivery (Phantom Duplicate) | Broker rebalance or network timeout triggers replay of already acknowledged event. | Duplicate business mutation (e.g. double debit). | **Layer 1 Inbox Deduplication (`consumer_inbox`)**: Atomic upsert with `ON CONFLICT (tenant_id, handler_id, event_id) DO NOTHING`. Returns `rows_affected == 0`, short-circuiting handler dispatch. |
| **FC-IC-02** | Upstream Event Loss / Version Gap | Producer fails to publish event $V=3$ or out-of-order rebalance delivers $V=4$ first on `DELTA` topic. | Corrupted state calculation due to missing intermediate differential. | **Layer 2 Version Gate (`EvaluateVersionRule`)**: If $V > V_{\text{last}} + 1$ under semantic class `DELTA`, flags `ActionHoldGap`. Key processing is parked until $V=3$ is reconciled. |
| **FC-IC-03** | Split-Brain Collision (Poison Pill) | Producer restores from old backup or dual-leader bug produces same version $V=2$ with different payload hash. | Silent state divergence across replicas or downstream projections. | **Content Hash Comparison (`IsContentHashConsistent`)**: If $V == V_{\text{last}}$ and `SHA-256(payload) != last_payload_hash`, immediately update `is_parked = TRUE` and halt aggregate processing. |
| **FC-IC-04** | Zombie Consumer Overwrite | Consumer instance stalls (GC pause / network partition), consumer group reassigns partition, new consumer processes $V=3$, old consumer awakens and attempts to apply $V=2$. | Lost update: newer state overwritten by stale zombie worker. | **Optimistic / Guarded DB Update**: `FlowUpsertAggregateProjection` includes `WHERE consumer_projections.last_version < $4`. The stale write affects 0 rows and aborts. |
| **FC-IC-05** | Two-Phase Drift (DB Commit vs Broker Ack) | Database transaction commits projection and inbox record, but consumer process crashes before committing Kafka offset. | Broker redelivers event on restart. | **Idempotent Storage Re-entry**: Redelivered event hits `consumer_inbox` conflict target, succeeds cleanly with `ActionSkip`, and advances Kafka offset safely. |
| **FC-IC-06** | Unnormalized Key Bypass | Event contains leading/trailing whitespace in identifiers (e.g., `"ord_100 "` vs `"ord_100"`). | Bypasses unique constraints, resulting in duplicate records and projection corruption. | **Storage Gate Trimming Constraints**: Database enforces `CHECK (col = btrim(col) AND col <> '')` on all tenant, handler, and aggregate identifiers. |

---

## 2. Distributed Failure Details & Invariants

### FC-IC-03: Split-Brain Poison Pill Handling
When a split-brain collision occurs:
1. `consumer_projections.is_parked` is set to `TRUE`.
2. The consumer service halts mutation for that specific `(tenant_id, aggregate_id)`.
3. An alert is emitted to the operations dashboard with correlation and event IDs.
4. The message is routed to DLQ with reason `split-brain content collision`.

### FC-IC-05: Atomic Effect Integrity (IC-01)
As mandated by Category 1.4 and 1.6:
- The business state change, inbox ledger insertion, and projection version update must execute in **the exact same database transaction**.
- Never perform non-transactional network I/O inside the database transaction boundary.


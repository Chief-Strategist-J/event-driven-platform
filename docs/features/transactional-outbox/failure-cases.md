# Transactional Outbox: Failure Cases and Edge-Case Mitigations

## 1. Deep Edge-Case Analysis

### F1.2 Ambiguous Commit (Connection drops during COMMIT)
- **Scenario**: The client issues an order placement command. The database transaction completes the aggregate mutation and outbox insert, but the TCP connection drops during the final `COMMIT` packet. The client receives a connection reset error.
- **Impact**: The client does not know whether the order succeeded. Naively retrying could charge the credit card twice and create two distinct aggregates.
- **Handling**: Step 1 of the write path executes an idempotency claim in `outbox_idempotency_keys` (`INSERT INTO outbox_idempotency_keys (tenant_id, idempotency_key, record_id) ... ON CONFLICT DO NOTHING`). When the client retries with the same `x-idempotency-key`, the query returns the already generated `record_id`, returning the existing `OutboxRecord` without re-executing business logic or creating duplicate outbox records.
- **Detection**: Spike in idempotency conflict metrics indicates client-to-ingress network degradation.

### F1.5 Concurrent Mutations on Same Aggregate
- **Scenario**: Two concurrent HTTP requests attempt to modify the same aggregate (e.g., adding an item and changing the shipping address simultaneously).
- **Impact**: Both could read version $k$, compute version $k+1$, and attempt to insert conflicting outbox records.
- **Handling**: The database table enforces `CONSTRAINT outbox_aggregate_version_uk UNIQUE (tenant_id, aggregate_type, aggregate_id, aggregate_version)`. The second transaction immediately aborts with a unique violation, which the domain service maps to a `ConflictError` (HTTP 409), requiring the client to reload the latest version and retry.

### F1.7 Oversized Payload (Exceeds Kafka Broker Cap)
- **Scenario**: A user uploads an invoice containing embedded base64 image data exceeding 1MB.
- **Impact**: The database accepts the row, but when the relay attempts to publish to Kafka, the broker rejects it (`RecordTooLargeException`). The event becomes a permanent poison pill, stalling the aggregate key indefinitely.
- **Handling**: `rules.IsPayloadSizeValid` rejects payloads $> 1\text{MB}$ at ingress before database persistence. Business transactions fail immediately with a 400 Validation Error, requiring large blobs to use a claim-check pattern (uploading to object storage and passing the URI reference).

### F1.9 Floating-Point Precision Loss
- **Scenario**: An event payload contains numeric financial data serialized as a JSON float (e.g. `{"price": 19.99}`).
- **Impact**: Downstream consumer runtimes deserialize the number into IEEE 754 `float64`, causing cent rounding errors and balance drift across ledgers.
- **Handling**: `rules.HasFloatingPointNumbers` uses a tokenizing JSON decoder (`UseNumber`) to inspect all numeric values. If any number contains a decimal point (`.`) or scientific notation (`e`, `E`), the record is rejected at ingress with `ValidationRuleNoFloats`. Money must be passed as integer minor units (e.g. cents) or decimal strings.

### F4.0 Row Locks Held During Network Dispatch
- **Scenario**: Poller executes `SELECT ... FOR UPDATE` and holds the transaction open while calling Kafka `Publish()`.
- **Impact**: If Kafka experiences latency or a broker leader rebalance, the database row lock remains open for seconds or minutes. PostgreSQL vacuum cannot advance past the transaction horizon, causing database-wide table bloat and connection exhaustion.
- **Handling**: Lease model. Poller executes a short database transaction to claim records into `status = 'IN_FLIGHT'` with a `worker_id` and a 30s `lease_expires_at`, commits immediately, and closes the database transaction. Kafka dispatch occurs with 0 open database transactions. Once acknowledged, a second short transaction marks the records `PUBLISHED`.

### F4.3 Commit-Order Visibility Gap
- **Scenario**: A transaction started at 10:00:00 inserts an outbox record with an earlier ID/timestamp, but takes 500ms to commit. Meanwhile, a poller runs at 10:00:01 and reads records based on `WHERE id > last_seen_id` or `created_at > last_watermark`.
- **Impact**: The earlier transaction commits after the poller has already advanced its cursor. The event is skipped and never published (silent loss).
- **Handling**: Poller queries never rely on incremental watermarks or `>= timestamp`. Poller selects strictly by `status = 'PENDING' OR (status = 'IN_FLIGHT' AND lease_expires_at < NOW())`. Any transaction that commits with `PENDING` will be picked up on the next polling cycle regardless of its creation timestamp.

### F4.6 Worker Lease Expiry & Fencing
- **Scenario**: Worker A claims a batch with a 30s lease. Worker A encounters a 35s garbage collection pause. Worker B notices the lease expired, reclaims the batch, and starts publishing. Worker A wakes up and attempts to update the database to `PUBLISHED`.
- **Impact**: Stale worker overwrites the status, causing inconsistent retry counters or double-processing.
- **Handling**: `MarkPublishedByWorker` executes `UPDATE outbox SET status = 'PUBLISHED' WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 AND (lease_expires_at IS NULL OR lease_expires_at >= NOW())`. If Worker A's lease expired or Worker B reclaimed the record (`worker_id != Worker A`), 0 rows are updated, and Worker A receives `ErrWorkerOwnershipLost`.

### F4.7 Poison Event Aggregate Parking
- **Scenario**: A malformed event passes validation but fails schema registry validation at the broker or trips a broker constraint.
- **Impact**: The relay retries the record endlessly. If the relay skips it and moves to $k+1$, per-aggregate ordering (INV-03) is permanently broken.
- **Handling**: After 5 failed publish attempts (`retry_count >= 5`), the record status is set to `PARKED`. Poller queries include `NOT EXISTS (SELECT 1 FROM outbox p WHERE p.aggregate_id = o.aggregate_id AND p.status = 'PARKED')`. This completely halts publication of versions $> k$ for that specific aggregate, isolating the poison pill while allowing all other aggregates to proceed normally.

### F4.11 Cleanup Deletes Unpublished Records
- **Scenario**: A scheduled cleanup job runs to prune old records.
- **Impact**: If cleanup logic is based purely on `created_at < NOW() - INTERVAL '48 hours'`, slow-moving or parked aggregates could be permanently deleted before being published.
- **Handling**: `FlowPurgeExpiredPublishedRecords` explicitly filters `WHERE status = 'PUBLISHED' AND created_at < NOW() - INTERVAL '48 hours'`. Records in `PENDING`, `IN_FLIGHT`, or `PARKED` states are never eligible for cleanup.

---

## 2. Comprehensive Failure Reference Matrix

| Code | Failure Scenario | Impact | Code Mitigation | Live Detection |
|---|---|---|---|---|
| **F1.1** | Dual-write attempt | Event loss or ghost event | Zero Kafka imports in business services; mandatory outbox insertion | CI dependency scanner |
| **F1.2** | Ambiguous commit | Duplicate aggregate & event | `outbox_idempotency_keys` table claims key first; retries return existing record | Idempotency conflict rate |
| **F1.5** | Concurrent aggregate write | Version collision | `UNIQUE (tenant_id, aggregate_type, aggregate_id, aggregate_version)` | Unique constraint violation metric |
| **F1.7** | Oversized payload | Broker rejection & partition block | Hard write-time check $\le 1\text{MB}$ in `rules.IsPayloadSizeValid` | Payload size histogram |
| **F1.9** | Float in JSON contract | Financial cents drift | `rules.HasFloatingPointNumbers` rejects floats in JSON numbers | Contract validation error counter |
| **F1.14** | Null key or aggregate ID | Skipping / unroutable event | `rules.ValidateOutboxInput` enforces non-empty strings on all identifiers | Validation failure metric |
| **F2.1** | Kafka down / broker halt | Postgres disk bloat | Records stay safely `PENDING`; backpressure monitored | Slot lag bytes & WAL free disk % |
| **F4.0** | Row lock held over network | DB connection exhaustion | Lease model: records claimed as `IN_FLIGHT`, send without open DB tx | Oldest active transaction age |
| **F4.2** | Multiple workers reorder versions | Downstream corruption | Batch ordered by `aggregate_version ASC`; hash partition affinity on `aggregate_id` | Consumer gap detector |
| **F4.3** | Visibility gap on commit | Skipped / lost event | Query by status (`PENDING` / expired `IN_FLIGHT`), never cursor/watermark | Oldest PENDING age metric |
| **F4.6** | Worker lease expires mid-send | Overwrite / race condition | `MarkPublishedByWorker` fences updates on worker ownership & active lease | Reclaim counter spike |
| **F4.7** | Poison event | Partition outage | Retry count $\ge 5 \to \text{PARKED}$; queries block subsequent versions $k+1$ | Alert when `count(status='PARKED') > 0` |
| **F4.11** | Retention purge deletes pending | Silent data loss | Purge query strictly filters `WHERE status = 'PUBLISHED'` | Purge count vs published count |
| **F9.1** | Silent pipeline stoppage | Undetected loss | Synthetic canary emitted every ~30s (`EmitCanaryHeartbeat`) | Canary consumer latency SLO |

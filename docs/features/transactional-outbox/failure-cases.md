# Transactional Outbox: Failure Cases and Mitigations

| Failure Code | Scenario | Impact | Code Handling & Architectural Guardrail | Detection |
|---|---|---|---|---|
| **F1.1** | Dual-write attempt | Event Loss / Ghost State | Zero Kafka clients imported in domain feature; all writes go to `outbox` | CI dependency scanner |
| **F1.2** | Ambiguous commit / network drop | Duplicate aggregate & event | Step 1 claims `x-idempotency-key` in `outbox_idempotency_keys` table; retry returns existing record | Unique violation counter |
| **F1.5** | Concurrent mutations on same aggregate | Race condition / version collision | `UNIQUE(tenant_id, aggregate_type, aggregate_id, aggregate_version)` constraint | SQL Unique Violation (mapped to `ConflictError`) |
| **F1.7** | Oversized payload | Broker partition block / poison pill | Write-time rejection of payloads > 1MB (`IsPayloadSizeValid`); requires claim-check | Payload size histogram |
| **F1.9** | Floating-point numbers in payload | Financial precision loss & cents drift | `HasFloatingPointNumbers` scans payload and rejects floats (`.`, `e`, `E`) | Validation error counter |
| **F2.1** | Kafka down / Relay down | PostgreSQL WAL bloat | Outbox rows accumulate safely as `PENDING`; backpressure monitored | Slot lag bytes & WAL disk alerts |
| **F4.0** | Row locks held during network send | Lock pileup & DB outage | Lease model: records claimed as `IN_FLIGHT` with worker ID and lease expiry | Oldest transaction age |
| **F4.2** | Concurrent workers reorder versions | Downstream corruption | `ORDER BY aggregate_version ASC` + partition key affinity on `aggregate_id` | Consumer gap detector |
| **F4.6** | Worker lease expires during dispatch | Duplicates & race collision | `MarkPublishedByWorker` fences confirm step on worker ownership & active lease | Reclaim counter |
| **F4.7** | Poison event payload | Outage on partition | Transitions to `PARKED` after 5 retries; query suppresses versions $> k$ | Alert when `count(status='PARKED') > 0` |
| **F9.1** | Silent flow stoppage | Undetected outage | Synthetic canary heartbeat emitted every ~30s (`EmitCanaryHeartbeat`) | Canary consumer latency |

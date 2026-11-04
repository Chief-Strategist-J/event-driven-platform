# Retry Topics — Failure Taxonomy & Edge Case Catalog

## 1. Overview
This catalog enumerates all edge cases and failure modes for **Retry Topics (WP-5)** pursuant to [Retry Handling Business Logic Decision Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/retry-topics/business-logic.md).

---

## 2. Failure Handling Matrix

| Scenario Code | Root Cause | Impact | Detection | Handling Strategy | Invariant / Policy |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **FL-RT-01** | Environmental failure (DB failover, network down) | Downstream dependency unavailable | 503, connection refused, broker timeout | Trip circuit breaker, pause consumer partition; do NOT consume retry attempt budget | **RT-02** |
| **FL-RT-02** | Message retry on strict ordered stream routed to retry topic | Out-of-order execution corrupts aggregate sequence | Stream declared as `O-STRICT` | Strictly forbid retry topic routing. Persist in `retry_attempt_ledger`, park key, advance offset | **RT-01** |
| **FL-RT-03** | Attempt due time exceeds event validity deadline | Outdated business event executed | `due_at > valid_until` | Mark attempt `EXPIRED` in ledger; do not re-execute; alert metrics | **RT-07** |
| **FL-RT-04** | Transient failure reaches max attempts | Downstream system cannot recover in time | `attempt_number >= max_attempts` | Mark attempt `EXHAUSTED` in ledger; route event to Dead Letter Quarantine | **RT-06** |
| **FL-RT-05** | Permanent schema or domain rejection | Irrecoverable data invalidity | Validation error or typed business rejection | Route immediately to DLQ; never schedule retries | **RT-01, RT-06** |
| **FL-RT-06** | Side-effect executed during retry attempt | Duplicate charges or state corruption on target | Retry execution of non-idempotent action | Deterministic idempotency key: $\text{SHA-256}(\text{handler} \mathbin{\Vert} \text{event} \mathbin{\Vert} \text{effect})$ | **RT-08** |
| **FL-RT-07** | Thundering herd on retry execution | Synchronous retry attempts overwhelm downstream | Burst of failed events with identical due times | Cryptographic jitter: $\pm 20\%$ entropy added to exponential backoff | **RT-11** |


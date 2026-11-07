# Dead-Letter Handling Failure Cases & Edge Matrix

Governed by:
- `event-driven-platform/docs/features/dead-letter-handling/business-logic.md` (v2)
- `llm-obs-policies/rules/database/problems/change-safety.md`
- `llm-obs-policies/rules/folder-structure/05-db-writing-rules.md`

---

## 1. Matrix of Failure Cases

| Failure ID | Scenario | Trigger Condition | Severity | Defensive Handling |
|---|---|---|---|---|
| **F2.1** | DLQ Publish Fails | Storage/broker down when saving quarantine record | **LOSS** | Source offset must NOT advance; stop consumption partition; alert immediately (DL-05). |
| **F2.2** | Envelope Exceeds Size | Diagnostic headers + large payload exceeds limits | **OUTAGE/LOSS** | Store stack traces in dedicated error logs, preserving original payload bytes lossless in quarantine. |
| **F3.1** | Key Unparking Race | Redrive succeeds while new live version arrives concurrently | **CORRUPT** | Layer 2 version guard in consumer rejects out-of-order writes; unparking only clears the lock, not version order. |
| **F3.2** | Bounded Redrive Loop | Handler has persistent bug; redrives continually fail | **OUTAGE** | Hard stop at `max_redrive_attempts = 3` (DL-11); forces human operator triage before any further attempts. |
| **F4.1** | System Outage Sent to DLQ | Database blip or network drop misclassified as poison message | **DEGRADE** | Invariant DL-08 rejects non-defect errors (`TIMEOUT`, `503`); pauses partition instead of quarantining. |
| **F5.1** | Silent Event Loss / Expiry | Quarantined message neglected until TTL elapses | **COMPLIANCE** | Invariant DL-01 mandates explicit disposition: `REDRIVEN_SUCCESS`, `COMPENSATED`, or `DISCARDED`. |


## 2. Matrix of Failure Cases (Amendment 2026-11-07: API ingest handling)

| Failure ID | Scenario | Trigger Condition | Severity | Defensive Handling |
|---|---|---|---|---|
| **F2.3** | Quarantine request without `headers` | Client omits `headers`; nil map marshals to JSON `null`, violating `dlq_headers_is_object_ck`. | **LOSS** (event not quarantined, HTTP 500) | Handler and Postgres adapter default headers to `{}`. Operator: none needed; if 500 persists check DB constraint migration state. |
| **F2.4** | Non-UUID `eventId` or `correlationId` | Client sends free-text ids. | **DEGRADE** | `ValidateQuarantineInput` returns 400 with per-field detail. Operator: fix producer to send UUIDs. |
| **F2.5** | Duplicate `externalId` on quarantine | Producer retry reuses `externalId`. | **DEGRADE** | Unique violation mapped to 409 `CONFLICT`; handlers use `errors.As` so wrapped errors keep their status. Operator: treat 409 as already quarantined. |
| **F2.6** | List quarantined without `tenantId` | Missing query parameter. | **DEGRADE** | 400 `tenantId query parameter is required`. |
| **F2.7** | Redrive/discard with non-UUID `quarantineId` | Client passes a non-UUID id. | **DEGRADE** | **Not handled**: returns 500 (known open gap). Operator: pass the UUID `id` from the quarantine record. |

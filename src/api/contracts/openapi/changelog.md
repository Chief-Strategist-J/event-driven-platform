# OpenAPI Changelog

## 2026-11-07 - Error semantics, validation and persistence honesty

See `docs/adr/ADR-0002-api-error-semantics-validation-and-persistence-honesty.md`. "BREAKING" marks changes a correct existing client may have to handle.

### New routes (non-breaking)
* `POST /api/v1/schemas/upcasters` - register an upcaster (built-in `identity` transformer for additive bumps; duplicate `externalId` returns 409).
* `GET /api/v1/offsets/lag` - evaluate consumer lag (`tenant_id` query or `X-Tenant-Id`, `group_id`, `topic`, optional `partition`, `rate` default 1).

### Status code changes
* `POST /api/v1/outbox`: duplicate `externalId` without idempotency key 500 -> 409. BREAKING.
* `POST /api/v1/communication/commands/dispatch`: invalid input 400, duplicate `external_id` 409. `command_payload` must be a JSON string. BREAKING.
* `POST /api/v1/consistency/incidents/{id}/heal` (nonexistent incident): 200 `healed:true` -> 404. BREAKING.
* `POST /api/v1/ops/triage/localize` (healthy/unknown flow): 400 -> 200. BREAKING for clients that treated 400 as expected.
* `POST /api/v1/tracing/coverage/report`: invalid reports (missing fields, bad `hop_type`, negative counts, propagated > total) 200 -> 400. BREAKING.
* `POST /api/v1/schemas/register`: incompatible schema, duplicate fingerprint or non-monotonic version -> 409 (previously accepted); unknown compatibility mode -> 400. BREAKING.
* `POST /api/v1/offsets/rewind`: forward jump or target beyond log end -> 400; no stored offset -> 404 (previously accepted). BREAKING.
* `POST /offsets/commit`: invalid ids, partition, offset or non-UUID `correlationId` -> clean 400 (previously raw database error).
* `POST /tenants` invalid tier 500 -> 400; `PUT` tenant lifecycle unknown tenant 500 -> 404; unknown `target_state` 409 -> 400. BREAKING.
* Materialized views / partitioning: duplicate view, topology or rebuild 400 -> 409. `POST /materialized-views/query` unknown view 200 (version 0) -> 404; missing tenant or view -> 400. BREAKING.
* `POST /replay/runs/advance`: run in `INITIALIZING` -> 409; non-UUID `runId` -> 400 (use `id`, not `publicId` `rpl_*`); unknown run -> 404. BREAKING.
* CDC: duplicate `slot_name` or connector 500 -> 409. `GET /cdc/connectors/{name}` always-404 -> 200 for existing connectors.
* `POST /dlq/quarantine`: missing headers 500 -> accepted (headers default `{}`); non-UUID `eventId`/`correlationId` 500 -> 400; duplicate `externalId` 500 -> 409. `GET /dlq/quarantined` without `tenantId` 200 `data:null` -> 400. BREAKING.
* Backpressure: non-UUID `correlationId` -> clean 400; unknown record 404; signal not saturated 400; invalid transition 409.
* `POST /consumers/inbox`: non-UUID `eventId` 500 -> 400; unknown `semanticClass` (previously silently `DELTA`) -> 400; empty still defaults to `DELTA`. BREAKING.
* `GET /queries/resolve`: invalid `x-consistency-token` (previously silently `IMMEDIATE`) -> 400. BREAKING.
* `GET /api/v1/consumer-groups/members` and `GET /api/v1/dlq/quarantined` without the tenant parameter: 200 `data:null` -> 400. BREAKING. (`GET /api/v1/audits/query` is unchanged: it defaults a missing `tenant_id` to `default` and requires `shard_id`.)
* Database integrity errors without a specific mapping now return the message `request conflicts with existing data or violates a data integrity rule`; constraint names are no longer included.

### Field and response changes
* `commands/dispatch` status `DISPATCHED_TO_OUTBOX` -> `QUEUED_IN_OUTBOX` (an outbox row is now persisted). BREAKING for clients matching the old string.
* `POST /commands/keys`: `raw_secret_value` is no longer returned. `secret_key_hash` is still returned. BREAKING.
* Encryption decrypt failures (UNAUTHORIZED / TAMPERED / MALFORMED): envelope `success` is now `false` (was `true` alongside HTTP 403/400). BREAKING for clients keying on `success`.
* `POST /replay/effects/evaluate`: `source_service`, `external_id`, `correlation_id` are defaulted; a supplied `correlation_id` must be a UUID (400 otherwise).
* Replay run identifiers: `runId` must be a UUID.
* `POST /api/v1/domain-events/envelope/build` with `transaction_group_id`: `group_size` is now required (> 0); persistence failures return a generic 500. BREAKING.
* Canary probe (`.../probes/canary`): outcome is now `UNKNOWN` (was always `MATCH`) and a `CANARY_PROBE` history row is recorded. BREAKING for clients expecting `MATCH`.
* Backpressure `ConfirmRecovery` restores the concurrency limit to the baseline 10.
* Outbox relay publishes Kafka headers `event_type`, `schema_version`, `source_service` plus custom headers (not an HTTP contract change).

## 1.2.0 (second pass, 2026-11-07)
* `POST /api/v1/dlq/redrive`, `POST /api/v1/dlq/discard`: non-UUID `quarantineId` 500 -> 400. BREAKING (status only).
* `GET /api/v1/retries/due`, `GET /api/v1/events/held`, `GET /api/v1/consistency/views/{view}/watermark|incidents|sweeps`, `GET /api/v1/queries/resolve`, `GET /api/v1/queries/authoritative-own`, `GET /api/v1/audits/query`: missing `tenant_id` 200 `data:null` (or silent `default` tenant) -> 400. BREAKING.
* `GET /api/v1/queries/resolve` with a token and no active signing key for the tenant: -> 409 CONFLICT. `POST /api/v1/commands/dispatch` returns an empty `ConsistencyToken` when the tenant has no active key. `POST /api/v1/commands/keys` no longer returns `secret_key_hash`. BREAKING.
* `POST /api/v1/replay/runs`: non-UUID `correlationId` / `X-Correlation-ID` -> 400. BREAKING.
* `POST /api/v1/tenants/verify-envelope`: SUSPENDED tenant -> 403. BREAKING.
* Spec: added `servers` (Envoy gateway, direct port), the admin-listener health endpoints `GET /health/live` and `GET /health/ready`, and a runtime/deployment section in `info.description` (Docker Hub image, ports, `DATABASE_URL`, outbox topic prerequisite, no `/metrics`).

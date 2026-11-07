# ADR-0002: API Error Semantics, Input Validation and Persistence Honesty

* **Status**: Accepted
* **Date**: 2026-11-07
* **Deciders**: Platform Architecture Core Team
* **Technical Domain**: REST API, Error Handling, Persistence, Test Strategy

---

## Context and Problem Statement

How do we make every REST route return a status code that matches what actually happened, reject client mistakes before they reach PostgreSQL, and never report success for work that was not persisted?

### Incident

An end-to-end API test of roughly 90 routes was run against the live stack (PostgreSQL, Kafka, Apicurio). It found about 14 real defects and several status-code and validation inconsistencies. Representative symptoms:

* Client mistakes returned 500 (duplicate outbox `externalId`, bad tenant tier, non-UUID `eventId`, missing headers on DLQ quarantine) or leaked raw `pq:` text and constraint names (backpressure, materialized views, CDC, audit trail).
* Real 404s and 409s were reported as 500 or 400 (heal of a nonexistent incident, rewind of an unknown partition, unknown tenant on lifecycle update).
* Routes returned success with nothing persisted: `commands/dispatch` answered `DISPATCHED_TO_OUTBOX` with no outbox row; `envelope/build` answered 200 with no transaction group row; `replay/effects/evaluate` answered `SUPPRESSED` with no log row; canary probes always reported `MATCH`.
* Contract violations: encryption failures returned HTTP 403/400 with an envelope saying `success:true`; `POST /commands/keys` echoed `raw_secret_value`; list endpoints answered `200` with `data:null` when the tenant scope was missing.
* Routes that could never work: `POST /api/v1/schemas/upcast` (no way to register an upcaster), `GET /cdc/connectors/{name}` (always 404), `GET /api/v1/offsets/lag` (not registered).

---

## Root-Cause Analysis

The causes below are **inferred from code evidence, not from commit history or interviews**. The history of why the code looked this way is unknown.

| ID | Root cause | Evidence in code | Effect |
| :--- | :--- | :--- | :--- |
| RC1 | Handlers mapped service errors to HTTP status by substring match on the message (`"validation"`, `"blank"`, `"conflict"`, `"duplicate"`) or by a bare type assertion `err.(*ApplicationError)`. Services wrap errors with `%w`, so the assertion never matched. | Handler error-mapping code before the fix; the shared helper `src/api/rest/v1/handlers/service_error.go` (`classifyServiceError`) now uses `errors.As`. | Real 404/409/400 became 500 or 400. |
| RC2 | Validation was delegated to PostgreSQL CHECK, UUID-cast and UNIQUE constraints. | Raw `pq:` messages and constraint names reached clients; `src/infra/database/error_mapper.go` class-23 fallback used to include the constraint name. | 500s and information leaks for client mistakes. |
| RC3 | Errors were discarded with `_ =` / `_, _ =` in services (tracing coverage upsert, envelope build, replay effect log). | Fixed by returning or logging the error. | HTTP 200 with nothing persisted. |
| RC4 | Scaffolded stubs returned success without side effects (commands dispatch, canary probe, CDC mock adapter). | Dispatch now inserts an outbox row; canary records a row with outcome `UNKNOWN`. | Callers believed work had been done. |
| RC5 | Features were built independently (camelCase vs snake_case, header vs body tenant, differing error styles). Tests ran against in-memory adapters; there was no HTTP-level or Postgres-backed conformance suite. Integration tests that need Postgres skip silently when the DSN is absent. | Integration tests read `DATABASE_URL` / `TEST_DATABASE_URL` and skip if unset. | Integration-only defects (SQL parameter typing, CHECK constraints, wrapped errors) were never exercised. |

RC6 and RC7 (operational prerequisites) are not code defects and are covered in ADR-0003.

---

## Decision Drivers

* Clients must be able to rely on the status code and the `success` flag without parsing messages.
* Database internals (constraint names, `pq:` text) must not reach clients.
* A 2xx response must mean the described side effect happened, or the response must say explicitly that it is pending or unknown.
* Fixes must be enforceable by tests, not by convention.
* Existing in-memory adapter unit tests should keep working.

---

## Considered Options

### Option 1: Keep substring mapping in handlers
* *Eliminated*: Message text is not a contract. Wrapping with `%w` and message edits silently change the status code. This is the mechanism behind RC1.

### Option 2: Fix each defect ad hoc in its handler
* *Eliminated*: Fixes the observed 14 defects but not the mechanism. The next feature repeats the pattern, and handlers diverge further.

### Option 3: Rely on database constraints as the validation layer
* *Eliminated*: Produces RC2 by construction: constraint names and raw driver text leak, and client errors surface as 500.

### Option 4: Typed/sentinel errors, one shared classifier, service-level validation, honest stubs, plus conformance tests (Selected)
* *Selected*: Addresses RC1–RC5 at the mechanism level. The conformance-test part is a follow-up (see below); the code rules are already applied to the routes in the table.

---

## Decision

The following rules are normative for all API handlers and services.

### Checklist

- [ ] **(a) Typed errors.** Services return typed or sentinel errors (for example `ErrInvalidCoverageReport`, `ErrReplayNotAdvanceable`, `ErrIncidentNotFound`). Handlers classify them with `errors.As` through the shared helper `classifyServiceError` in `src/api/rest/v1/handlers/service_error.go`. Never match on message substrings and never use a bare type assertion on an error that may be wrapped. Messages that start with `database integrity constraint violation` are replaced by a generic message by the helper.
- [ ] **(b) Validate before the database.** Validate UUIDs, enums and numeric bounds in the service (or contract) layer and return 400. Database constraints are a backstop only; their names never reach clients. `src/infra/database/error_mapper.go` maps class-23 violations to the generic message `request conflicts with existing data or violates a data integrity rule`. Unique violations are mapped to 409 with a domain message (`database.MapDatabaseError`, `mapUniqueViolation`, `mapInsertError`).
- [ ] **(c) Never discard errors.** No `_ =` or `_, _ =` on persistence calls. A best-effort write (for example the span-audit injection in tracing coverage) must log its error. A required write returns the error wrapped in a sentinel that handlers map to a generic 500.
- [ ] **(d) No success-without-effect stubs.** A stub or placeholder must not return success. It returns an explicit state (`PENDING`, `QUEUED_IN_OUTBOX`, `UNKNOWN`) or HTTP 501.
- [ ] **(e) Envelope honesty.** The response envelope has `success:false` whenever the HTTP status is >= 400.
- [ ] **(f) Tenant scope on list/read.** List and read endpoints require their tenant scope. A missing scope is 400, not `200` with `data:null`.
- [ ] **(g) Secrets never echoed.** Responses must not contain raw secret material.

### HTTP status mapping

| Status | Meaning | Examples from this change |
| :--- | :--- | :--- |
| 400 | Request malformed or violates a validation rule checked before persistence: bad UUID, unknown enum, out-of-range value, missing required field or tenant scope. | non-UUID `eventId`; unknown `semanticClass`; `group_size` missing; rewind target above stored offset; malformed consistency token |
| 404 | The addressed resource (or prerequisite record) does not exist in the caller's scope. | heal of unknown incident; query of unknown materialized view; advance of unknown replay run; rewind with no stored offset; lifecycle update of unknown tenant |
| 409 | Request is well-formed but conflicts with current state: duplicate identity, incompatible change, illegal state transition. | duplicate outbox `externalId`; incompatible schema; replay advance in `INITIALIZING`; duplicate view/topology/rebuild; duplicate CDC slot |
| 422 | Reserved for semantically invalid input that is not a conflict with stored state. **No route in this change was moved to 422**; the existing 400/409 split above is used. | none |
| 500 | Unexpected server or storage failure. Message is generic; no driver text, no constraint names. | storage failure during replay advance; failed group write in envelope build |

### Before / after per route

| Route | Before | After |
| :--- | :--- | :--- |
| `POST /api/v1/outbox` duplicate `externalId` without idempotency key | 500 | 409 `CONFLICT` |
| Outbox relay Kafka messages | no headers | headers `event_type`, `schema_version`, `source_service` plus custom headers (optional `producers.HeaderedProducerPort` / `PublishRecord`; `ProducerPort` unchanged) |
| `POST /api/v1/communication/commands/dispatch` | 202 `DISPATCHED_TO_OUTBOX`, nothing persisted | outbox row inserted (PENDING), status `QUEUED_IN_OUTBOX`; 400 invalid; 409 duplicate `external_id`; `command_payload` must be a JSON string |
| `POST .../incidents/{id}/heal` nonexistent | 200 `healed:true` | 404; `heal_attempts` is read and incremented so the max-attempts / MANUAL_INTERVENTION path is reachable |
| `POST .../probes/canary` | always `MATCH` | records `CANARY_PROBE` row in `consistency_check_run_history`, outcome `UNKNOWN` |
| `POST /api/v1/ops/triage/localize` healthy/unknown flow | 400 (empty link violates `ck_ops_incident_triage_link_btrim`) | 200, default link `/control-plane/consumers/status`, `localized_hop` `NONE` |
| `POST /api/v1/tracing/coverage/report` | accepted anything; errors discarded | 400 `ErrInvalidCoverageReport` (required fields, `hop_type` enum, counts >= 0, propagated <= total); upsert errors returned; span-audit error logged |
| `POST /api/v1/domain-events/envelope/build` with `transaction_group_id` | 200, no group row (SQL "inconsistent types deduced for parameter $9") | `$9::timestamptz` casts; `ErrPersistence` (generic 500) if group/causal write fails; `group_size` > 0 required |
| `POST /api/v1/schemas/upcast` | could never succeed (400 "incomplete upcast chain") | new `POST /api/v1/schemas/upcasters` registers upcasters; built-in `identity` transformer for additive bumps; duplicate `externalId` 409 |
| `POST /api/v1/schemas/register` | compatibility never checked | `CheckSchemaCompatibility` (Avro and JSON Schema); incompatible, duplicate fingerprint or non-monotonic version 409; unknown mode 400 |
| `POST /api/v1/offsets/rewind` | accepted forward jump and unknown partition | 404 if no stored offset; 400 if target > stored offset or > log end |
| `GET /api/v1/offsets/lag` | not registered | registered (`tenant_id` or `X-Tenant-Id`, `group_id`, `topic`, optional `partition`, `rate` default 1) |
| `POST /offsets/commit` | raw `pq` on bad input | 400 (non-empty trimmed ids, partition >= 0, 0 <= offset <= logEnd, `correlationId` UUID) |
| `POST /tenants` bad tier | 500 | 400 |
| `PUT` tenant lifecycle unknown tenant | 500 | 404 |
| `PUT` tenant lifecycle unknown `target_state` | 409 | 400 |
| Duplicate materialized view / topology / rebuild | 400 with raw constraint name | 409 domain message |
| `POST /materialized-views/query` unknown view | 200 version 0 | 404; missing tenant/view 400 |
| `POST /replay/runs/advance` in `INITIALIZING` | allowed | 409 `ErrReplayNotAdvanceable` (only STREAMING / FOLLOWING / CAUGHT_UP) |
| `POST /replay/runs/advance` non-UUID `runId` | raw `pq` | 400 `ErrInvalidRunID` (use `id`, not `publicId` `rpl_*`); unknown run 404; storage failure generic 500 |
| `POST /replay/effects/evaluate` | 200 `SUPPRESSED`, no `replay_effect_log` row | `source_service` / `external_id` / `correlation_id` defaulted; supplied `correlation_id` validated as UUID; insert error returned |
| `GET /cdc/connectors/{name}` | always 404 | 200 via `r.PathValue("name")` |
| CDC signals/heartbeat | required `correlation_id` | `correlation_id` defaulted (UUIDv7); errors via `errors.As` (400/404/409) |
| CDC duplicate `slot_name` / connector | 500 with raw `pq` text | 409 |
| `POST /dlq/quarantine` without headers | 500 (nil map serialised as JSON null) | headers default `{}` |
| `POST /dlq/quarantine` non-UUID `eventId`/`correlationId` | 500 | 400 |
| `POST /dlq/quarantine` duplicate `externalId` | 500 | 409 |
| `GET /dlq/quarantined` missing `tenantId` | 200 `data:null` | 400 |
| Backpressure non-UUID `correlationId` | 400 with raw `pq` text | 400 clean message |
| Backpressure record/signal/transition errors | mixed | `ErrRecordNotFound` 404, `ErrSignalNotSaturated` 400, invalid transition 409 |
| Backpressure `ConfirmRecovery` | concurrency limit stuck at 1 | restored to baseline `rules.DefaultConcurrencyLimit` = 10 (pre-pause limit is not stored) |
| `POST /consumers/inbox` non-UUID `eventId` | 500 | 400 |
| `POST /consumers/inbox` unknown `semanticClass` | silently `DELTA` | 400 (empty still defaults to `DELTA`) |
| `POST /commands/keys` | echoed `raw_secret_value` | not echoed (`signingKeyResponse`); `secret_key_hash` still echoed |
| `GET /queries/resolve` garbage `x-consistency-token` | silent `IMMEDIATE` | 400 `ErrInvalidConsistencyToken` (malformed, expired and bad signature are indistinguishable) |
| Encryption decrypt UNAUTHORIZED / TAMPERED / MALFORMED | envelope `success:true` with HTTP 403/400 | `success:false` when status >= 400 |
| Consumer-groups and DLQ list without tenant parameter | 200 `data:null` | 400 |
| Audit-trail Postgres adapter errors | raw `pq:` text | mapped via `MapDatabaseError` (9 sites); `GET /audits/query` still defaults a missing `tenant_id` to `default` (open) |
| Any class-23 database error without a specific mapping | message included constraint name | generic message, no constraint name |

---

## Failure-First Analysis & Failure Modes

| Failure Archetype | Trigger Condition | Detection Signal | Prevention / Mitigation Mechanism |
| :--- | :--- | :--- | :--- |
| **Wrapped error misclassified** | Service wraps with `%w`; handler uses bare assertion or substring | Real 404/409 returned as 500 or 400 | Rule (a): `classifyServiceError` with `errors.As` |
| **Constraint leak** | Client input reaches the database unvalidated | `pq:` text or `ck_`/`uk_` names in a response body | Rule (b): service-level validation; generic class-23 message |
| **Silent persistence loss** | Error ignored with `_ =` | 2xx response, no row | Rule (c): return or log; wrap sentinel to generic 500 |
| **False success from stub** | Scaffold left in a route | 2xx with no side effect | Rule (d): explicit PENDING/UNKNOWN/501 |
| **Envelope contradicts status** | Handler writes `success:true` on an error path | `success:true` with HTTP >= 400 | Rule (e) |
| **Scope-less list** | Tenant parameter omitted | 200 with `data:null` | Rule (f): 400 |

---

## Consequences

### Positive
* Status codes on the routes above are derived from typed errors, not message text.
* Clients no longer receive driver text or constraint names from the fixed routes.
* Two previously unusable capabilities exist: upcaster registration and offset lag evaluation.

### Negative
* Several responses changed status codes (see `src/api/contracts/openapi/changelog.md` for the breaking subset). Existing clients that relied on, for example, 500 for duplicates must change.
* `ConfirmRecovery` restores a fixed baseline limit (10), not the pre-pause limit.

### Neutral
* `ProducerPort` is unchanged; header support is an optional extension interface.

### Still open (not fixed)
* `redrive` / `discard` with a non-UUID `quarantineId` still returns 500.
* `GET retries/due`, `events/held`, consistency `watermark` / `incidents` / `sweeps` still return `200 data:null` without a tenant parameter (outside the scope of the fixing agents).
* `secret_key_hash` is still echoed by `POST /commands/keys`.
* Replay create-run with non-UUID `correlation_id` is not validated.
* `verify-envelope` passes for a SUSPENDED tenant.
* Apicurio is written only by `POST /api/v1/schemas`.
* `/metrics` is absent.
* There are no HTTP-level tests for the handler fixes (partial: `handlers/multi_tenant_handler_test.go`).
* Postgres-backed integration paths (unique-violation mapping, migration 0028, replay adapter) are untested in CI.
* Custom upcast transformers are not registered at startup; an unknown transformer name is accepted at registration and fails with 400 at upcast time.
* CDC still uses the mock Kafka Connect adapter; Debezium is not integrated.

### Follow-ups
1. Add an HTTP-level conformance suite that drives every route through the real router against PostgreSQL, asserting status code, `success` flag, absence of `pq:` / constraint names in bodies, and tenant-scope 400s.
2. Run that suite in CI with a Postgres service and **fail** (not skip) when `DATABASE_URL` / `TEST_DATABASE_URL` is unset, so integration-only defects cannot skip silently.
3. Close the "Still open" items above, starting with the UUID validation for quarantine ids and the tenant-scope 400s.
4. Apply migration 0028 through the migration tooling (see ADR-0003).

---

## Verification

* `go build`, `go vet` and `go test` were run across all packages.
* The image `chiefj/event-driven-platform:fixes` was rebuilt and run against the live PostgreSQL, Kafka and Apicurio stack, and each fixed route was re-called over HTTP. Routes marked "unit-only" in the working notes were not verified live: the `schemas/upcasters` registration flow beyond route existence is covered by unit tests only.
* Migration 0028 was **not** applied to the live database, so the per-tenant uniqueness change is unverified against Postgres.

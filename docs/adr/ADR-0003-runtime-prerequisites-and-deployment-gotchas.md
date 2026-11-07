# ADR-0003: Runtime Prerequisites and Deployment Gotchas

* **Status**: Accepted
* **Date**: 2026-11-07
* **Deciders**: Platform Architecture Core Team
* **Technical Domain**: Runtime Infrastructure, Deployment, Operations

---

## Context and Problem Statement

While running the end-to-end API test described in ADR-0002 against the live stack, several failures were operational rather than code defects (RC6, RC7). They are inferred from code and runtime observation, not from history. This ADR records them so they are not rediscovered.

---

## Decision Drivers

* Each prerequisite must be discoverable without reading source.
* Do not claim a gotcha is fixed when it is not.
* Commands must be copy-pasteable.

---

## Considered Options

### Option 1: Fix every item in code or compose now
* *Not done*: the AppArmor/tini conflict needs a user decision on which hardening to relax.

### Option 2: Leave undocumented
* *Eliminated*: each item cost debugging time.

### Option 3: Document prerequisites; fix only what is unambiguous (Selected)
* The Dockerfile builder argument is documented (build-time override); the compose/tini conflict is documented with options and **is not fixed in the repo**.

---

## Decision

Treat the items below as documented prerequisites.

### 1. Outbox topics must exist (RC6)

The relay publishes to the Kafka topic named by the row's `aggregate_type`. The broker (`apache/kafka:3.8.0` in `deploy/docker/compose/docker-compose.infra.yml`) has no topic auto-creation configured by this repo's compose file that we could verify; in the tested stack, publishing to a missing topic failed. A row moves to `PARKED` once `retry_count + 1 >= 5` (see `src/features/transactional-outbox/infrastructure/queries/transactional-outbox.queries.go`), with the error "Unknown Topic Or Partition".

Create topics before sending outbox traffic (container name taken from the compose project; adjust if yours differs):

```bash
docker exec event_platform-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic <aggregate_type> --partitions 3 --replication-factor 1
```

Note that `commands/dispatch` sets `aggregate_type = target_topic`, so `target_topic` must be an existing topic.

Recommendation: either provision topics through deployment tooling per aggregate type, or enable `auto.create.topics.enable=true` on non-production brokers only. Parked rows stay in the table and are visible through the outbox parked queries; they are not retried automatically.

### 2. The app reads only `DATABASE_URL`

`src/infra/config/config.go` and `src/infra/chassis/config.go` read `DATABASE_URL`. The variables `DB_HOST` and `DB_PASSWORD_FILE` are not read. Without `DATABASE_URL` the app falls back to `127.0.0.1:5432`.

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable'
```

### 3. `serve` does not run migrations

Startup does not apply migrations, and the live database had no `schema_migrations` table, so there is no applied-version tracking. Migration `0028_scope_partitioning_and_rebuild_uniqueness_per_tenant.sql` (scopes `partition_topic_topology.topic_name`, `partition_migration_runbook_log.migration_id` and `materialized_view_rebuild_log.rebuild_id` to `(tenant_id, ...)`) was **not applied** to the live database.

`scripts/migrate/migrate.sh` iterates every forward migration file in order with no tracking, so re-running it re-executes all files. To apply only 0028:

```bash
psql "$DATABASE_URL" -f database/migrations/sql/0028_scope_partitioning_and_rebuild_uniqueness_per_tenant.sql
```

Rollback: each migration has a `.rollback.sql` companion. `scripts/migrate/rollback.sh` in the repo is a stub (it only prints a success message), so roll back by hand:

```bash
psql "$DATABASE_URL" -f database/migrations/sql/0028_scope_partitioning_and_rebuild_uniqueness_per_tenant.rollback.sql
```

The 0028 rollback restores global uniqueness and fails if tenants by then share values.

### 4. Dockerfile builder image for Go 1.26

`deploy/docker/service/Dockerfile` (and `Dockerfile.dev`) default to `ARG BUILDER_IMAGE=golang:1.23-bookworm`. `go.mod` now requires Go 1.26 (ADR-0001), so the default build fails. Override at build time:

```bash
docker build -f deploy/docker/service/Dockerfile \
  --build-arg BUILDER_IMAGE=golang:1.26-bookworm \
  -t chiefj/event-driven-platform:fixes .
```

The default in the Dockerfile was not changed by this work.

### 5. Compose hardening vs image tini (RC7, fixed with an explicit host override)

**Observed.** On a host using Docker's default AppArmor profile, the image's `ENTRYPOINT ["/usr/bin/tini", ...]` fails with `exec /usr/bin/tini: operation not permitted` when the container runs with `no-new-privileges`. This was reproduced outside compose with `docker run --cap-drop ALL --security-opt no-new-privileges --entrypoint /usr/bin/tini <image> -h` (fails), and the same command without `no-new-privileges` (works). `init: true` additionally wrapped tini in `docker-init`, which failed the same way, and it is redundant because the image already runs tini as PID 1.

**Change (`deploy/docker/compose/docker-compose.single.yml`).**
* `init: false`: the image provides tini.
* `security_opt: ["no-new-privileges:${NO_NEW_PRIVILEGES:-true}"]`: the hardened default is unchanged; a host that hits the AppArmor problem opts out explicitly with `NO_NEW_PRIVILEGES=false`. This was a deliberate choice over silently dropping the option for every host. It was deployed on the affected host with the override and the container reached `healthy`.
* Healthcheck: the runtime image has no `wget` (the old check could never pass). It now uses `bash` and `/dev/tcp` against the admin port: `["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8081 && printf 'GET /health/live HTTP/1.0\\r\\n\\r\\n' >&3 && head -n1 <&3 | grep -q ' 200 '"]`. `CMD-SHELL` is not used because `/dev/tcp` is a bash feature and `CMD-SHELL` runs `sh`.

**Deploy command used.**

```bash
cd deploy/docker/compose
export COMPOSE_PROJECT_NAME=event_platform REGISTRY=chiefj NO_NEW_PRIVILEGES=false
docker compose --profile observability -f docker-compose.single.yml up -d --no-build --no-deps app
```

`COMPOSE_PROJECT_NAME` must be `event_platform` to join the existing stack (the default is the directory name `compose`, which creates separate networks). The `observability` profile is required because `app` depends on `otel-collector`. The app exposes 8080/8081 only inside the Docker network; reach the API through Envoy on host port 8080 (`/api/v1/...`).

**Limits.** The root cause inside AppArmor/runc was not investigated; the override is a host-level workaround. Hosts without the problem should leave `NO_NEW_PRIVILEGES` unset.

### 6. Health and metrics endpoints

Health endpoints are served on the admin listener only (`/health/live`, `/health/ready`, from `src/infra/chassis/runner.go`), default `127.0.0.1:8081` (`config/default.yaml`, `SERVER_ADMIN_BIND_ADDRESS`). They are not on the public port 8080. In a container, the admin listener on loopback is unreachable from outside unless bound differently.

```bash
curl -s http://127.0.0.1:8081/health/live
curl -s http://127.0.0.1:8081/health/ready
```

`/metrics` is not exposed by the service.

---

## Consequences

### Positive
* Known failure causes are written down with commands.

### Negative
* Items 4 and 5 still require manual action; item 5 leaves the single-host compose stack failing on affected hosts until a decision is made.
* `rollback.sh` being a stub means rollback is manual.

### Neutral
* No code changed for this ADR.


---

## Production-readiness pass (2026-11-07)

### 7. One-command, self-contained stack

`deploy/docker/compose/docker-compose.all.yml` starts Postgres (logical WAL), Kafka (single-node KRaft, topic auto-create on), Apicurio, Debezium/Kafka Connect, a one-shot `migrate` job (`platform-service migrate`, tracked by `schema_migrations`) and the app (starts only after `migrate` succeeded and dependencies are healthy). Every image, host port and password is an environment variable with a default (`deploy/docker/compose/.env.example`; table in the root `README.md`). Verified from scratch in an isolated project on alternate ports: `up -d --wait` reached all-healthy, the migrate job exited 0 with 28 migrations applied (74 public tables), and `scripts/test/smoke.sh` passed 10/10.

Quickstart defaults are for local/dev: `POSTGRES_PASSWORD=postgres`, Kafka topic auto-create on, a single broker. Change the password (it must be URL-safe) and use a managed/replicated Kafka and registry outside local use.

### 8. Kafka producer topic creation (bug found while verifying the one-command stack)

`kafka-go` `Writer` does not ask the broker to create a missing topic unless `AllowAutoTopicCreation` is set, so even with `auto.create.topics.enable=true` on the broker the first outbox publish failed with `[3] Unknown Topic Or Partition` and the row was PARKED after 5 retries (a topic created by a console consumer masked this during earlier testing). The producer now sets `AllowAutoTopicCreation` (default on; `KAFKA_PRODUCER_AUTO_CREATE_TOPICS=false` disables). The broker policy still decides: with broker auto-create disabled, topics named after `aggregateType` must still be pre-created (section 1). Verified: a never-seen `aggregateType` published, with headers, in the fresh stack.

### 9. Real CDC (Kafka Connect / Debezium)

Setting `KAFKA_CONNECT_URL` makes the CDC feature call the Kafka Connect REST API instead of the in-memory mock (unset: mock, with a startup warning). Database settings for the connector: `CDC_DB_HOST` (default `db`), `CDC_DB_PORT` (`5432`), `CDC_DB_USER` (`postgres`), `CDC_DB_NAME` (`event_store`), `CDC_DB_PASSWORD` or `CDC_DB_PASSWORD_FILE`, `CDC_RAW_TABLE_INCLUDE_LIST` (raw-table mode). Verified live: `POST /api/v1/cdc/connectors` created a connector that Debezium reported as `RUNNING` (connector and task). Limits: create is never retried; other calls retry up to 3 times on 5xx/connection errors, so a retried delete can return 404 if the first attempt succeeded but its response was lost.

### 10. Metrics, body limit, shutdown

* `GET /metrics` (Prometheus text format) is on the admin listener (8081), not the public port: `http_requests_total{method,route,status}` (route = mux pattern, `unmatched` otherwise), `http_request_duration_seconds`, `http_requests_in_flight`, `go_goroutines`, `go_memstats_heap_alloc_bytes`, `go_gc_cycles_total`, `process_start_time_seconds`, `edp_build_info{version}` (image `GIT_SHA`).
* Request bodies are capped (`MAX_REQUEST_BODY_BYTES`, default 1 MiB): a declared oversize body gets 413 `PAYLOAD_TOO_LARGE` (verified). Chunked bodies are capped by `MaxBytesReader` and surface as the handler's 400.
* Shutdown: readiness flips to false first, then in-flight requests drain (15 s); a defect where the root context was not cancelled on the fatal-error path (daemons such as the outbox poller kept running) was fixed. **Not done:** the Postgres pool and Kafka producer are not closed on shutdown (no close hook in the service container), and components stop concurrently rather than in dependency order.

### 11. Remaining limits (be explicit before calling this production-grade)

* Single-node dev topology in the quickstart; no replication, no TLS between components, default credentials.
* No outbox backlog / parked-row gauge; alert on the `outbox` table (`status = 'PARKED'`) until one exists.
* Postgres-backed tests skip without a DSN locally; the new CI workflow runs them against a Postgres service and fails on skips (not yet run on GitHub).
* The `no-new-privileges` exec failure is a host AppArmor issue worked around with `NO_NEW_PRIVILEGES=false` (section 5).

### 12. Secret files and the container user

The service runs as UID 10001. A compose secret bind-mounted from a host file owned by another user with mode 0600 is unreadable inside the container (`CDC: cannot read CDC_DB_PASSWORD_FILE` in the log). `docker-compose.single.yml` therefore passes `CDC_DB_PASSWORD` (default `postgres`, overridable) like `DATABASE_URL` already does; use `CDC_DB_PASSWORD_FILE` only when the file is readable by UID 10001. The app itself does not read `DB_PASSWORD_FILE`; only `DATABASE_URL` is used for the database.

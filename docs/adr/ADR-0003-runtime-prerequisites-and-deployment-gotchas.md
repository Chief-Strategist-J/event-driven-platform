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

### 5. Compose hardening vs image tini (RC7, NOT fixed)

`deploy/docker/compose/docker-compose.single.yml` sets `security_opt: ["no-new-privileges:true"]` and `init: true` for hardened app services, while the image already has `ENTRYPOINT ["/usr/bin/tini", "--", "/app/bin/platform-service"]` (`deploy/docker/service/Dockerfile`). On hosts using the default Docker AppArmor profile, the container fails with `exec /usr/bin/tini: operation not permitted`. This is a repo-level conflict and **remains unfixed pending a decision**. Options:

1. Drop `init: true` from the hardening anchor (the image's tini already acts as init). Smallest change; keeps `no-new-privileges`.
2. Adjust the host's AppArmor handling (custom profile or `apparmor=unconfined` for the service). Weakens isolation.
3. Remove tini from the image `ENTRYPOINT` and rely on `init: true`. Changes the image for non-compose users.

Not verified here: which option resolves it on every host; only the failing configuration was observed.

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

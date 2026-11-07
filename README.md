# event-driven-platform
Kafka, CQRS, Event Sourcing, CDC

## Run everything with one command

Requires Docker with Compose v2. No other files or configuration are needed (the image is pulled from Docker Hub).

```bash
docker compose -f deploy/docker/compose/docker-compose.all.yml up -d --wait
# or, the wrapper (checks prerequisites, pulls, starts, prints URLs):
scripts/run/quickstart.sh
```

The stack starts Postgres (logical WAL), Kafka (single-node KRaft, topic auto-create on), Apicurio Registry,
Debezium/Kafka Connect, a one-shot `migrate` job (applies `database/migrations/sql`), then the app.
Stop and wipe data: `docker compose -f deploy/docker/compose/docker-compose.all.yml down -v`.
Optional Prometheus (scrapes `app:8081/metrics`): add `--profile observability`.

### Endpoints
| Endpoint | Default URL |
|---|---|
| API (`/api/v1/...`) | `http://127.0.0.1:8080` |
| Liveness / readiness | `http://127.0.0.1:8081/health/live`, `/health/ready` |
| Metrics | `http://127.0.0.1:8081/metrics` |
| Prometheus (profile `observability`) | `http://127.0.0.1:9090` |

### Configuration (environment variables, all optional)
Put them in the shell or in `deploy/docker/compose/.env` (template: `deploy/docker/compose/.env.example`).

| Variable | Default | Purpose |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `event-driven-platform` | Compose project name |
| `EDP_IMAGE` / `EDP_TAG` | `chiefj/event-driven-platform` / `latest` | App image |
| `POSTGRES_IMAGE` | `postgres:16-bookworm` | Postgres image |
| `KAFKA_IMAGE` | `apache/kafka:3.8.0` | Kafka image |
| `APICURIO_IMAGE` | `apicurio/apicurio-registry-mem:2.6.5.Final` | Schema registry image |
| `DEBEZIUM_IMAGE` | `debezium/connect:2.7.3.Final` | Kafka Connect image |
| `PROMETHEUS_IMAGE` | `prom/prometheus:v2.54.1` | Prometheus image (profile `observability`) |
| `EDP_BIND_ADDR` | `127.0.0.1` | Host interface for published ports |
| `EDP_API_PORT` | `8080` | Host port for the API |
| `EDP_ADMIN_PORT` | `8081` | Host port for health/metrics |
| `EDP_POSTGRES_PORT` | `15432` | Host port for Postgres |
| `EDP_PROMETHEUS_PORT` | `9090` | Host port for Prometheus |
| `POSTGRES_DB` / `POSTGRES_USER` | `event_store` / `postgres` | Database name and user |
| `POSTGRES_PASSWORD` | `postgres` | Database password (must be URL-safe; change outside local use) |
| `ENVIRONMENT` | `production` | Passed to the app as `ENVIRONMENT` |
| `MAX_REQUEST_BODY_BYTES` | `1048576` | Passed to the app (request body cap) |
| `NO_NEW_PRIVILEGES` | `true` | Set `false` if your AppArmor profile blocks exec under no-new-privileges (see ADR-0003) |

Inside the stack the app receives `DATABASE_URL`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `KAFKA_CONNECT_URL`,
`CDC_DB_HOST/PORT/USER/PASSWORD/NAME`, `PORT=8080`, `ADMIN_PORT=8081`, `ADMIN_HOST=0.0.0.0`. The Kafka-Connect/CDC and
`MAX_REQUEST_BODY_BYTES` settings and `/metrics` require an image tag that includes those features; older tags ignore them.

### Smoke test
```bash
scripts/test/smoke.sh      # BASE / ADMIN env vars override the URLs; exits non-zero on any failure
```

### More
* [ADR-0002: API error semantics, validation and persistence honesty](docs/adr/ADR-0002-api-error-semantics-validation-and-persistence-honesty.md)
* [ADR-0003: Runtime prerequisites and deployment gotchas](docs/adr/ADR-0003-runtime-prerequisites-and-deployment-gotchas.md)
* [OpenAPI contract](src/api/contracts/openapi/v1.yaml)

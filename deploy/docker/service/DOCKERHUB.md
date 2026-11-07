# event-driven-platform

Go service implementing an event-driven platform on **PostgreSQL + Kafka (+ Apicurio schema registry)**.
It exposes a REST API (`/api/v1/...`, about 100 routes) covering the transactional outbox and relay, CQRS with
signed consistency tokens, replay, retry topics, dead-letter handling, idempotent consumers, consumer
groups, offset management, schema evolution and versioning, multi-tenant isolation, materialized views,
partitioning, encryption (crypto-shredding), audit trail (hash chain), distributed tracing and
consistency monitoring.

**Source:** https://github.com/Chief-Strategist-J/event-driven-platform
**API contract:** `src/api/contracts/openapi/v1.yaml` (OpenAPI 3.1, v1.2.0)

## Tags
| Tag | Meaning |
|---|---|
| `latest`, `v2`, `fixes` | Current build (same digest): API error-semantics hardening, tenant-scope validation, CQRS fallback-secret removal. |
| `d4d9b0a` | Earlier build, before the fixes. |

## Ports
| Port | Purpose |
|---|---|
| 8080 | Public API (`/api/v1/...`) |
| 8081 | Admin: `GET /health/live`, `GET /health/ready` (there is no `/metrics`) |

## Run
```bash
docker run -d --name edp -p 8080:8080 -p 8081:8081 \
  -e PORT=8080 -e ADMIN_PORT=8081 -e ADMIN_HOST=0.0.0.0 \
  -e DATABASE_URL='postgres://USER:PASS@HOST:5432/event_store?sslmode=disable' \
  -e KAFKA_BROKERS=HOST:9092 \
  -e SCHEMA_REGISTRY_URL=http://HOST:8080/apis/registry/v2 \
  chiefj/event-driven-platform:latest
curl -s localhost:8081/health/ready
```

## Configuration (environment)
| Variable | Notes |
|---|---|
| `DATABASE_URL` | **Required.** The only database setting read; without it the service falls back to `127.0.0.1:5432`. |
| `KAFKA_BROKERS` | Comma-separated brokers (default `127.0.0.1:9092`). |
| `SCHEMA_REGISTRY_URL` | Apicurio v2 API (used by `POST /api/v1/schemas`). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint. |
| `PORT`, `ADMIN_PORT`, `ADMIN_HOST` | Listener settings. Set `ADMIN_HOST=0.0.0.0` in a container. |

## Things to know
* **Outbox topics:** the relay publishes to a Kafka topic named after the record's `aggregateType`. The broker must already have that topic (auto-create is off in the reference stack); otherwise rows are PARKED after 5 retries.
* **Migrations are not run at startup.** Apply `database/migrations/sql/*.sql` yourself (0028 scopes topic, migration and rebuild ids per tenant).
* **Docker hardening:** the image runs `tini` as PID 1 (do not add `--init`). On hosts whose AppArmor profile blocks exec under `no-new-privileges`, the reference compose file reads `NO_NEW_PRIVILEGES=false`.
* **CDC** uses a mock Kafka Connect adapter (no Debezium integration yet).
* Runs as non-root (UID 10001) on `debian:bookworm-slim`.

## Compose
`deploy/docker/compose/docker-compose.single.yml` in the repository (project name `event_platform`, profile `observability`).

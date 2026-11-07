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
| `latest`, `v3` | Current build: real Kafka Connect/Debezium CDC (`KAFKA_CONNECT_URL`), `/metrics`, request body cap, producer topic auto-creation, error-semantics and tenant-scope hardening. |
| `v2`, `fixes` | Previous build (error-semantics hardening, no metrics, CDC mock only). |
| `d4d9b0a` | Earlier build, before the fixes. |

## Ports
| Port | Purpose |
|---|---|
| 8080 | Public API (`/api/v1/...`) |
| 8081 | Admin: `GET /health/live`, `GET /health/ready`, `GET /metrics` (Prometheus) |

## Run everything with one command
The repository ships a self-contained compose file (Postgres, Kafka, Apicurio, Debezium, a migrate job and this image):
```bash
git clone https://github.com/Chief-Strategist-J/event-driven-platform && cd event-driven-platform
docker compose -f deploy/docker/compose/docker-compose.all.yml up -d --wait
scripts/test/smoke.sh   # optional: 10 end-to-end checks
```
All images, host ports and passwords are env vars with defaults (`deploy/docker/compose/.env.example`). API: http://127.0.0.1:8080, health/metrics: http://127.0.0.1:8081.

## Run only this image
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
| `KAFKA_CONNECT_URL` | Kafka Connect/Debezium REST URL. Set = real CDC; unset = in-memory mock. Uses `CDC_DB_HOST/PORT/USER/NAME` and `CDC_DB_PASSWORD` or `CDC_DB_PASSWORD_FILE`. |
| `MAX_REQUEST_BODY_BYTES` | Request body cap (default 1048576; larger declared bodies get 413). |
| `KAFKA_PRODUCER_AUTO_CREATE_TOPICS` | `false` stops the producer asking the broker to auto-create topics (default on). |
| `PORT`, `ADMIN_PORT`, `ADMIN_HOST` | Listener settings. Set `ADMIN_HOST=0.0.0.0` in a container. |

## Things to know
* **Outbox topics:** the relay publishes to a Kafka topic named after the record's `aggregateType`. The producer asks the broker to auto-create it; if the broker has auto-create disabled the topic must already exist, otherwise rows are PARKED after 5 retries.
* **Migrations are not run by `serve`.** Run the same image with the `migrate` command once (`docker run --rm -e DATABASE_URL=... chiefj/event-driven-platform:latest migrate`); the all-in-one compose does this for you.
* **Docker hardening:** the image runs `tini` as PID 1 (do not add `--init`). On hosts whose AppArmor profile blocks exec under `no-new-privileges`, the reference compose file reads `NO_NEW_PRIVILEGES=false`.
* **CDC** talks to Kafka Connect/Debezium when `KAFKA_CONNECT_URL` is set; otherwise an in-memory mock is used (startup warning).
* Runs as non-root (UID 10001) on `debian:bookworm-slim`.

## Compose
`deploy/docker/compose/docker-compose.single.yml` in the repository (project name `event_platform`, profile `observability`).

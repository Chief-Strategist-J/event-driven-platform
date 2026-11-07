## Amendment 2026-11-07: Apicurio write path and GET semantics

- `POST /api/v1/schemas` is the only route that writes to Apicurio.
- schema-evolution (`POST /api/v1/schemas/register`) and event-versioning (`POST /api/v1/schemas/upcasters`, upcast) write to Postgres only; they do not publish to Apicurio.
- `GET /api/v1/schemas` is intentionally unsupported and returns 405.

package queries

const (
	FlowInsertSchemaVersion = `
INSERT INTO event_schema_version (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    stream_name, event_type, schema_version, schema_id, format,
    compatibility_mode, schema_definition, payload_hash, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);`

	FlowGetLatestSchemaVersion = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       stream_name, event_type, schema_version, schema_id, format,
       compatibility_mode, schema_definition, payload_hash, created_at
FROM event_schema_version
WHERE tenant_id = $1 AND stream_name = $2 AND event_type = $3
ORDER BY schema_version DESC
LIMIT 1;`

	FlowGetSchemaVersion = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       stream_name, event_type, schema_version, schema_id, format,
       compatibility_mode, schema_definition, payload_hash, created_at
FROM event_schema_version
WHERE tenant_id = $1 AND stream_name = $2 AND event_type = $3 AND schema_version = $4;`

	FlowListSchemaVersions = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       stream_name, event_type, schema_version, schema_id, format,
       compatibility_mode, schema_definition, payload_hash, created_at
FROM event_schema_version
WHERE tenant_id = $1 AND stream_name = $2 AND event_type = $3
ORDER BY schema_version ASC;`

	FlowInsertUpcaster = `
INSERT INTO event_upcaster_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    event_type, from_version, to_version, transformer_name, is_active, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (tenant_id, event_type, from_version, to_version)
DO UPDATE SET transformer_name = EXCLUDED.transformer_name,
              is_active = EXCLUDED.is_active;`

	FlowGetUpcasterChain = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       event_type, from_version, to_version, transformer_name, is_active, created_at
FROM event_upcaster_registry
WHERE tenant_id = $1 AND event_type = $2 AND from_version >= $3 AND to_version <= $4 AND is_active = TRUE
ORDER BY from_version ASC;`
)

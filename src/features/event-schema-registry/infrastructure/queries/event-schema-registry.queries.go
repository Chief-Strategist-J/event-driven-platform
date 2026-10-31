package queries

const (
	FlowInsertSchema = "INSERT INTO event_schemas (id, tenant_id, subject, version, format, schema, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
	FlowSelectSchemaByID = "SELECT id, tenant_id, subject, version, format, schema, status, created_at, updated_at FROM event_schemas WHERE tenant_id = $1 AND id = $2"
)

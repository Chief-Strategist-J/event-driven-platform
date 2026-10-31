package eventschemaregistry

type SchemaStatus string

const (
	SchemaStatusDraft      SchemaStatus = "DRAFT"
	SchemaStatusActive     SchemaStatus = "ACTIVE"
	SchemaStatusDeprecated SchemaStatus = "DEPRECATED"
	SchemaStatusArchived   SchemaStatus = "ARCHIVED"
)

type SchemaDefinition struct {
	ID          string       `json:"id"`
	TenantID    string       `json:"tenantId"`
	Subject     string       `json:"subject"`
	Version     int32        `json:"version"`
	Format      string       `json:"format"`
	Schema      string       `json:"schema"`
	Status      SchemaStatus `json:"status"`
	CreatedAt   string       `json:"createdAt"`
	UpdatedAt   string       `json:"updatedAt"`
}

type RegisterSchemaInput struct {
	ID       string `json:"id"`
	TenantID string `json:"tenantId"`
	Subject  string `json:"subject"`
	Format   string `json:"format"`
	Schema   string `json:"schema"`
}

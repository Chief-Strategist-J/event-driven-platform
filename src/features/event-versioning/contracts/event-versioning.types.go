package contracts

import (
	"encoding/json"
	"time"
)

type SchemaFormat string

const (
	FormatJSONSchema SchemaFormat = "JSON_SCHEMA"
	FormatAvro       SchemaFormat = "AVRO"
	FormatProtobuf   SchemaFormat = "PROTOBUF"
)

type CompatibilityMode string

const (
	CompatFullTransitive     CompatibilityMode = "FULL_TRANSITIVE"
	CompatBackwardTransitive CompatibilityMode = "BACKWARD_TRANSITIVE"
	CompatForwardTransitive  CompatibilityMode = "FORWARD_TRANSITIVE"
	CompatNone               CompatibilityMode = "NONE"
)

type RegisterSchemaVersionInput struct {
	TenantID          string            `json:"tenantId"`
	SourceService     string            `json:"sourceService"`
	ExternalID        string            `json:"externalId"`
	CorrelationID     string            `json:"correlationId,omitempty"`
	StreamName        string            `json:"streamName"`
	EventType         string            `json:"eventType"`
	SchemaVersion     int               `json:"schemaVersion"`
	SchemaID          string            `json:"schemaId"`
	Format            SchemaFormat      `json:"format"`
	CompatibilityMode CompatibilityMode `json:"compatibilityMode"`
	SchemaDefinition  json.RawMessage   `json:"schemaDefinition"`
}

type SchemaVersionRecord struct {
	ID                string            `json:"id"`
	PublicID          string            `json:"publicId"`
	TenantID          string            `json:"tenantId"`
	SourceService     string            `json:"sourceService"`
	ExternalID        string            `json:"externalId"`
	CorrelationID     string            `json:"correlationId"`
	StreamName        string            `json:"streamName"`
	EventType         string            `json:"eventType"`
	SchemaVersion     int               `json:"schemaVersion"`
	SchemaID          string            `json:"schemaId"`
	Format            SchemaFormat      `json:"format"`
	CompatibilityMode CompatibilityMode `json:"compatibilityMode"`
	SchemaDefinition  json.RawMessage   `json:"schemaDefinition"`
	PayloadHash       string            `json:"payloadHash"`
	CreatedAt         time.Time         `json:"createdAt"`
}

type CheckCompatibilityInput struct {
	TenantID          string            `json:"tenantId"`
	StreamName        string            `json:"streamName"`
	EventType         string            `json:"eventType"`
	TargetVersion     int               `json:"targetVersion"`
	CompatibilityMode CompatibilityMode `json:"compatibilityMode"`
	NewSchema         json.RawMessage   `json:"newSchema"`
}

type CompatibilityCheckResult struct {
	IsCompatible bool     `json:"isCompatible"`
	Violations   []string `json:"violations,omitempty"`
}

type RegisterUpcasterInput struct {
	TenantID        string `json:"tenantId"`
	SourceService   string `json:"sourceService"`
	ExternalID      string `json:"externalId"`
	CorrelationID   string `json:"correlationId,omitempty"`
	EventType       string `json:"eventType"`
	FromVersion     int    `json:"fromVersion"`
	ToVersion       int    `json:"toVersion"`
	TransformerName string `json:"transformerName"`
}

type UpcastStepInput struct {
	TenantID      string          `json:"tenantId"`
	EventType     string          `json:"eventType"`
	SourceVersion int             `json:"sourceVersion"`
	TargetVersion int             `json:"targetVersion"`
	Payload       json.RawMessage `json:"payload"`
}

type UpcastResult struct {
	EventType      string          `json:"eventType"`
	InitialVersion int             `json:"initialVersion"`
	FinalVersion   int             `json:"finalVersion"`
	Payload        json.RawMessage `json:"payload"`
	StepsApplied   int             `json:"stepsApplied"`
}

type UpcasterRecord struct {
	ID              string    `json:"id"`
	PublicID        string    `json:"publicId"`
	TenantID        string    `json:"tenantId"`
	SourceService   string    `json:"sourceService"`
	ExternalID      string    `json:"externalId"`
	CorrelationID   string    `json:"correlationId"`
	EventType       string    `json:"eventType"`
	FromVersion     int       `json:"fromVersion"`
	ToVersion       int       `json:"toVersion"`
	TransformerName string    `json:"transformerName"`
	IsActive        bool      `json:"isActive"`
	CreatedAt       time.Time `json:"createdAt"`
}

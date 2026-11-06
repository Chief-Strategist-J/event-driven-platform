package contracts

import (
	"time"

	"github.com/google/uuid"
)

type CompatibilityMode string

const (
	CompatibilityFullTransitive     CompatibilityMode = "FULL_TRANSITIVE"
	CompatibilityBackwardTransitive CompatibilityMode = "BACKWARD_TRANSITIVE"
	CompatibilityForwardTransitive  CompatibilityMode = "FORWARD_TRANSITIVE"
)

type EvolutionStatus string

const (
	EvolutionStatusActive     EvolutionStatus = "ACTIVE"
	EvolutionStatusDeprecated EvolutionStatus = "DEPRECATED"
	EvolutionStatusRetired    EvolutionStatus = "RETIRED"
)

type IncidentClass string

const (
	IncidentSingleCorrupt   IncidentClass = "SINGLE_CORRUPT"
	IncidentSystemicFailure IncidentClass = "SYSTEMIC_FAILURE"
	IncidentUnknownSchemaID IncidentClass = "UNKNOWN_SCHEMA_ID"
	IncidentSemanticDrift   IncidentClass = "SEMANTIC_DRIFT"
)

type SchemaVersionRecord struct {
	ID                uuid.UUID         `json:"id"`
	PublicID          string            `json:"public_id"`
	TenantID          string            `json:"tenant_id"`
	SourceService     string            `json:"source_service"`
	ExternalID        string            `json:"external_id"`
	CorrelationID     uuid.UUID         `json:"correlation_id"`
	SubjectName       string            `json:"subject_name"`
	SchemaVersion     int               `json:"schema_version"`
	CompatibilityMode CompatibilityMode `json:"compatibility_mode"`
	SchemaFingerprint string            `json:"schema_fingerprint"`
	SchemaDefinition  string            `json:"schema_definition"`
	Status            EvolutionStatus   `json:"status"`
	Version           int64             `json:"version"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type ConsumerSupportRecord struct {
	ID                  uuid.UUID `json:"id"`
	PublicID            string    `json:"public_id"`
	TenantID            string    `json:"tenant_id"`
	SourceService       string    `json:"source_service"`
	ExternalID          string    `json:"external_id"`
	CorrelationID       uuid.UUID `json:"correlation_id"`
	ConsumerGroup       string    `json:"consumer_group"`
	SubjectName         string    `json:"subject_name"`
	MinSupportedVersion int       `json:"min_supported_version"`
	MaxSupportedVersion int       `json:"max_supported_version"`
	RunCadence          string    `json:"run_cadence"`
	Status              string    `json:"status"`
	Version             int64     `json:"version"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type DualPublishRecord struct {
	ID                   uuid.UUID `json:"id"`
	PublicID             string    `json:"public_id"`
	TenantID             string    `json:"tenant_id"`
	SourceService        string    `json:"source_service"`
	ExternalID           string    `json:"external_id"`
	CorrelationID        uuid.UUID `json:"correlation_id"`
	SubjectName          string    `json:"subject_name"`
	LegacyTopic          string    `json:"legacy_topic"`
	ModernTopic          string    `json:"modern_topic"`
	SunsetDeadline       time.Time `json:"sunset_deadline"`
	Status               string    `json:"status"`
	DecommissionAttested bool      `json:"decommission_attested"`
	ZeroLagDays          int       `json:"zero_lag_days"`
	Version              int64     `json:"version"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type EvolutionIncidentRecord struct {
	ID                 uuid.UUID     `json:"id"`
	PublicID           string        `json:"public_id"`
	TenantID           string        `json:"tenant_id"`
	SourceService      string        `json:"source_service"`
	ExternalID         string        `json:"external_id"`
	CorrelationID      uuid.UUID     `json:"correlation_id"`
	SubjectName        string        `json:"subject_name"`
	ConsumerGroup      string        `json:"consumer_group"`
	IncidentClass      IncidentClass `json:"incident_class"`
	PayloadFingerprint string        `json:"payload_fingerprint"`
	ErrorMessage       string        `json:"error_message"`
	Status             string        `json:"status"`
	OccurredAt         time.Time     `json:"occurred_at"`
	CreatedAt          time.Time     `json:"created_at"`
}

type RegisterSchemaInput struct {
	TenantID          string            `json:"tenant_id"`
	SourceService     string            `json:"source_service"`
	ExternalID        string            `json:"external_id"`
	CorrelationID     string            `json:"correlation_id"`
	SubjectName       string            `json:"subject_name"`
	SchemaVersion     int               `json:"schema_version"`
	CompatibilityMode CompatibilityMode `json:"compatibility_mode"`
	SchemaDefinition  string            `json:"schema_definition"`
}

type CheckConsumerReadinessInput struct {
	TenantID      string `json:"tenant_id"`
	SubjectName   string `json:"subject_name"`
	TargetVersion int    `json:"target_version"`
}

type ConsumerReadinessOutput struct {
	IsReady            bool     `json:"is_ready"`
	SubjectName        string   `json:"subject_name"`
	TargetVersion      int      `json:"target_version"`
	UnreadyConsumers   []string `json:"unready_consumers"`
	TotalActiveConsumers int    `json:"total_active_consumers"`
}

type InitiateDualPublishInput struct {
	TenantID       string    `json:"tenant_id"`
	SourceService  string    `json:"source_service"`
	CorrelationID  string    `json:"correlation_id"`
	SubjectName    string    `json:"subject_name"`
	LegacyTopic    string    `json:"legacy_topic"`
	ModernTopic    string    `json:"modern_topic"`
	SunsetDeadline time.Time `json:"sunset_deadline"`
}

type EvaluateDecommissionInput struct {
	TenantID             string `json:"tenant_id"`
	SubjectName          string `json:"subject_name"`
	ActiveConsumerCount  int    `json:"active_consumer_count"`
	ZeroLagDays          int    `json:"zero_lag_days"`
	TelemetryReadCount   int    `json:"telemetry_read_count"`
	DecommissionAttested bool   `json:"decommission_attested"`
}

type DecommissionCheckOutput struct {
	CanDecommission bool     `json:"can_decommission"`
	BlockingReasons []string `json:"blocking_reasons"`
}

type RecordIncidentInput struct {
	TenantID           string        `json:"tenant_id"`
	SourceService      string        `json:"source_service"`
	CorrelationID      string        `json:"correlation_id"`
	SubjectName        string        `json:"subject_name"`
	ConsumerGroup      string        `json:"consumer_group"`
	IncidentClass      IncidentClass `json:"incident_class"`
	PayloadFingerprint string        `json:"payload_fingerprint"`
	ErrorMessage       string        `json:"error_message"`
}


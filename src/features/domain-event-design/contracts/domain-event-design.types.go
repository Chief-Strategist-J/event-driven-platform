package contracts

import (
	"time"

	"github.com/google/uuid"
)

type SemanticsClass string

const (
	SemanticsClassState        SemanticsClass = "STATE"
	SemanticsClassDelta        SemanticsClass = "DELTA"
	SemanticsClassNotification SemanticsClass = "NOTIFICATION"
)

type EventVisibility string

const (
	EventVisibilityPrivate EventVisibility = "PRIVATE"
	EventVisibilityPublic  EventVisibility = "PUBLIC"
)

type PIIClassification string

const (
	PIIClassNone         PIIClassification = "NONE"
	PIIClassConfidential PIIClassification = "CONFIDENTIAL"
	PIIClassRestricted   PIIClassification = "RESTRICTED"
)

type TraceContext struct {
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
}

type CanonicalEnvelope struct {
	EventID            string            `json:"event_id"`
	EventType          string            `json:"event_type"`
	SchemaVersion      string            `json:"schema_version"`
	AggregateType      string            `json:"aggregate_type"`
	AggregateID        string            `json:"aggregate_id"`
	AggregateVersion   int64             `json:"aggregate_version"`
	TenantID           string            `json:"tenant_id"`
	EffectiveAt        time.Time         `json:"effective_at"`
	RecordedAt         time.Time         `json:"recorded_at"`
	CausationID        string            `json:"causation_id"`
	CorrelationID      string            `json:"correlation_id"`
	CausationDepth     int               `json:"causation_depth"`
	TransactionGroupID string            `json:"transaction_group_id"`
	GroupSize          int               `json:"group_size"`
	SemanticsClass     SemanticsClass    `json:"semantics_class"`
	Visibility         EventVisibility   `json:"visibility"`
	PIIClassification  PIIClassification `json:"pii_classification"`
	SourceService      string            `json:"source_service"`
	TraceContext       TraceContext      `json:"trace_context"`
	Payload            any               `json:"payload"`
}

type EventCatalogEntry struct {
	ID                uuid.UUID         `json:"id"`
	PublicID          string            `json:"public_id"`
	TenantID          string            `json:"tenant_id"`
	SourceService     string            `json:"source_service"`
	ExternalID        string            `json:"external_id"`
	CorrelationID     uuid.UUID         `json:"correlation_id"`
	EventType         string            `json:"event_type"`
	OwningAggregate   string            `json:"owning_aggregate"`
	SemanticsClass    SemanticsClass    `json:"semantics_class"`
	Visibility        EventVisibility   `json:"visibility"`
	PIIClassification PIIClassification `json:"pii_classification"`
	SchemaFingerprint string            `json:"schema_fingerprint"`
	IsActive          bool              `json:"is_active"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type AggregateRootVersion struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"public_id"`
	TenantID       string    `json:"tenant_id"`
	SourceService  string    `json:"source_service"`
	ExternalID     string    `json:"external_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	AggregateType  string    `json:"aggregate_type"`
	AggregateID    string    `json:"aggregate_id"`
	CurrentVersion int64     `json:"current_version"`
	IsTerminal     bool      `json:"is_terminal"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type TransactionGroupRecord struct {
	ID                 uuid.UUID `json:"id"`
	PublicID           string    `json:"public_id"`
	TenantID           string    `json:"tenant_id"`
	SourceService      string    `json:"source_service"`
	ExternalID         string    `json:"external_id"`
	CorrelationID      uuid.UUID `json:"correlation_id"`
	TransactionGroupID string    `json:"transaction_group_id"`
	ExpectedGroupSize  int       `json:"expected_group_size"`
	ReceivedCount      int       `json:"received_count"`
	IsComplete         bool      `json:"is_complete"`
	CreatedAt          time.Time `json:"created_at"`
	CompletedAt        time.Time `json:"completed_at"`
}

type CausalChainEntry struct {
	ID                uuid.UUID `json:"id"`
	PublicID          string    `json:"public_id"`
	TenantID          string    `json:"tenant_id"`
	SourceService     string    `json:"source_service"`
	ExternalID        string    `json:"external_id"`
	CorrelationID     uuid.UUID `json:"correlation_id"`
	EventID           string    `json:"event_id"`
	CausationID       string    `json:"causation_id"`
	CausationDepth    int       `json:"causation_depth"`
	CausationPathHash string    `json:"causation_path_hash"`
	CreatedAt         time.Time `json:"created_at"`
}

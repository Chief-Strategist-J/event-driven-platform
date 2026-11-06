package contracts

import (
	"time"
)

type BuildEnvelopeCommand struct {
	TenantID           string            `json:"tenant_id"`
	SourceService      string            `json:"source_service"`
	EventType          string            `json:"event_type"`
	AggregateType      string            `json:"aggregate_type"`
	AggregateID        string            `json:"aggregate_id"`
	ExpectedVersion    int64             `json:"expected_version"`
	VersionIncrement   int64             `json:"version_increment"`
	EffectiveAt        time.Time         `json:"effective_at"`
	CausationID        string            `json:"causation_id"`
	CorrelationID      string            `json:"correlation_id"`
	CausationDepth     int               `json:"causation_depth"`
	TransactionGroupID string            `json:"transaction_group_id"`
	GroupSize          int               `json:"group_size"`
	TraceContext       TraceContext      `json:"trace_context"`
	Payload            any               `json:"payload"`
}

type BuildEnvelopeResult struct {
	Envelope CanonicalEnvelope `json:"envelope"`
	Status   string            `json:"status"`
}

type RegisterCatalogEventCommand struct {
	TenantID          string            `json:"tenant_id"`
	SourceService     string            `json:"source_service"`
	ExternalID        string            `json:"external_id"`
	CorrelationID     string            `json:"correlation_id"`
	EventType         string            `json:"event_type"`
	OwningAggregate   string            `json:"owning_aggregate"`
	SemanticsClass    SemanticsClass    `json:"semantics_class"`
	Visibility        EventVisibility   `json:"visibility"`
	PIIClassification PIIClassification `json:"pii_classification"`
	SchemaFingerprint string            `json:"schema_fingerprint"`
}

type ReserveAggregateVersionCommand struct {
	TenantID         string `json:"tenant_id"`
	SourceService    string `json:"source_service"`
	ExternalID       string `json:"external_id"`
	CorrelationID    string `json:"correlation_id"`
	AggregateType    string `json:"aggregate_type"`
	AggregateID      string `json:"aggregate_id"`
	ExpectedVersion  int64  `json:"expected_version"`
	VersionIncrement int64  `json:"version_increment"`
	MarkTerminal     bool   `json:"mark_terminal"`
}

type ReserveAggregateVersionResult struct {
	AggregateType   string `json:"aggregate_type"`
	AggregateID     string `json:"aggregate_id"`
	PreviousVersion int64  `json:"previous_version"`
	ReservedVersion int64  `json:"reserved_version"`
	IsTerminal      bool   `json:"is_terminal"`
}

type VerifyCausalChainCommand struct {
	TenantID       string `json:"tenant_id"`
	SourceService  string `json:"source_service"`
	ExternalID     string `json:"external_id"`
	CorrelationID  string `json:"correlation_id"`
	EventID        string `json:"event_id"`
	CausationID    string `json:"causation_id"`
	CausationDepth int    `json:"causation_depth"`
	EventType      string `json:"event_type"`
	AggregateID    string `json:"aggregate_id"`
}

type VerifyCausalChainResult struct {
	EventID        string `json:"event_id"`
	IsValid        bool   `json:"is_valid"`
	CausationDepth int    `json:"causation_depth"`
	PathHash       string `json:"path_hash"`
	LoopDetected   bool   `json:"loop_detected"`
}

type RegisterTransactionGroupPartCommand struct {
	TenantID           string `json:"tenant_id"`
	SourceService      string `json:"source_service"`
	ExternalID         string `json:"external_id"`
	CorrelationID      string `json:"correlation_id"`
	TransactionGroupID string `json:"transaction_group_id"`
	ExpectedGroupSize  int    `json:"expected_group_size"`
}

type RegisterTransactionGroupPartResult struct {
	TransactionGroupID string `json:"transaction_group_id"`
	ExpectedGroupSize  int    `json:"expected_group_size"`
	ReceivedCount      int    `json:"received_count"`
	IsComplete         bool   `json:"is_complete"`
}

package contracts

import (
	"time"

	"github.com/google/uuid"
)

type ViewClass string

const (
	ViewClassState  ViewClass = "V_STATE"
	ViewClassAgg    ViewClass = "V_AGG"
	ViewClassJoin   ViewClass = "V_JOIN"
	ViewClassSearch ViewClass = "V_SEARCH"
	ViewClassKV     ViewClass = "V_KV"
	ViewClassReport ViewClass = "V_REPORT"
)

type StorageTarget string

const (
	StorageTargetPostgres   StorageTarget = "POSTGRES"
	StorageTargetRedis      StorageTarget = "REDIS"
	StorageTargetOpenSearch StorageTarget = "OPENSEARCH"
)

type ConsistencyMode string

const (
	ConsistencyModeEventual     ConsistencyMode = "EVENTUAL"
	ConsistencyModeTokenBounded ConsistencyMode = "TOKEN_BOUNDED"
	ConsistencyModeStrictSerial ConsistencyMode = "STRICT_SERIAL"
)

type ViewStatus string

const (
	ViewStatusOnline         ViewStatus = "ONLINE"
	ViewStatusRebuilding     ViewStatus = "REBUILDING"
	ViewStatusDegraded       ViewStatus = "DEGRADED"
	ViewStatusDecommissioned ViewStatus = "DECOMMISSIONED"
)

type RebuildPhase string

const (
	RebuildPhaseInitiated          RebuildPhase = "INITIATED"
	RebuildPhaseHistoricalReplay   RebuildPhase = "HISTORICAL_REPLAY"
	RebuildPhaseStreamCatchup      RebuildPhase = "STREAM_CATCHUP"
	RebuildPhasePromoted           RebuildPhase = "PROMOTED"
	RebuildPhaseDecommissioned     RebuildPhase = "DECOMMISSIONED_OLD"
)

type ProjectionDecision string

const (
	DecisionApply             ProjectionDecision = "APPLY"
	DecisionDuplicate         ProjectionDecision = "DUPLICATE_NOOP"
	DecisionStaleDrop         ProjectionDecision = "STALE_DROP"
	DecisionGapHold           ProjectionDecision = "GAP_HOLD"
	DecisionIntegrityConflict ProjectionDecision = "INTEGRITY_CONFLICT"
)

type PeriodStatus string

const (
	PeriodStatusOpen   PeriodStatus = "OPEN"
	PeriodStatusFrozen PeriodStatus = "FROZEN"
	PeriodStatusClosed PeriodStatus = "CLOSED"
)

type AggContributionRecord struct {
	ID            uuid.UUID `json:"id"`
	PublicID      string    `json:"public_id"`
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID uuid.UUID `json:"correlation_id"`
	ViewName      string    `json:"view_name"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	SourceEventID uuid.UUID `json:"source_event_id"`
	SourceSeq     int64     `json:"source_seq"`
	DeltaAmount   float64   `json:"delta_amount"`
	IsReversal    bool      `json:"is_reversal"`
	Version       int64     `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
}

type PeriodLedgerRecord struct {
	ID                 uuid.UUID    `json:"id"`
	PublicID           string       `json:"public_id"`
	TenantID           string       `json:"tenant_id"`
	SourceService      string       `json:"source_service"`
	ExternalID         string       `json:"external_id"`
	CorrelationID      uuid.UUID    `json:"correlation_id"`
	ViewName           string       `json:"view_name"`
	PeriodKey          string       `json:"period_key"`
	PeriodStatus       PeriodStatus `json:"period_status"`
	BaseAmount         float64      `json:"base_amount"`
	AdjustmentAmount   float64      `json:"adjustment_amount"`
	ClosingPositionLSN string       `json:"closing_position_lsn"`
	Version            int64        `json:"version"`
	CreatedAt          time.Time    `json:"created_at"`
	UpdatedAt          time.Time    `json:"updated_at"`
}

type ViewRegistryRecord struct {
	ID              uuid.UUID       `json:"id"`
	PublicID        string          `json:"public_id"`
	TenantID        string          `json:"tenant_id"`
	SourceService   string          `json:"source_service"`
	ExternalID      string          `json:"external_id"`
	CorrelationID   uuid.UUID       `json:"correlation_id"`
	ViewName        string          `json:"view_name"`
	ViewClass       ViewClass       `json:"view_class"`
	PrimaryStore    StorageTarget   `json:"primary_store"`
	ActiveTableName string          `json:"active_table_name"`
	FreshnessSLOMs  int64           `json:"freshness_slo_ms"`
	ConsistencyMode ConsistencyMode `json:"consistency_mode"`
	Status          ViewStatus      `json:"status"`
	Version         int64           `json:"version"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type ProjectionStateRecord struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"public_id"`
	TenantID       string    `json:"tenant_id"`
	SourceService  string    `json:"source_service"`
	ExternalID     string    `json:"external_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	ViewName       string    `json:"view_name"`
	AggregateType  string    `json:"aggregate_type"`
	AggregateID    string    `json:"aggregate_id"`
	CurrentVersion int64     `json:"current_version"`
	LastEventID    uuid.UUID `json:"last_event_id"`
	PayloadHash    string    `json:"payload_hash"`
	MaterializedAt time.Time `json:"materialized_at"`
	Version        int64     `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type HeldEventRecord struct {
	ID            uuid.UUID `json:"id"`
	PublicID      string    `json:"public_id"`
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID uuid.UUID `json:"correlation_id"`
	ViewName      string    `json:"view_name"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	ExpectedSeq   int64     `json:"expected_seq"`
	ReceivedSeq   int64     `json:"received_seq"`
	EventPayload  string    `json:"event_payload"`
	HeldUntil     time.Time `json:"held_until"`
	CreatedAt     time.Time `json:"created_at"`
}

type RebuildLogRecord struct {
	ID                 uuid.UUID     `json:"id"`
	PublicID           string        `json:"public_id"`
	TenantID           string        `json:"tenant_id"`
	SourceService      string        `json:"source_service"`
	ExternalID         string        `json:"external_id"`
	CorrelationID      uuid.UUID     `json:"correlation_id"`
	RebuildID          string        `json:"rebuild_id"`
	ViewName           string        `json:"view_name"`
	SourceStore        StorageTarget `json:"source_store"`
	TargetShadowTable  string        `json:"target_shadow_table"`
	TargetLSN          string        `json:"target_lsn"`
	CurrentCursorSeq   int64         `json:"current_cursor_seq"`
	Phase              RebuildPhase  `json:"phase"`
	Version            int64         `json:"version"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
}

type RegisterViewInput struct {
	TenantID        string          `json:"tenant_id"`
	SourceService   string          `json:"source_service"`
	ExternalID      string          `json:"external_id"`
	CorrelationID   string          `json:"correlation_id"`
	ViewName        string          `json:"view_name"`
	ViewClass       ViewClass       `json:"view_class"`
	PrimaryStore    StorageTarget   `json:"primary_store"`
	ActiveTableName string          `json:"active_table_name"`
	FreshnessSLOMs  int64           `json:"freshness_slo_ms"`
	ConsistencyMode ConsistencyMode `json:"consistency_mode"`
}

type ApplyProjectionInput struct {
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID string    `json:"correlation_id"`
	ViewName      string    `json:"view_name"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	AggregateSeq  int64     `json:"aggregate_seq"`
	EventID       string    `json:"event_id"`
	PayloadRaw    string    `json:"payload_raw"`
	OccurredAt    time.Time `json:"occurred_at"`
}

type InitiateRebuildInput struct {
	TenantID          string        `json:"tenant_id"`
	SourceService     string        `json:"source_service"`
	ExternalID        string        `json:"external_id"`
	CorrelationID     string        `json:"correlation_id"`
	RebuildID         string        `json:"rebuild_id"`
	ViewName          string        `json:"view_name"`
	SourceStore       StorageTarget `json:"source_store"`
	TargetShadowTable string        `json:"target_shadow_table"`
	TargetLSN         string        `json:"target_lsn"`
}

type AdvanceRebuildInput struct {
	TenantID      string       `json:"tenant_id"`
	RebuildID     string       `json:"rebuild_id"`
	ExpectedPhase RebuildPhase `json:"expected_phase"`
	TargetPhase   RebuildPhase `json:"target_phase"`
	CorrelationID string       `json:"correlation_id"`
}

type ProjectionEvaluation struct {
	Decision       ProjectionDecision `json:"decision"`
	ExpectedSeq    int64              `json:"expected_seq"`
	IncomingSeq    int64              `json:"incoming_seq"`
	PayloadHash    string             `json:"payload_hash"`
	GapSize        int64              `json:"gap_size"`
	IsQuarantined  bool               `json:"is_quarantined"`
}

type ApplyContributionInput struct {
	TenantID      string  `json:"tenant_id"`
	SourceService string  `json:"source_service"`
	ExternalID    string  `json:"external_id"`
	CorrelationID string  `json:"correlation_id"`
	ViewName      string  `json:"view_name"`
	AggregateType string  `json:"aggregate_type"`
	AggregateID   string  `json:"aggregate_id"`
	SourceEventID string  `json:"source_event_id"`
	SourceSeq     int64   `json:"source_seq"`
	DeltaAmount   float64 `json:"delta_amount"`
	IsReversal    bool    `json:"is_reversal"`
}

type RecordPeriodEventInput struct {
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID string    `json:"correlation_id"`
	ViewName      string    `json:"view_name"`
	PeriodKey     string    `json:"period_key"`
	Amount        float64   `json:"amount"`
	EventTime     time.Time `json:"event_time"`
}

type QueryViewInput struct {
	TenantID         string `json:"tenant_id"`
	ViewName         string `json:"view_name"`
	AggregateType    string `json:"aggregate_type"`
	AggregateID      string `json:"aggregate_id"`
	ConsistencyToken string `json:"consistency_token"`
	TimeoutBudgetMs  int64  `json:"timeout_budget_ms"`
}

type QueryViewOutput struct {
	ViewName       string                 `json:"view_name"`
	AggregateID    string                 `json:"aggregate_id"`
	CurrentVersion int64                  `json:"current_version"`
	IsSatisfied    bool                   `json:"is_satisfied"`
	IsPending      bool                   `json:"is_pending"`
	State          *ProjectionStateRecord `json:"state,omitempty"`
}


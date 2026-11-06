package contracts

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantID     = errors.New("tenant_id must not be empty or whitespace")
	ErrInvalidSourceService = errors.New("source_service must not be empty or whitespace")
	ErrInvalidExternalID   = errors.New("external_id must not be empty or whitespace")
	ErrInvalidFlowName     = errors.New("flow_name must not be empty or whitespace")
	ErrInvalidHopCode      = errors.New("hop_code must be H1_OUTBOX, H2_RELAY, H3_KAFKA, H4_CONSUMER, H5_AUTH_VIEW, or H6_DERIVED_STORE")
	ErrInvalidTier         = errors.New("tier must be T0, T1, or T2")
	ErrInvalidStatus       = errors.New("status must be OK, LAGGING, STALLED, STOPPED, PAUSED, or UNKNOWN")
	ErrInvalidPauseReason  = errors.New("pause_reason must be NONE, BREAKER_OPEN, BACKPRESSURE, OPERATOR_HALT, REBALANCE, or CORRUPT_HEAD")
	ErrInvalidRunbookURL   = errors.New("runbook_url must not be empty or whitespace")
	ErrNegativeLag         = errors.New("lag_seconds must not be negative")
	ErrNegativeHeldAge     = errors.New("oldest_held_age_seconds must not be negative")
	ErrInvalidIncidentID   = errors.New("incident_id must not be empty or whitespace")
	ErrInvalidSeverity     = errors.New("severity must be SEV0, SEV1, SEV2, or INFO")
	ErrCustomerPayloadDetected = errors.New("OD-07 violation: customer payload or PII detected in observability telemetry")

	HopPublicIDRegex = regexp.MustCompile(`^hop_[0-9a-z]{16,26}$`)
	ItlPublicIDRegex = regexp.MustCompile(`^itl_[0-9a-z]{16,26}$`)
)

type HopCode string

const (
	HopH1Outbox       HopCode = "H1_OUTBOX"
	HopH2Relay        HopCode = "H2_RELAY"
	HopH3Kafka        HopCode = "H3_KAFKA"
	HopH4Consumer     HopCode = "H4_CONSUMER"
	HopH5AuthView     HopCode = "H5_AUTH_VIEW"
	HopH6DerivedStore HopCode = "H6_DERIVED_STORE"
)

type FlowTier string

const (
	Tier0 FlowTier = "T0"
	Tier1 FlowTier = "T1"
	Tier2 FlowTier = "T2"
)

type HopStatus string

const (
	StatusOk      HopStatus = "OK"
	StatusLagging HopStatus = "LAGGING"
	StatusStalled HopStatus = "STALLED"
	StatusStopped HopStatus = "STOPPED"
	StatusPaused  HopStatus = "PAUSED"
	StatusUnknown HopStatus = "UNKNOWN"
)

type PauseReason string

const (
	PauseReasonNone         PauseReason = "NONE"
	PauseReasonBreakerOpen  PauseReason = "BREAKER_OPEN"
	PauseReasonBackpressure PauseReason = "BACKPRESSURE"
	PauseReasonOperatorHalt PauseReason = "OPERATOR_HALT"
	PauseReasonRebalance    PauseReason = "REBALANCE"
	PauseReasonCorruptHead  PauseReason = "CORRUPT_HEAD"
)

type IncidentSeverity string

const (
	Sev0 IncidentSeverity = "SEV0"
	Sev1 IncidentSeverity = "SEV1"
	Sev2 IncidentSeverity = "SEV2"
	SevInfo IncidentSeverity = "INFO"
)

type PipelineHopRecord struct {
	ID                   uuid.UUID   `json:"id"`
	PublicID             string      `json:"public_id"`
	TenantID             string      `json:"tenant_id"`
	SourceService        string      `json:"source_service"`
	ExternalID           string      `json:"external_id"`
	CorrelationID        uuid.UUID   `json:"correlation_id"`
	FlowName             string      `json:"flow_name"`
	HopCode              HopCode     `json:"hop_code"`
	Tier                 FlowTier    `json:"tier"`
	Status               HopStatus   `json:"status"`
	PauseReason          PauseReason `json:"pause_reason"`
	LagSeconds           float64     `json:"lag_seconds"`
	OldestHeldAgeSeconds float64     `json:"oldest_held_age_seconds"`
	RunbookURL           string      `json:"runbook_url"`
	LastReportedAt       time.Time   `json:"last_reported_at"`
	CreatedAt            time.Time   `json:"created_at"`
	UpdatedAt            time.Time   `json:"updated_at"`
}

type IncidentTriageRecord struct {
	ID             uuid.UUID        `json:"id"`
	PublicID       string           `json:"public_id"`
	TenantID       string           `json:"tenant_id"`
	SourceService  string           `json:"source_service"`
	ExternalID     string           `json:"external_id"`
	CorrelationID  uuid.UUID        `json:"correlation_id"`
	IncidentID     string           `json:"incident_id"`
	FlowName       string           `json:"flow_name"`
	LocalizedHop   string           `json:"localized_hop"`
	Severity       IncidentSeverity `json:"severity"`
	TriageAction   string           `json:"triage_action"`
	CCTLActionLink string           `json:"c_ctl_action_link"`
	TriagedBy      string           `json:"triaged_by"`
	TriagedAt      time.Time        `json:"triaged_at"`
}

type ReportHopTelemetryInput struct {
	TenantID             string      `json:"tenant_id"`
	SourceService        string      `json:"source_service"`
	ExternalID           string      `json:"external_id"`
	CorrelationID        uuid.UUID   `json:"correlation_id"`
	FlowName             string      `json:"flow_name"`
	HopCode              HopCode     `json:"hop_code"`
	Tier                 FlowTier    `json:"tier"`
	Status               HopStatus   `json:"status"`
	PauseReason          PauseReason `json:"pause_reason"`
	LagSeconds           float64     `json:"lag_seconds"`
	OldestHeldAgeSeconds float64     `json:"oldest_held_age_seconds"`
	RunbookURL           string      `json:"runbook_url"`
}

type TriageFlowInput struct {
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID uuid.UUID `json:"correlation_id"`
	IncidentID    string    `json:"incident_id"`
	FlowName      string    `json:"flow_name"`
	TriagedBy     string    `json:"triaged_by"`
}

type TriageFlowOutput struct {
	IncidentID       string           `json:"incident_id"`
	FlowName         string           `json:"flow_name"`
	LocalizedHop     HopCode          `json:"localized_hop"`
	HopStatus        HopStatus        `json:"hop_status"`
	PauseReason      PauseReason      `json:"pause_reason"`
	Severity         IncidentSeverity `json:"severity"`
	TriageAction     string           `json:"triage_action"`
	RunbookURL       string           `json:"runbook_url"`
	CCTLActionLink   string           `json:"c_ctl_action_link"`
	TriagedAt        string           `json:"triaged_at"`
}

type EventOperationsDashboardRepository interface {
	SaveHopTelemetry(ctx context.Context, hop *PipelineHopRecord) error
	GetHopsByFlow(ctx context.Context, tenantID, flowName string) ([]*PipelineHopRecord, error)
	GetHopByPublicID(ctx context.Context, publicID string) (*PipelineHopRecord, error)
	SaveTriageLog(ctx context.Context, triage *IncidentTriageRecord) error
	GetTriageLogByPublicID(ctx context.Context, publicID string) (*IncidentTriageRecord, error)
}

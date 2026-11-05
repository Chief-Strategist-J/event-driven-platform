package contracts

import "time"

type BackpressureState string

const (
	StateNormal     BackpressureState = "NORMAL"
	StateSaturated  BackpressureState = "SATURATED"
	StateRecovering BackpressureState = "RECOVERING"
)

type BackpressureEvent string

const (
	EventSaturationDetected BackpressureEvent = "SATURATION_DETECTED"
	EventRecoveryStarted    BackpressureEvent = "RECOVERY_STARTED"
	EventRecoveryConfirmed  BackpressureEvent = "RECOVERY_CONFIRMED"
)

type PauseReason string

const (
	PauseReasonDBLatency     PauseReason = "DB_LATENCY"
	PauseReasonMemoryBudget  PauseReason = "MEMORY_BUDGET"
	PauseReasonQueueFull     PauseReason = "LANE_QUEUE_FULL"
	PauseReasonRejections    PauseReason = "DEPENDENCY_REJECTIONS"
)

type OrderingClass string

const (
	OrderingStrict OrderingClass = "O-STRICT"
	OrderingState  OrderingClass = "O-STATE"
	OrderingNone   OrderingClass = "O-NONE"
)

type PriorityTier string

const (
	TierT0 PriorityTier = "T0"
	TierT1 PriorityTier = "T1"
	TierT2 PriorityTier = "T2"
)

type SaturationSignal struct {
	HandlerID          string        `json:"handlerId"`
	DependencyID       string        `json:"dependencyId"`
	QueueingDelayMs    float64       `json:"queueingDelayMs"`
	LatencyP99Ms       float64       `json:"latencyP99Ms"`
	BaselineLatencyMs  float64       `json:"baselineLatencyMs"`
	InFlight           int64         `json:"inFlight"`
	ConcurrencyLimit   int64         `json:"concurrencyLimit"`
	LiveHeapBytes      int64         `json:"liveHeapBytes"`
	MemoryLimitBytes   int64         `json:"memoryLimitBytes"`
	RejectionCount     int64         `json:"rejectionCount"`
	MeasuredAt         time.Time     `json:"measuredAt"`
}

type BackpressureRecord struct {
	ID               string            `json:"id"`
	PublicID         string            `json:"publicId"`
	TenantID         string            `json:"tenantId"`
	SourceService    string            `json:"sourceService"`
	ExternalID       string            `json:"externalId"`
	CorrelationID    string            `json:"correlationId"`
	HandlerID        string            `json:"handlerId"`
	DependencyID     string            `json:"dependencyId"`
	State            BackpressureState `json:"state"`
	PauseReason      PauseReason       `json:"pauseReason"`
	ConcurrencyLimit int64             `json:"concurrencyLimit"`
	PausedAt         *time.Time        `json:"pausedAt,omitempty"`
	ResumedAt        *time.Time        `json:"resumedAt,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

type BackpressureAuditRecord struct {
	ID            string            `json:"id"`
	PublicID      string            `json:"publicId"`
	TenantID      string            `json:"tenantId"`
	SourceService string            `json:"sourceService"`
	ExternalID    string            `json:"externalId"`
	CorrelationID string            `json:"correlationId"`
	HandlerID     string            `json:"handlerId"`
	DependencyID  string            `json:"dependencyId"`
	FromState     BackpressureState `json:"fromState"`
	ToState       BackpressureState `json:"toState"`
	Event         BackpressureEvent `json:"event"`
	PauseReason   PauseReason       `json:"pauseReason"`
	Reason        string            `json:"reason"`
	CreatedAt     time.Time         `json:"createdAt"`
}

type ReportSaturationInput struct {
	TenantID      string           `json:"tenantId"`
	SourceService string           `json:"sourceService"`
	ExternalID    string           `json:"externalId"`
	CorrelationID string           `json:"correlationId"`
	Signal        SaturationSignal `json:"signal"`
	PauseReason   PauseReason      `json:"pauseReason"`
}

type ReportRecoveryInput struct {
	TenantID      string      `json:"tenantId"`
	SourceService string      `json:"sourceService"`
	ExternalID    string      `json:"externalId"`
	CorrelationID string      `json:"correlationId"`
	HandlerID     string      `json:"handlerId"`
	DependencyID  string      `json:"dependencyId"`
}

type GetBackpressureStateInput struct {
	TenantID     string `json:"tenantId"`
	HandlerID    string `json:"handlerId"`
	DependencyID string `json:"dependencyId"`
}

type BackpressureStateResult struct {
	HandlerID        string            `json:"handlerId"`
	DependencyID     string            `json:"dependencyId"`
	State            BackpressureState `json:"state"`
	ConcurrencyLimit int64             `json:"concurrencyLimit"`
	PauseReason      PauseReason       `json:"pauseReason,omitempty"`
	PausedAt         *time.Time        `json:"pausedAt,omitempty"`
	PauseDurationMs  int64             `json:"pauseDurationMs,omitempty"`
}

type SaturationEvalResult struct {
	IsSaturated      bool        `json:"isSaturated"`
	SaturationReason PauseReason `json:"saturationReason,omitempty"`
	Recommendation   string      `json:"recommendation"`
}


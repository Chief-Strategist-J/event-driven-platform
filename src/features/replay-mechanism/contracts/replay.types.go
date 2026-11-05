package contracts

import (
	"time"
)

type ReplayRunMode string

const (
	ModeRebuild  ReplayRunMode = "REBUILD"
	ModeHydrate  ReplayRunMode = "HYDRATE"
	ModeForensic ReplayRunMode = "FORENSIC"
	ModeRecover  ReplayRunMode = "RECOVER"
	ModeRepair   ReplayRunMode = "REPAIR"
	ModeLive     ReplayRunMode = "LIVE"
)

type ReplayState string

const (
	StateInitializing ReplayState = "INITIALIZING"
	StateStreaming    ReplayState = "STREAMING"
	StateFollowing    ReplayState = "FOLLOWING"
	StateCaughtUp     ReplayState = "CAUGHT_UP"
	StatePromoted     ReplayState = "PROMOTED"
	StateCompleted    ReplayState = "COMPLETED"
	StateAborted      ReplayState = "ABORTED"
)

type ReplayEvent string

const (
	EventStartStreaming ReplayEvent = "START_STREAMING"
	EventFollowLive     ReplayEvent = "FOLLOW_LIVE"
	EventCatchUpReached ReplayEvent = "CATCH_UP_REACHED"
	EventPromoteWriter  ReplayEvent = "PROMOTE_WRITER"
	EventCompleteCutover ReplayEvent = "COMPLETE_CUTOVER"
	EventAbortRun       ReplayEvent = "ABORT_RUN"
)

type EffectType string

const (
	EffectExternalPayment      EffectType = "EXTERNAL_PAYMENT"
	EffectExternalNotification EffectType = "EXTERNAL_NOTIFICATION"
	EffectOutboxEvent          EffectType = "OUTBOX_EVENT"
	EffectSearchIndex          EffectType = "SEARCH_INDEX"
	EffectCacheWrite           EffectType = "CACHE_WRITE"
)

type EffectMode string

const (
	EffectModeSuppress  EffectMode = "SUPPRESS"
	EffectModeAllow     EffectMode = "ALLOW"
	EffectModeReconcile EffectMode = "RECONCILE"
)

type EffectDecision string

const (
	DecisionSuppressed EffectDecision = "SUPPRESSED"
	DecisionExecuted   EffectDecision = "EXECUTED"
	DecisionReconciled EffectDecision = "RECONCILED"
)

type ReplayScope string

const (
	ScopeGlobalTopic  ReplayScope = "GLOBAL_TOPIC"
	ScopeTimeWindow   ReplayScope = "TIME_WINDOW"
	ScopeAggregateSet ReplayScope = "AGGREGATE_SET"
)

type ReplayRunRecord struct {
	ID                  string        `json:"id"`
	PublicID            string        `json:"publicId"`
	TenantID            string        `json:"tenantId"`
	SourceService       string        `json:"sourceService"`
	ExternalID          string        `json:"externalId"`
	CorrelationID       string        `json:"correlationId"`
	TargetProjection    string        `json:"targetProjection"`
	Mode                ReplayRunMode `json:"mode"`
	State               ReplayState   `json:"state"`
	Scope               ReplayScope   `json:"scope"`
	StartOffset         int64         `json:"startOffset"`
	CurrentOffset       int64         `json:"currentOffset"`
	EndOffset           int64         `json:"endOffset"`
	ReplayConsumerGroup string        `json:"replayConsumerGroup"`
	Version             int64         `json:"version"`
	CreatedAt           time.Time     `json:"createdAt"`
	UpdatedAt           time.Time     `json:"updatedAt"`
}

type ReplayEffectRecord struct {
	ID             string         `json:"id"`
	PublicID       string         `json:"publicId"`
	TenantID       string         `json:"tenantId"`
	SourceService  string         `json:"sourceService"`
	ExternalID     string         `json:"externalId"`
	CorrelationID  string         `json:"correlationId"`
	RunID          string         `json:"runId"`
	EffectType     EffectType     `json:"effectType"`
	EffectMode     EffectMode     `json:"effectMode"`
	Decision       EffectDecision `json:"decision"`
	IdempotencyKey string         `json:"idempotencyKey"`
	PayloadDigest  string         `json:"payloadDigest"`
	CreatedAt      time.Time      `json:"createdAt"`
}

type ReplayAuditRecord struct {
	ID            string      `json:"id"`
	PublicID      string      `json:"publicId"`
	TenantID      string      `json:"tenantId"`
	SourceService string      `json:"sourceService"`
	ExternalID    string      `json:"externalId"`
	CorrelationID string      `json:"correlationId"`
	RunID         string      `json:"runId"`
	FromState     ReplayState `json:"fromState"`
	ToState       ReplayState `json:"toState"`
	Event         ReplayEvent `json:"event"`
	Reason        string      `json:"reason"`
	CreatedAt     time.Time   `json:"createdAt"`
}

type CreateReplayRunInput struct {
	TenantID         string        `json:"tenantId"`
	SourceService    string        `json:"sourceService"`
	ExternalID       string        `json:"externalId"`
	CorrelationID    string        `json:"correlationId"`
	TargetProjection string        `json:"targetProjection"`
	Mode             ReplayRunMode `json:"mode"`
	Scope            ReplayScope   `json:"scope"`
	StartOffset      int64         `json:"startOffset"`
	EndOffset        int64         `json:"endOffset"`
}

type AdvanceReplayInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	RunID         string `json:"runId"`
	CurrentOffset int64  `json:"currentOffset"`
}

type PromoteReplayInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	RunID         string `json:"runId"`
	Reason        string `json:"reason"`
}

type CompleteCutoverInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	RunID         string `json:"runId"`
	Reason        string `json:"reason"`
}

type AbortReplayInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	RunID         string `json:"runId"`
	Reason        string `json:"reason"`
}

type EvaluateEffectInput struct {
	TenantID       string     `json:"tenantId"`
	SourceService  string     `json:"sourceService"`
	ExternalID     string     `json:"externalId"`
	CorrelationID  string     `json:"correlationId"`
	RunID          string     `json:"runId"`
	EffectType     EffectType `json:"effectType"`
	IdempotencyKey string     `json:"idempotencyKey"`
	PayloadDigest  string     `json:"payloadDigest"`
}

type EffectEvaluationResult struct {
	Decision   EffectDecision `json:"decision"`
	EffectMode EffectMode     `json:"effectMode"`
	Reason     string         `json:"reason"`
}

type GetReplayRunInput struct {
	TenantID string `json:"tenantId"`
	RunID    string `json:"runId"`
}


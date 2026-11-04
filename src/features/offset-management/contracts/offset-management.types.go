package contracts

import (
	"time"
)

type OffsetCommitStrategy string

const (
	CommitStrategyAuto   OffsetCommitStrategy = "AUTO"
	CommitStrategyManual OffsetCommitStrategy = "MANUAL_COMMIT_AFTER_PROCESS"
)

type LagSeverity string

const (
	LagSeverityHealthy LagSeverity = "HEALTHY"
	LagSeveritySurge   LagSeverity = "SURGE"
	LagSeverityStalled LagSeverity = "STALLED"
)

type OffsetOperationType string

const (
	OperationCommit      OffsetOperationType = "COMMIT"
	OperationRewind      OffsetOperationType = "REWIND"
	OperationFastForward OffsetOperationType = "FAST_FORWARD"
	OperationReset       OffsetOperationType = "RESET"
)

type ConsumerStoredOffsetRecord struct {
	ID            string    `json:"id"`
	PublicID      string    `json:"publicId"`
	TenantID      string    `json:"tenantId"`
	SourceService string    `json:"sourceService"`
	ExternalID    string    `json:"externalId"`
	CorrelationID string    `json:"correlationId"`
	GroupID       string    `json:"groupId"`
	Topic         string    `json:"topic"`
	Partition     int32     `json:"partition"`
	StoredOffset  int64     `json:"storedOffset"`
	LogEndOffset  int64     `json:"logEndOffset"`
	Lag           int64     `json:"lag"`
	CommitEpoch   int64     `json:"commitEpoch"`
	Metadata      []byte    `json:"metadata"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type ConsumerOffsetAuditLogRecord struct {
	ID             string              `json:"id"`
	PublicID       string              `json:"publicId"`
	TenantID       string              `json:"tenantId"`
	SourceService  string              `json:"sourceService"`
	ExternalID     string              `json:"externalId"`
	CorrelationID  string              `json:"correlationId"`
	GroupID        string              `json:"groupId"`
	Topic          string              `json:"topic"`
	Partition      int32               `json:"partition"`
	OperationType  OffsetOperationType `json:"operationType"`
	PreviousOffset int64               `json:"previousOffset"`
	TargetOffset   int64               `json:"targetOffset"`
	Reason         string              `json:"reason"`
	CreatedAt      time.Time           `json:"createdAt"`
}

type CommitOffsetInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	GroupID       string `json:"groupId"`
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	Offset        int64  `json:"offset"`
	LogEndOffset  int64  `json:"logEndOffset"`
	Metadata      []byte `json:"metadata,omitempty"`
}

type RewindOffsetInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	GroupID       string `json:"groupId"`
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	TargetOffset  int64  `json:"targetOffset"`
	Reason        string `json:"reason"`
}

type FastForwardOffsetInput struct {
	TenantID      string `json:"tenantId"`
	SourceService string `json:"sourceService"`
	ExternalID    string `json:"externalId"`
	CorrelationID string `json:"correlationId"`
	GroupID       string `json:"groupId"`
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	ShiftBy       int64  `json:"shiftBy"`
	Reason        string `json:"reason"`
}

type LagEvaluationResult struct {
	GroupID      string      `json:"groupId"`
	Topic        string      `json:"topic"`
	Partition    int32       `json:"partition"`
	StoredOffset int64       `json:"storedOffset"`
	LogEndOffset int64       `json:"logEndOffset"`
	Lag          int64       `json:"lag"`
	Severity     LagSeverity `json:"severity"`
	Action       string      `json:"action"`
}

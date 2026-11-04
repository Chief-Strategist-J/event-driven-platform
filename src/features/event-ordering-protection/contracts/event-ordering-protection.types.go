package contracts

import (
	"encoding/json"
	"time"
)

type SequenceAction string

const (
	ActionApply             SequenceAction = "APPLY"
	ActionDuplicateSkip     SequenceAction = "DUPLICATE_SKIP"
	ActionStaleSkip         SequenceAction = "STALE_SKIP"
	ActionGapHeld           SequenceAction = "GAP_HELD"
	ActionIntegrityMismatch SequenceAction = "INTEGRITY_MISMATCH"
	ActionFencedEpochReject SequenceAction = "FENCED_EPOCH_REJECT"
)

type GapClass string

const (
	GapClassTransient GapClass = "TRANSIENT_POSSIBLE"
	GapClassDefect    GapClass = "DEFECT"
	GapClassSystemic  GapClass = "SYSTEMIC"
)

type HoldReason string

const (
	HoldReasonGap           HoldReason = "GAP"
	HoldReasonParkedFailure HoldReason = "PARKED_FAILURE"
	HoldReasonIntegrity     HoldReason = "INTEGRITY"
	HoldReasonResyncing     HoldReason = "RESYNCING"
)

type IngestEventSequenceInput struct {
	TenantID         string          `json:"tenantId"`
	SourceService    string          `json:"sourceService"`
	ExternalID       string          `json:"externalId"`
	CorrelationID    string          `json:"correlationId,omitempty"`
	ConsumerGroup    string          `json:"consumerGroup"`
	HandlerID        string          `json:"handlerId"`
	AggregateType    string          `json:"aggregateType"`
	AggregateID      string          `json:"aggregateId"`
	AggregateVersion int64           `json:"aggregateVersion"`
	EventID          string          `json:"eventId"`
	EventType        string          `json:"eventType"`
	Payload          json.RawMessage `json:"payload"`
	Headers          json.RawMessage `json:"headers,omitempty"`
	SourceOffset     int64           `json:"sourceOffset"`
	OwnerEpoch       int64           `json:"ownerEpoch"`
}

type SequenceEvaluationResult struct {
	Action           SequenceAction `json:"action"`
	CurrentVersion   int64          `json:"currentVersion"`
	IncomingVersion  int64          `json:"incomingVersion"`
	GapClass         GapClass       `json:"gapClass,omitempty"`
	MissingVersions  []int64        `json:"missingVersions,omitempty"`
	IsHeld           bool           `json:"isHeld"`
	HoldReason       HoldReason     `json:"holdReason,omitempty"`
	RequiresDLQAlert bool           `json:"requiresDlqAlert"`
}

type DrainHeldEventsInput struct {
	TenantID      string `json:"tenantId"`
	ConsumerGroup string `json:"consumerGroup"`
	HandlerID     string `json:"handlerId"`
	AggregateType string `json:"aggregateType"`
	AggregateID   string `json:"aggregateId"`
	OwnerEpoch    int64  `json:"ownerEpoch"`
}

type DrainHeldEventsResult struct {
	EventsDrained int64 `json:"eventsDrained"`
	NewVersion    int64 `json:"newVersion"`
	RemainingHeld int64 `json:"remainingHeld"`
	IsUnheld      bool  `json:"isUnheld"`
}

type AggregateSequenceTrackerRecord struct {
	ID                 string     `json:"id"`
	PublicID           string     `json:"publicId"`
	TenantID           string     `json:"tenantId"`
	SourceService      string     `json:"sourceService"`
	ExternalID         string     `json:"externalId"`
	CorrelationID      string     `json:"correlationId"`
	ConsumerGroup      string     `json:"consumerGroup"`
	HandlerID          string     `json:"handlerId"`
	AggregateType      string     `json:"aggregateType"`
	AggregateID        string     `json:"aggregateId"`
	LastAppliedVersion int64      `json:"lastAppliedVersion"`
	LastPayloadHash    string     `json:"lastPayloadHash"`
	OwnerEpoch         int64      `json:"ownerEpoch"`
	IsHeld             bool       `json:"isHeld"`
	HoldReason         HoldReason `json:"holdReason,omitempty"`
	BlockingVersion    *int64     `json:"blockingVersion,omitempty"`
	UpdatedAt          time.Time  `json:"updatedAt"`
}

type SequenceHeldEventRecord struct {
	ID               string          `json:"id"`
	PublicID         string          `json:"publicId"`
	TenantID         string          `json:"tenantId"`
	SourceService    string          `json:"sourceService"`
	ExternalID       string          `json:"externalId"`
	CorrelationID    string          `json:"correlationId"`
	ConsumerGroup    string          `json:"consumerGroup"`
	HandlerID        string          `json:"handlerId"`
	AggregateType    string          `json:"aggregateType"`
	AggregateID      string          `json:"aggregateId"`
	AggregateVersion int64           `json:"aggregateVersion"`
	EventID          string          `json:"eventId"`
	EventType        string          `json:"eventType"`
	Payload          json.RawMessage `json:"payload"`
	PayloadHash      string          `json:"payloadHash"`
	Headers          json.RawMessage `json:"headers"`
	SourceOffset     int64           `json:"sourceOffset"`
	ArrivedAt        time.Time       `json:"arrivedAt"`
}


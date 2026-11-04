package contracts

import "time"

type SemanticClass string

const (
	SemanticState SemanticClass = "STATE"
	SemanticDelta SemanticClass = "DELTA"
)

type ConsumerProcessAction string

const (
	ActionApply     ConsumerProcessAction = "APPLY"
	ActionSkip      ConsumerProcessAction = "SKIP_DUPLICATE"
	ActionHoldGap   ConsumerProcessAction = "HOLD_GAP"
	ActionParkPoison ConsumerProcessAction = "PARK_POISON"
)

type InboundConsumerEvent struct {
	EventID          string            `json:"event_id"`
	TenantID         string            `json:"tenant_id"`
	HandlerID        string            `json:"handler_id"`
	AggregateType    string            `json:"aggregate_type"`
	AggregateID      string            `json:"aggregate_id"`
	AggregateVersion int64             `json:"aggregate_version"`
	EventType        string            `json:"event_type"`
	SchemaVersion    string            `json:"schema_version"`
	SemanticClass    SemanticClass     `json:"semantic_class"`
	Payload          []byte            `json:"payload"`
	Headers          map[string]string `json:"headers"`
	EventOccurredAt  time.Time         `json:"event_occurred_at"`
}

type ConsumerInboxRecord struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	HandlerID        string    `json:"handler_id"`
	EventID          string    `json:"event_id"`
	AggregateType    string    `json:"aggregate_type"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	EventType        string    `json:"event_type"`
	ProcessedAt      time.Time `json:"processed_at"`
	PayloadHash      string    `json:"payload_hash"`
}

type AggregateProjectionState struct {
	TenantID        string    `json:"tenant_id"`
	AggregateType   string    `json:"aggregate_type"`
	AggregateID     string    `json:"aggregate_id"`
	LastVersion     int64     `json:"last_version"`
	LastEventID     string    `json:"last_event_id"`
	LastPayloadHash string    `json:"last_payload_hash"`
	IsParked        bool      `json:"is_parked"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type ConsumeResult struct {
	EventID       string                `json:"event_id"`
	AggregateID   string                `json:"aggregate_id"`
	Action        ConsumerProcessAction `json:"action"`
	CurrentVersion int64                `json:"current_version"`
	AppliedVersion int64                `json:"applied_version"`
	Reason        string                `json:"reason,omitempty"`
}

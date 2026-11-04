package contracts

import "time"

type QuarantineStatus string

const (
	StatusQuarantined     QuarantineStatus = "QUARANTINED"
	StatusRedriving       QuarantineStatus = "REDRIVING"
	StatusRedrivenSuccess QuarantineStatus = "REDRIVEN_SUCCESS"
	StatusCompensated     QuarantineStatus = "COMPENSATED"
	StatusDiscarded       QuarantineStatus = "DISCARDED"
)

type AttemptRecord struct {
	TenantID              string    `json:"tenant_id"`
	ConsumerGroup         string    `json:"consumer_group"`
	HandlerID             string    `json:"handler_id"`
	EventID               string    `json:"event_id"`
	AttemptCount          int       `json:"attempt_count"`
	FirstAttemptAt        time.Time `json:"first_attempt_at"`
	LastAttemptAt         time.Time `json:"last_attempt_at"`
	LastErrorFingerprint  string    `json:"last_error_fingerprint,omitempty"`
	LastErrorClass        string    `json:"last_error_class,omitempty"`
}

type ParkedEventRecord struct {
	ID               string            `json:"id"`
	TenantID         string            `json:"tenant_id"`
	ConsumerGroup    string            `json:"consumer_group"`
	HandlerID        string            `json:"handler_id"`
	AggregateType    string            `json:"aggregate_type"`
	AggregateID      string            `json:"aggregate_id"`
	AggregateVersion int64             `json:"aggregate_version"`
	EventID          string            `json:"event_id"`
	EventType        string            `json:"event_type"`
	Payload          []byte            `json:"payload"`
	Headers          map[string]string `json:"headers"`
	SourceOffset     int64             `json:"source_offset"`
	ArrivedAt        time.Time         `json:"arrived_at"`
}

type ConsumedOffsetRecord struct {
	TenantID      string    `json:"tenant_id"`
	ConsumerGroup string    `json:"consumer_group"`
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	NextOffset    int64     `json:"next_offset"`
	OwnerEpoch    int64     `json:"owner_epoch"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type DeadLetterQuarantineRecord struct {
	ID                 string           `json:"id"`
	PublicID           string           `json:"public_id"`
	TenantID           string           `json:"tenant_id"`
	SourceService      string           `json:"source_service"`
	ExternalID         string           `json:"external_id"`
	CorrelationID      string           `json:"correlation_id"`
	Topic              string           `json:"topic"`
	Partition          int              `json:"partition"`
	SourceOffset       int64            `json:"source_offset"`
	ConsumerGroup      string           `json:"consumer_group"`
	HandlerID          string           `json:"handler_id"`
	EventID            string           `json:"event_id"`
	AggregateType      string           `json:"aggregate_type"`
	AggregateID        string           `json:"aggregate_id"`
	AggregateVersion   int64            `json:"aggregate_version"`
	EventType          string           `json:"event_type"`
	Payload            []byte           `json:"payload"`
	Headers            map[string]string `json:"headers"`
	FailureReason      string           `json:"failure_reason"`
	ErrorCode          string           `json:"error_code"`
	ErrorFingerprint   string           `json:"error_fingerprint"`
	Status             QuarantineStatus `json:"status"`
	RedriveCount       int              `json:"redrive_count"`
	MaxRedriveAttempts int              `json:"max_redrive_attempts"`
	QuarantinedAt      time.Time        `json:"quarantined_at"`
	UpdatedAt          time.Time        `json:"updated_at"`
}

type AggregateParkingState struct {
	TenantID         string    `json:"tenant_id"`
	HandlerID        string    `json:"handler_id"`
	AggregateType    string    `json:"aggregate_type"`
	AggregateID      string    `json:"aggregate_id"`
	IsParked         bool      `json:"is_parked"`
	ParkedReason     string    `json:"parked_reason"`
	QuarantineID     string    `json:"quarantine_id"`
	OwnerEpoch       int64     `json:"owner_epoch"`
	ParkedEventCount int       `json:"parked_event_count"`
	ParkedAt         time.Time `json:"parked_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type QuarantineEventInput struct {
	TenantID         string            `json:"tenant_id"`
	SourceService    string            `json:"source_service"`
	ExternalID       string            `json:"external_id"`
	CorrelationID    string            `json:"correlation_id"`
	Topic            string            `json:"topic"`
	Partition        int               `json:"partition"`
	SourceOffset     int64             `json:"source_offset"`
	ConsumerGroup    string            `json:"consumer_group"`
	HandlerID        string            `json:"handler_id"`
	EventID          string            `json:"event_id"`
	AggregateType    string            `json:"aggregate_type"`
	AggregateID      string            `json:"aggregate_id"`
	AggregateVersion int64             `json:"aggregate_version"`
	EventType        string            `json:"event_type"`
	Payload          []byte            `json:"payload"`
	Headers          map[string]string `json:"headers"`
	FailureReason    string            `json:"failure_reason"`
	ErrorCode        string            `json:"error_code"`
	OwnerEpoch       int64             `json:"owner_epoch,omitempty"`
}

type RedriveResult struct {
	QuarantineID string           `json:"quarantine_id"`
	Status       QuarantineStatus `json:"status"`
	RedriveCount int              `json:"redrive_count"`
	DrainedCount int              `json:"drained_count"`
	Success      bool             `json:"success"`
	Reason       string           `json:"reason,omitempty"`
}

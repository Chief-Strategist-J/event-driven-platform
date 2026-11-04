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
	TenantID      string    `json:"tenant_id"`
	HandlerID     string    `json:"handler_id"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	IsParked      bool      `json:"is_parked"`
	ParkedReason  string    `json:"parked_reason"`
	QuarantineID  string    `json:"quarantine_id"`
	ParkedAt      time.Time `json:"parked_at"`
	UpdatedAt     time.Time `json:"updated_at"`
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
}

type RedriveResult struct {
	QuarantineID string           `json:"quarantine_id"`
	Status       QuarantineStatus `json:"status"`
	RedriveCount int              `json:"redrive_count"`
	Success      bool             `json:"success"`
	Reason       string           `json:"reason,omitempty"`
}


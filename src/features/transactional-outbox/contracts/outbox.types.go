package contracts

import "time"

type OutboxStatus string

const (
	StatusPending   OutboxStatus = "PENDING"
	StatusInFlight  OutboxStatus = "IN_FLIGHT"
	StatusPublished OutboxStatus = "PUBLISHED"
	StatusFailed    OutboxStatus = "FAILED"
	StatusParked    OutboxStatus = "PARKED"
)

type OutboxRecord struct {
	ID               string            `json:"id"`
	PublicID         string            `json:"public_id"`
	TenantID         string            `json:"tenant_id"`
	SourceService    string            `json:"source_service"`
	ExternalID       string            `json:"external_id"`
	CorrelationID    string            `json:"correlation_id"`
	CausationID      string            `json:"causation_id"`
	AggregateType    string            `json:"aggregate_type"`
	AggregateID      string            `json:"aggregate_id"`
	AggregateVersion int64             `json:"aggregate_version"`
	EventType        string            `json:"event_type"`
	SchemaVersion    string            `json:"schema_version"`
	Payload          []byte            `json:"payload"`
	Headers          map[string]string `json:"headers"`
	Status           OutboxStatus      `json:"status"`
	WorkerID         string            `json:"worker_id,omitempty"`
	LeaseExpiresAt   *time.Time        `json:"lease_expires_at,omitempty"`
	RetryCount       int               `json:"retry_count"`
	LastError        string            `json:"last_error,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	ProcessedAt      *time.Time        `json:"processed_at,omitempty"`
}

type CreateOutboxRecordInput struct {
	TenantID         string            `json:"tenant_id"`
	SourceService    string            `json:"source_service"`
	ExternalID       string            `json:"external_id"`
	CorrelationID    string            `json:"correlation_id"`
	CausationID      string            `json:"causation_id"`
	AggregateType    string            `json:"aggregate_type"`
	AggregateID      string            `json:"aggregate_id"`
	AggregateVersion int64             `json:"aggregate_version"`
	EventType        string            `json:"event_type"`
	SchemaVersion    string            `json:"schema_version"`
	Payload          []byte            `json:"payload"`
	Headers          map[string]string `json:"headers"`
	CDCMode          bool              `json:"cdc_mode,omitempty"`
}

type IdempotencyRecord struct {
	TenantID       string    `json:"tenant_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	RecordID       string    `json:"record_id"`
	CreatedAt      time.Time `json:"created_at"`
}

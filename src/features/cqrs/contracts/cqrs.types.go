package contracts

import (
	"time"

	"github.com/google/uuid"
)

type CommandStatus string

const (
	CommandStatusInProgress CommandStatus = "IN_PROGRESS"
	CommandStatusCompleted  CommandStatus = "COMPLETED"
	CommandStatusRejected   CommandStatus = "REJECTED"
	CommandStatusConflict   CommandStatus = "CONFLICT"
)

type ConsistencyResolution string

const (
	ResolutionImmediate     ConsistencyResolution = "IMMEDIATE"
	ResolutionWaited        ConsistencyResolution = "WAITED"
	ResolutionPending       ConsistencyResolution = "PENDING"
	ResolutionAuthoritative ConsistencyResolution = "AUTHORITATIVE"
)

type CommandRecord struct {
	ID                 uuid.UUID     `json:"id"`
	PublicID           string        `json:"public_id"`
	TenantID           string        `json:"tenant_id"`
	SourceService      string        `json:"source_service"`
	ExternalID         string        `json:"external_id"`
	CorrelationID      uuid.UUID     `json:"correlation_id"`
	IdempotencyKey     string        `json:"idempotency_key"`
	CommandName        string        `json:"command_name"`
	AggregateType      string        `json:"aggregate_type"`
	AggregateID        string        `json:"aggregate_id"`
	RequestFingerprint string        `json:"request_fingerprint"`
	Status             CommandStatus `json:"status"`
	ResponsePayload    string        `json:"response_payload"`
	ConsistencyToken   string        `json:"consistency_token"`
	Version            int64         `json:"version"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
}

type AggregateRecord struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"public_id"`
	TenantID       string    `json:"tenant_id"`
	SourceService  string    `json:"source_service"`
	ExternalID     string    `json:"external_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	AggregateType  string    `json:"aggregate_type"`
	AggregateID    string    `json:"aggregate_id"`
	CurrentVersion int64     `json:"current_version"`
	StatePayload   string    `json:"state_payload"`
	IsDeleted      bool      `json:"is_deleted"`
	Version        int64     `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type TokenSigningKeyRecord struct {
	ID             uuid.UUID `json:"id"`
	PublicID       string    `json:"public_id"`
	TenantID       string    `json:"tenant_id"`
	SourceService  string    `json:"source_service"`
	ExternalID     string    `json:"external_id"`
	CorrelationID  uuid.UUID `json:"correlation_id"`
	KeyVersion     int       `json:"key_version"`
	SecretKeyHash  string    `json:"secret_key_hash"`
	RawSecretValue string    `json:"raw_secret_value"`
	Status         string    `json:"status"`
	ExpiresAt      time.Time `json:"expires_at"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type ConsistencyToken struct {
	TenantID      string    `json:"tenant_id"`
	PrincipalID   string    `json:"principal_id"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	MinVersion    int64     `json:"min_version"`
	ExpiresAt     time.Time `json:"expires_at"`
	KeyVersion    int       `json:"key_version"`
	Signature     string    `json:"signature"`
}

type DispatchCommandInput struct {
	TenantID        string `json:"tenant_id"`
	PrincipalID     string `json:"principal_id"`
	SourceService   string `json:"source_service"`
	ExternalID      string `json:"external_id"`
	CorrelationID   string `json:"correlation_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	CommandName     string `json:"command_name"`
	AggregateType   string `json:"aggregate_type"`
	AggregateID     string `json:"aggregate_id"`
	ExpectedVersion int64  `json:"expected_version"`
	PayloadRaw      string `json:"payload_raw"`
}

type DispatchCommandOutput struct {
	CommandID        string        `json:"command_id"`
	AggregateID      string        `json:"aggregate_id"`
	NewVersion       int64         `json:"new_version"`
	Status           CommandStatus `json:"status"`
	ConsistencyToken string        `json:"consistency_token,omitempty"`
	ResponsePayload  string        `json:"response_payload"`
}

type ResolveQueryInput struct {
	TenantID            string `json:"tenant_id"`
	PrincipalID         string `json:"principal_id"`
	ViewName            string `json:"view_name"`
	AggregateType       string `json:"aggregate_type"`
	AggregateID         string `json:"aggregate_id"`
	ConsistencyTokenRaw string `json:"consistency_token_raw"`
	TimeoutBudgetMs     int64  `json:"timeout_budget_ms"`
}

type ResolveQueryOutput struct {
	Data              string                `json:"data"`
	Version           int64                 `json:"version"`
	ConsistencyStatus ConsistencyResolution `json:"consistency_status"`
	ExpectedVersion   int64                 `json:"expected_version"`
}

type RegisterSigningKeyInput struct {
	TenantID       string    `json:"tenant_id"`
	SourceService  string    `json:"source_service"`
	CorrelationID  string    `json:"correlation_id"`
	KeyVersion     int       `json:"key_version"`
	RawSecretValue string    `json:"raw_secret_value"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type AuthoritativeReadInput struct {
	TenantID            string `json:"tenant_id"`
	PrincipalID         string `json:"principal_id"`
	AggregateType       string `json:"aggregate_type"`
	AggregateID         string `json:"aggregate_id"`
	ConsistencyTokenRaw string `json:"consistency_token_raw"`
}


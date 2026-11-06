package contracts

import (
	"time"
)

type AuditOutcome string

const (
	OutcomeSucceeded AuditOutcome = "SUCCEEDED"
	OutcomeDenied    AuditOutcome = "DENIED"
	OutcomeFailed    AuditOutcome = "FAILED"
	OutcomeIntent    AuditOutcome = "INTENT"
)

type AuditActorType string

const (
	ActorTypeUser    AuditActorType = "USER"
	ActorTypeService AuditActorType = "SERVICE"
	ActorTypeSystem  AuditActorType = "SYSTEM"
	ActorTypeAPIKey  AuditActorType = "API_KEY"
)

type ComplianceClass string

const (
	ClassA ComplianceClass = "CLASS_A"
	ClassB ComplianceClass = "CLASS_B"
	ClassC ComplianceClass = "CLASS_C"
	ClassD ComplianceClass = "CLASS_D"
)

type VerificationStatus string

const (
	StatusIntact   VerificationStatus = "INTACT"
	StatusMismatch VerificationStatus = "MISMATCH"
	StatusTampered VerificationStatus = "TAMPERED"
	StatusUnknown  VerificationStatus = "UNKNOWN"
)

type AuditRecord struct {
	ID               string          `json:"id"`
	PublicID         string          `json:"public_id"`
	TenantID         string          `json:"tenant_id"`
	SourceService    string          `json:"source_service"`
	ExternalID       string          `json:"external_id"`
	CorrelationID    string          `json:"correlation_id"`
	ShardID          string          `json:"shard_id"`
	SequenceNumber   int64           `json:"sequence_number"`
	ActorID          string          `json:"actor_id"`
	ActorType        AuditActorType  `json:"actor_type"`
	ActionName       string          `json:"action_name"`
	TargetType       string          `json:"target_type"`
	TargetID         string          `json:"target_id"`
	Outcome          AuditOutcome    `json:"outcome"`
	ComplianceClass  ComplianceClass `json:"compliance_class"`
	PrevRecordHash   string          `json:"prev_record_hash"`
	RecordHash       string          `json:"record_hash"`
	CanonicalPayload string          `json:"canonical_payload"`
	OccurredAt       time.Time       `json:"occurred_at"`
	IngestedAt       time.Time       `json:"ingested_at"`
}

type CheckpointRecord struct {
	ID              string    `json:"id"`
	PublicID        string    `json:"public_id"`
	TenantID        string    `json:"tenant_id"`
	SourceService   string    `json:"source_service"`
	ExternalID      string    `json:"external_id"`
	CorrelationID   string    `json:"correlation_id"`
	ShardID         string    `json:"shard_id"`
	CheckpointSeq   int64     `json:"checkpoint_seq"`
	StartSequence   int64     `json:"start_sequence"`
	EndSequence     int64     `json:"end_sequence"`
	RecordCount     int64     `json:"record_count"`
	MerkleRootHash  string    `json:"merkle_root_hash"`
	HeadRecordHash  string    `json:"head_record_hash"`
	AnchorSignature string    `json:"anchor_signature"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
}

type AccessLogRecord struct {
	ID            string    `json:"id"`
	PublicID      string    `json:"public_id"`
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID string    `json:"correlation_id"`
	AuditorID     string    `json:"auditor_id"`
	CaseID        string    `json:"case_id"`
	QueryScope    string    `json:"query_scope"`
	Reason        string    `json:"reason"`
	AccessedAt    time.Time `json:"accessed_at"`
}

type IngestAuditRecordInput struct {
	TenantID        string          `json:"tenant_id"`
	SourceService   string          `json:"source_service"`
	ExternalID      string          `json:"external_id"`
	CorrelationID   string          `json:"correlation_id"`
	ShardID         string          `json:"shard_id"`
	ActorID         string          `json:"actor_id"`
	ActorType       AuditActorType  `json:"actor_type"`
	ActionName      string          `json:"action_name"`
	TargetType      string          `json:"target_type"`
	TargetID        string          `json:"target_id"`
	Outcome         AuditOutcome    `json:"outcome"`
	ComplianceClass ComplianceClass `json:"compliance_class"`
	RawPayload      string          `json:"raw_payload"`
	OccurredAt      time.Time       `json:"occurred_at"`
}

type EmitAuditInput struct {
	TenantID         string          `json:"tenant_id"`
	SourceService    string          `json:"source_service"`
	ExternalID       string          `json:"external_id"`
	CorrelationID    string          `json:"correlation_id"`
	ShardID          string          `json:"shard_id"`
	ActorID          string          `json:"actor_id"`
	ActorType        AuditActorType  `json:"actor_type"`
	ActionName       string          `json:"action_name"`
	TargetType       string          `json:"target_type"`
	TargetID         string          `json:"target_id"`
	Outcome          AuditOutcome    `json:"outcome"`
	ComplianceClass  ComplianceClass `json:"compliance_class"`
	CanonicalPayload string          `json:"canonical_payload"`
	RawPayload       string          `json:"raw_payload"`
	OccurredAt       time.Time       `json:"occurred_at"`
}

type VerifyChainInput struct {
	TenantID      string `json:"tenant_id"`
	ShardID       string `json:"shard_id"`
	SigningSecret string `json:"signing_secret"`
}

type VerifyChainOutput struct {
	IsIntact         bool   `json:"is_intact"`
	RecordsVerified  int64  `json:"records_verified"`
	LatestSequence   int64  `json:"latest_sequence"`
	LatestRecordHash string `json:"latest_record_hash"`
}

type CreateCheckpointInput struct {
	TenantID      string `json:"tenant_id"`
	SourceService string `json:"source_service"`
	CorrelationID string `json:"correlation_id"`
	ShardID       string `json:"shard_id"`
	SigningSecret string `json:"signing_secret"`
}

type VerifyShardIntegrityInput struct {
	TenantID      string `json:"tenant_id"`
	ShardID       string `json:"shard_id"`
	SigningSecret string `json:"signing_secret"`
}

type VerificationReport struct {
	TenantID           string             `json:"tenant_id"`
	ShardID            string             `json:"shard_id"`
	Status             VerificationStatus `json:"status"`
	LastSequenceNumber int64              `json:"last_sequence_number"`
	TotalRecords       int64              `json:"total_records"`
	CalculatedHeadHash string             `json:"calculated_head_hash"`
	AnchoredHeadHash   string             `json:"anchored_head_hash"`
	TamperedSequence   int64              `json:"tampered_sequence,omitempty"`
	Message            string             `json:"message"`
}

type QueryAuditInput struct {
	TenantID   string    `json:"tenant_id"`
	AuditorID  string    `json:"auditor_id"`
	CaseID     string    `json:"case_id"`
	Reason     string    `json:"reason"`
	ShardID    string    `json:"shard_id"`
	TargetType string    `json:"target_type"`
	TargetID   string    `json:"target_id"`
	ActorID    string    `json:"actor_id"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time"`
	Limit      int       `json:"limit"`
}

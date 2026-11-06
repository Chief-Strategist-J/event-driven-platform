package contracts

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantID        = errors.New("tenant_id must not be empty or whitespace")
	ErrInvalidSourceService   = errors.New("source_service must not be empty or whitespace")
	ErrInvalidCorrelationID   = errors.New("correlation_id must be a valid non-nil UUID")
	ErrInvalidShardID         = errors.New("shard_id must not be empty or whitespace")
	ErrInvalidActorID         = errors.New("actor_id must not be empty or whitespace")
	ErrInvalidActionName      = errors.New("action_name must not be empty or whitespace")
	ErrInvalidTargetType      = errors.New("target_type must not be empty or whitespace")
	ErrInvalidTargetID        = errors.New("target_id must not be empty or whitespace")
	ErrInvalidOutcome         = errors.New("invalid audit outcome")
	ErrInvalidActorType       = errors.New("invalid actor type")
	ErrInvalidClass           = errors.New("invalid compliance class")
	ErrSecretDetected         = errors.New("security violation: raw_payload contains forbidden secret patterns")
	ErrEmptyPayload           = errors.New("raw_payload must not be empty")
	ErrInvalidAuditorID       = errors.New("auditor_id must not be empty or whitespace")
	ErrInvalidCaseID          = errors.New("case_id must not be empty or whitespace")
	ErrInvalidSigningSecret   = errors.New("signing_secret must be at least 32 characters")
	ErrInvalidSequenceRange   = errors.New("start_sequence must be less than or equal to end_sequence")
)

func NormalizeIngestAuditRecordInput(in IngestAuditRecordInput) (IngestAuditRecordInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.ShardID = strings.TrimSpace(in.ShardID)
	if in.ShardID == "" {
		return in, ErrInvalidShardID
	}
	in.ActorID = strings.TrimSpace(in.ActorID)
	if in.ActorID == "" {
		return in, ErrInvalidActorID
	}
	in.ActionName = strings.TrimSpace(in.ActionName)
	if in.ActionName == "" {
		return in, ErrInvalidActionName
	}
	in.TargetType = strings.TrimSpace(in.TargetType)
	if in.TargetType == "" {
		return in, ErrInvalidTargetType
	}
	in.TargetID = strings.TrimSpace(in.TargetID)
	if in.TargetID == "" {
		return in, ErrInvalidTargetID
	}
	if in.ActorType == "" {
		in.ActorType = ActorTypeService
	}
	if in.Outcome == "" {
		in.Outcome = OutcomeSucceeded
	}
	if in.ComplianceClass == "" {
		in.ComplianceClass = ClassA
	}
	in.RawPayload = strings.TrimSpace(in.RawPayload)
	if in.RawPayload == "" {
		return in, ErrEmptyPayload
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	if in.ExternalID == "" {
		in.ExternalID = uuid.New().String()
	}
	if in.OccurredAt.IsZero() {
		in.OccurredAt = time.Now().UTC()
	}
	return in, nil
}

func NormalizeCreateCheckpointInput(in CreateCheckpointInput) (CreateCheckpointInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.ShardID = strings.TrimSpace(in.ShardID)
	if in.ShardID == "" {
		return in, ErrInvalidShardID
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	in.SigningSecret = strings.TrimSpace(in.SigningSecret)
	if len(in.SigningSecret) < 32 {
		return in, ErrInvalidSigningSecret
	}
	return in, nil
}

func NormalizeVerifyShardIntegrityInput(in VerifyShardIntegrityInput) (VerifyShardIntegrityInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.ShardID = strings.TrimSpace(in.ShardID)
	if in.ShardID == "" {
		return in, ErrInvalidShardID
	}
	in.SigningSecret = strings.TrimSpace(in.SigningSecret)
	if len(in.SigningSecret) < 32 {
		return in, ErrInvalidSigningSecret
	}
	return in, nil
}

func NormalizeQueryAuditInput(in QueryAuditInput) (QueryAuditInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.AuditorID = strings.TrimSpace(in.AuditorID)
	if in.AuditorID == "" {
		return in, ErrInvalidAuditorID
	}
	in.CaseID = strings.TrimSpace(in.CaseID)
	if in.CaseID == "" {
		return in, ErrInvalidCaseID
	}
	in.ShardID = strings.TrimSpace(in.ShardID)
	if in.Limit <= 0 || in.Limit > 500 {
		in.Limit = 50
	}
	return in, nil
}

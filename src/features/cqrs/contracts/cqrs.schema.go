package contracts

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantID      = errors.New("tenant_id must not be empty or whitespace")
	ErrInvalidPrincipalID   = errors.New("principal_id must not be empty or whitespace")
	ErrInvalidSourceService = errors.New("source_service must not be empty or whitespace")
	ErrInvalidIdempotencyKey = errors.New("idempotency_key must not be empty or whitespace")
	ErrInvalidCommandName   = errors.New("command_name must not be empty or whitespace")
	ErrInvalidAggregateType = errors.New("aggregate_type must not be empty or whitespace")
	ErrInvalidAggregateID   = errors.New("aggregate_id must not be empty or whitespace")
	ErrInvalidCorrelationID = errors.New("correlation_id must be a valid non-nil UUID")
	ErrNegativeVersion      = errors.New("expected_version must be non-negative")
	ErrInvalidSecretKey     = errors.New("raw_secret_value must be at least 16 characters")
	ErrInvalidKeyVersion    = errors.New("key_version must be positive")
	ErrInvalidKeyExpiry     = errors.New("signing key expires_at must be in the future")
	ErrInvalidViewName      = errors.New("view_name must not be empty or whitespace")
)

func NormalizeDispatchCommandInput(in DispatchCommandInput) (DispatchCommandInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.PrincipalID = strings.TrimSpace(in.PrincipalID)
	if in.PrincipalID == "" {
		return in, ErrInvalidPrincipalID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)
	if in.IdempotencyKey == "" {
		return in, ErrInvalidIdempotencyKey
	}
	in.CommandName = strings.TrimSpace(in.CommandName)
	if in.CommandName == "" {
		return in, ErrInvalidCommandName
	}
	in.AggregateType = strings.TrimSpace(in.AggregateType)
	if in.AggregateType == "" {
		return in, ErrInvalidAggregateType
	}
	in.AggregateID = strings.TrimSpace(in.AggregateID)
	if in.AggregateID == "" {
		return in, ErrInvalidAggregateID
	}
	if in.ExpectedVersion < 0 {
		return in, ErrNegativeVersion
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	if in.ExternalID == "" {
		in.ExternalID = in.IdempotencyKey
	}
	return in, nil
}

func NormalizeResolveQueryInput(in ResolveQueryInput) (ResolveQueryInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.PrincipalID = strings.TrimSpace(in.PrincipalID)
	if in.PrincipalID == "" {
		return in, ErrInvalidPrincipalID
	}
	in.ViewName = strings.TrimSpace(in.ViewName)
	if in.ViewName == "" {
		return in, ErrInvalidViewName
	}
	in.AggregateType = strings.TrimSpace(in.AggregateType)
	if in.AggregateType == "" {
		return in, ErrInvalidAggregateType
	}
	in.AggregateID = strings.TrimSpace(in.AggregateID)
	if in.AggregateID == "" {
		return in, ErrInvalidAggregateID
	}
	if in.TimeoutBudgetMs <= 0 {
		in.TimeoutBudgetMs = 500
	} else if in.TimeoutBudgetMs > 5000 {
		in.TimeoutBudgetMs = 5000
	}
	return in, nil
}

func NormalizeRegisterSigningKeyInput(in RegisterSigningKeyInput) (RegisterSigningKeyInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	if in.KeyVersion <= 0 {
		return in, ErrInvalidKeyVersion
	}
	if len(in.RawSecretValue) < 16 {
		return in, ErrInvalidSecretKey
	}
	if !in.ExpiresAt.After(time.Now().UTC()) {
		return in, ErrInvalidKeyExpiry
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	return in, nil
}

func NormalizeAuthoritativeReadInput(in AuthoritativeReadInput) (AuthoritativeReadInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.PrincipalID = strings.TrimSpace(in.PrincipalID)
	if in.PrincipalID == "" {
		return in, ErrInvalidPrincipalID
	}
	in.AggregateType = strings.TrimSpace(in.AggregateType)
	if in.AggregateType == "" {
		return in, ErrInvalidAggregateType
	}
	in.AggregateID = strings.TrimSpace(in.AggregateID)
	if in.AggregateID == "" {
		return in, ErrInvalidAggregateID
	}
	return in, nil
}


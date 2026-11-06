package contracts

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantID      = errors.New("tenant_id must not be empty or whitespace")
	ErrInvalidSourceService = errors.New("source_service must not be empty or whitespace")
	ErrInvalidViewName      = errors.New("view_name must not be empty or whitespace")
	ErrInvalidCorrelationID = errors.New("correlation_id must be a valid non-nil UUID")
	ErrInvalidOwnerID       = errors.New("owner_id must not be empty or whitespace")
	ErrInvalidReason        = errors.New("reason must not be empty")
	ErrInvalidSilenceExpiry = errors.New("silence expiry must be in the future and within 7 days")
	ErrNegativeOffset       = errors.New("offsets and lag values must not be negative")
	ErrNegativePartition    = errors.New("partition_id must be non-negative")
	ErrInvalidCounts        = errors.New("evaluated and outcome counts must be non-negative")
)

func NormalizeObserveWatermarkInput(in ObserveWatermarkInput) (ObserveWatermarkInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.ViewName = strings.TrimSpace(in.ViewName)
	if in.ViewName == "" {
		return in, ErrInvalidViewName
	}
	in.StreamName = strings.TrimSpace(in.StreamName)
	if in.StreamName == "" {
		in.StreamName = in.ViewName
	}
	if in.PartitionID < 0 {
		return in, ErrNegativePartition
	}
	if in.HighWatermarkOffset < 0 || in.ConsumerOffset < 0 || in.FreshnessLagMs < 0 {
		return in, ErrNegativeOffset
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	if in.ExternalID == "" {
		in.ExternalID = uuid.New().String()
	}
	return in, nil
}

func NormalizeRegisterSilenceInput(in RegisterSilenceInput) (RegisterSilenceInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.ViewName = strings.TrimSpace(in.ViewName)
	if in.ViewName == "" {
		return in, ErrInvalidViewName
	}
	in.OwnerID = strings.TrimSpace(in.OwnerID)
	if in.OwnerID == "" {
		return in, ErrInvalidOwnerID
	}
	in.Reason = strings.TrimSpace(in.Reason)
	if in.Reason == "" {
		return in, ErrInvalidReason
	}
	now := time.Now().UTC()
	if !in.ExpiresAt.After(now) {
		return in, ErrInvalidSilenceExpiry
	}
	if in.ExpiresAt.After(now.Add(7 * 24 * time.Hour)) {
		return in, ErrInvalidSilenceExpiry
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	if in.ExternalID == "" {
		in.ExternalID = uuid.New().String()
	}
	return in, nil
}

func NormalizeRunCheckSweepInput(in RunCheckSweepInput) (RunCheckSweepInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.ViewName = strings.TrimSpace(in.ViewName)
	if in.ViewName == "" {
		return in, ErrInvalidViewName
	}
	if in.EvaluatedKeys < 0 || in.MatchCount < 0 || in.MismatchCount < 0 || in.UnknownCount < 0 || in.DurationMs < 0 {
		return in, ErrInvalidCounts
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	return in, nil
}

func NormalizeCanaryProbeInput(in CanaryProbeInput) (CanaryProbeInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.StreamName = strings.TrimSpace(in.StreamName)
	if in.StreamName == "" {
		return in, ErrInvalidViewName
	}
	if in.PartitionID < 0 {
		return in, ErrNegativePartition
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	return in, nil
}


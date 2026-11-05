package contracts

import (
	"errors"
	"strings"
)

var (
	ErrMissingTenantID         = errors.New("tenantId is required")
	ErrMissingSourceService    = errors.New("sourceService is required")
	ErrMissingExternalID       = errors.New("externalId is required")
	ErrMissingCorrelationID    = errors.New("correlationId is required")
	ErrMissingTargetProjection = errors.New("targetProjection is required")
	ErrMissingRunID            = errors.New("runId is required")
	ErrInvalidRunMode          = errors.New("invalid replay run mode")
	ErrInvalidScope            = errors.New("invalid replay scope")
	ErrNegativeOffsetRange     = errors.New("startOffset and endOffset must be non-negative with endOffset >= startOffset")
	ErrMissingIdempotencyKey   = errors.New("idempotencyKey is required for effect evaluation")
)

func ValidateCreateReplayRunInput(input CreateReplayRunInput) (CreateReplayRunInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.TargetProjection = strings.TrimSpace(input.TargetProjection)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.SourceService == "" {
		return input, ErrMissingSourceService
	}
	if input.ExternalID == "" {
		return input, ErrMissingExternalID
	}
	if input.CorrelationID == "" {
		return input, ErrMissingCorrelationID
	}
	if input.TargetProjection == "" {
		return input, ErrMissingTargetProjection
	}
	switch input.Mode {
	case ModeRebuild, ModeHydrate, ModeForensic, ModeRecover, ModeRepair, ModeLive:
	default:
		return input, ErrInvalidRunMode
	}
	switch input.Scope {
	case ScopeGlobalTopic, ScopeTimeWindow, ScopeAggregateSet:
	default:
		return input, ErrInvalidScope
	}
	if input.StartOffset < 0 || input.EndOffset < input.StartOffset {
		return input, ErrNegativeOffsetRange
	}
	return input, nil
}

func ValidateAdvanceReplayInput(input AdvanceReplayInput) (AdvanceReplayInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.RunID = strings.TrimSpace(input.RunID)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.RunID == "" {
		return input, ErrMissingRunID
	}
	if input.CurrentOffset < 0 {
		return input, ErrNegativeOffsetRange
	}
	return input, nil
}

func ValidatePromoteReplayInput(input PromoteReplayInput) (PromoteReplayInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.RunID = strings.TrimSpace(input.RunID)
	input.Reason = strings.TrimSpace(input.Reason)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.RunID == "" {
		return input, ErrMissingRunID
	}
	return input, nil
}

func ValidateCompleteCutoverInput(input CompleteCutoverInput) (CompleteCutoverInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.RunID = strings.TrimSpace(input.RunID)
	input.Reason = strings.TrimSpace(input.Reason)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.RunID == "" {
		return input, ErrMissingRunID
	}
	return input, nil
}

func ValidateEvaluateEffectInput(input EvaluateEffectInput) (EvaluateEffectInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.RunID = strings.TrimSpace(input.RunID)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	input.PayloadDigest = strings.TrimSpace(input.PayloadDigest)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.RunID == "" {
		return input, ErrMissingRunID
	}
	if input.IdempotencyKey == "" {
		return input, ErrMissingIdempotencyKey
	}
	return input, nil
}

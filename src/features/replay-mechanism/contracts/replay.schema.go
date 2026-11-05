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

func ValidateCreateReplayRunInput(input CreateReplayRunInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.SourceService) == "" {
		return ErrMissingSourceService
	}
	if strings.TrimSpace(input.ExternalID) == "" {
		return ErrMissingExternalID
	}
	if strings.TrimSpace(input.CorrelationID) == "" {
		return ErrMissingCorrelationID
	}
	if strings.TrimSpace(input.TargetProjection) == "" {
		return ErrMissingTargetProjection
	}
	switch input.Mode {
	case ModeRebuild, ModeHydrate, ModeForensic, ModeRecover, ModeRepair, ModeLive:
	default:
		return ErrInvalidRunMode
	}
	switch input.Scope {
	case ScopeGlobalTopic, ScopeTimeWindow, ScopeAggregateSet:
	default:
		return ErrInvalidScope
	}
	if input.StartOffset < 0 || input.EndOffset < input.StartOffset {
		return ErrNegativeOffsetRange
	}
	return nil
}

func ValidateAdvanceReplayInput(input AdvanceReplayInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.RunID) == "" {
		return ErrMissingRunID
	}
	if input.CurrentOffset < 0 {
		return ErrNegativeOffsetRange
	}
	return nil
}

func ValidatePromoteReplayInput(input PromoteReplayInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.RunID) == "" {
		return ErrMissingRunID
	}
	return nil
}

func ValidateCompleteCutoverInput(input CompleteCutoverInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.RunID) == "" {
		return ErrMissingRunID
	}
	return nil
}

func ValidateEvaluateEffectInput(input EvaluateEffectInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.RunID) == "" {
		return ErrMissingRunID
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return ErrMissingIdempotencyKey
	}
	return nil
}


package contracts

import (
	"errors"
	"strings"
)

var (
	ErrMissingHandlerID     = errors.New("handlerId is required")
	ErrMissingDependencyID  = errors.New("dependencyId is required")
	ErrMissingTenantID      = errors.New("tenantId is required")
	ErrMissingSourceService = errors.New("sourceService is required")
	ErrMissingExternalID    = errors.New("externalId is required")
	ErrMissingCorrelationID = errors.New("correlationId is required")
)

func ValidateReportSaturationInput(input ReportSaturationInput) (ReportSaturationInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.Signal.HandlerID = strings.TrimSpace(input.Signal.HandlerID)
	input.Signal.DependencyID = strings.TrimSpace(input.Signal.DependencyID)

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
	if input.Signal.HandlerID == "" {
		return input, ErrMissingHandlerID
	}
	if input.Signal.DependencyID == "" {
		return input, ErrMissingDependencyID
	}
	return input, nil
}

func ValidateReportRecoveryInput(input ReportRecoveryInput) (ReportRecoveryInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.SourceService = strings.TrimSpace(input.SourceService)
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.HandlerID = strings.TrimSpace(input.HandlerID)
	input.DependencyID = strings.TrimSpace(input.DependencyID)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.SourceService == "" {
		return input, ErrMissingSourceService
	}
	if input.HandlerID == "" {
		return input, ErrMissingHandlerID
	}
	if input.DependencyID == "" {
		return input, ErrMissingDependencyID
	}
	return input, nil
}

func ValidateGetBackpressureStateInput(input GetBackpressureStateInput) (GetBackpressureStateInput, error) {
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.HandlerID = strings.TrimSpace(input.HandlerID)
	input.DependencyID = strings.TrimSpace(input.DependencyID)

	if input.TenantID == "" {
		return input, ErrMissingTenantID
	}
	if input.HandlerID == "" {
		return input, ErrMissingHandlerID
	}
	if input.DependencyID == "" {
		return input, ErrMissingDependencyID
	}
	return input, nil
}

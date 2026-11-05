package contracts

import (
	"errors"
	"strings"
)

var (
	ErrMissingHandlerID    = errors.New("handlerId is required")
	ErrMissingDependencyID = errors.New("dependencyId is required")
	ErrMissingTenantID     = errors.New("tenantId is required")
	ErrMissingSourceService = errors.New("sourceService is required")
	ErrMissingExternalID   = errors.New("externalId is required")
	ErrMissingCorrelationID = errors.New("correlationId is required")
)

func ValidateReportSaturationInput(input ReportSaturationInput) error {
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
	if strings.TrimSpace(input.Signal.HandlerID) == "" {
		return ErrMissingHandlerID
	}
	if strings.TrimSpace(input.Signal.DependencyID) == "" {
		return ErrMissingDependencyID
	}
	return nil
}

func ValidateReportRecoveryInput(input ReportRecoveryInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.SourceService) == "" {
		return ErrMissingSourceService
	}
	if strings.TrimSpace(input.HandlerID) == "" {
		return ErrMissingHandlerID
	}
	if strings.TrimSpace(input.DependencyID) == "" {
		return ErrMissingDependencyID
	}
	return nil
}

func ValidateGetBackpressureStateInput(input GetBackpressureStateInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(input.HandlerID) == "" {
		return ErrMissingHandlerID
	}
	if strings.TrimSpace(input.DependencyID) == "" {
		return ErrMissingDependencyID
	}
	return nil
}


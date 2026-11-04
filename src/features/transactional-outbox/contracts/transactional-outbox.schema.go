package contracts

import (
	"errors"
)

var (
	ErrInvalidTenantID      = errors.New("tenantId is required")
	ErrInvalidSourceService = errors.New("sourceService is required")
	ErrInvalidExternalID    = errors.New("externalId is required")
	ErrInvalidAggregateType = errors.New("aggregateType is required")
	ErrInvalidAggregateID   = errors.New("aggregateId is required")
	ErrInvalidVersion       = errors.New("aggregateVersion must be >= 1")
	ErrInvalidEventType     = errors.New("eventType is required")
	ErrEmptyPayload         = errors.New("payload cannot be empty")
)

func ValidateCreateOutboxRecord(input CreateOutboxRecordInput) error {
	if input.TenantID == "" {
		return ErrInvalidTenantID
	}
	if input.SourceService == "" {
		return ErrInvalidSourceService
	}
	if input.ExternalID == "" {
		return ErrInvalidExternalID
	}
	if input.AggregateType == "" {
		return ErrInvalidAggregateType
	}
	if input.AggregateID == "" {
		return ErrInvalidAggregateID
	}
	if input.AggregateVersion < 1 {
		return ErrInvalidVersion
	}
	if input.EventType == "" {
		return ErrInvalidEventType
	}
	if len(input.Payload) == 0 {
		return ErrEmptyPayload
	}
	return nil
}


package contracts

import (
	"errors"
)

var (
	ErrInvalidTenantID      = errors.New("tenantId is required")
	ErrInvalidSourceService = errors.New("sourceService is required")
	ErrInvalidExternalID    = errors.New("externalId is required")
	ErrInvalidConsumerGroup = errors.New("consumerGroup is required")
	ErrInvalidHandlerID     = errors.New("handlerId is required")
	ErrInvalidEventID       = errors.New("eventId is required")
	ErrInvalidAggregateType = errors.New("aggregateType is required")
	ErrInvalidAggregateID   = errors.New("aggregateId is required")
	ErrInvalidVersion       = errors.New("aggregateVersion must be >= 1")
	ErrInvalidAttempts      = errors.New("attemptNumber must be between 1 and maxAttempts")
	ErrInvalidOrderingClass = errors.New("invalid streamOrdering class")
	ErrInvalidClass         = errors.New("invalid failureClassification")
)

func ValidateScheduleRetry(input ScheduleRetryInput) error {
	if input.TenantID == "" {
		return ErrInvalidTenantID
	}
	if input.SourceService == "" {
		return ErrInvalidSourceService
	}
	if input.ExternalID == "" {
		return ErrInvalidExternalID
	}
	if input.ConsumerGroup == "" {
		return ErrInvalidConsumerGroup
	}
	if input.HandlerID == "" {
		return ErrInvalidHandlerID
	}
	if input.EventID == "" {
		return ErrInvalidEventID
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
	if input.MaxAttempts < 1 || input.AttemptNumber < 1 || input.AttemptNumber > input.MaxAttempts {
		return ErrInvalidAttempts
	}
	switch input.Classification {
	case ClassEnvironmental, ClassContention, ClassRateLimited, ClassMessageTransient, ClassPermanentData, ClassBusinessReject:
	default:
		return ErrInvalidClass
	}
	return nil
}

func ValidateEvaluateRetry(input EvaluateRetryInput) error {
	switch input.StreamOrdering {
	case OrderingStrict, OrderingState, OrderingNone:
	default:
		return ErrInvalidOrderingClass
	}
	switch input.Classification {
	case ClassEnvironmental, ClassContention, ClassRateLimited, ClassMessageTransient, ClassPermanentData, ClassBusinessReject:
	default:
		return ErrInvalidClass
	}
	return nil
}

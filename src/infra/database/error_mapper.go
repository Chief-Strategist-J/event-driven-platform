package database

import (
	"database/sql"
	"errors"

	"github.com/lib/pq"

	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
)

var constraintRegistry = map[string]*appErrors.ApplicationError{
	constants.ConstraintConsumerInboxDedupeUK: appErrors.NewConflictError("duplicate event received for handler"),
	constants.ConstraintConsumerInboxTenantIDCK: appErrors.NewValidationError(
		"tenant_id must be non-empty and trimmed",
		types.ResponseErrorDetail{
			Field: constants.FieldTenantID,
			Issue: "tenant_id cannot contain leading/trailing whitespace or be empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerInboxHandlerIDCK: appErrors.NewValidationError(
		"handler_id must be non-empty and trimmed",
		types.ResponseErrorDetail{
			Field: constants.FieldHandlerID,
			Issue: "handler_id cannot contain leading/trailing whitespace or be empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerInboxAggregateTypeCK: appErrors.NewValidationError(
		"aggregate_type must be non-empty and trimmed",
		types.ResponseErrorDetail{
			Field: constants.FieldAggregateType,
			Issue: "aggregate_type cannot contain leading/trailing whitespace or be empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerInboxAggregateIDCK: appErrors.NewValidationError(
		"aggregate_id must be non-empty and trimmed",
		types.ResponseErrorDetail{
			Field: constants.FieldAggregateID,
			Issue: "aggregate_id cannot contain leading/trailing whitespace or be empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerInboxAggregateVersionCK: appErrors.NewValidationError(
		"aggregate_version must be >= 1",
		types.ResponseErrorDetail{
			Field: constants.FieldAggregateVersion,
			Issue: "inbox records require monotonic version >= 1",
			Rule:  constants.ValidationRuleMinValue,
		},
	),
	constants.ConstraintConsumerInboxEventTypeCK: appErrors.NewValidationError(
		"event_type must be non-empty and trimmed",
		types.ResponseErrorDetail{
			Field: constants.FieldEventType,
			Issue: "event_type cannot be empty or unnormalized",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerInboxPayloadHashCK: appErrors.NewValidationError(
		"payload_hash must be a valid 64-character lowercase hex string",
		types.ResponseErrorDetail{
			Field: constants.FieldPayloadHash,
			Issue: "payload_hash must match ^[0-9a-f]{64}$",
			Rule:  constants.ValidationRuleFormat,
		},
	),
	constants.ConstraintConsumerProjectionsTenantIDCK: appErrors.NewValidationError(
		"tenant_id cannot be empty or unnormalized on projections",
		types.ResponseErrorDetail{
			Field: constants.FieldTenantID,
			Issue: "tenant_id must be trimmed and non-empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerProjectionsHandlerIDCK: appErrors.NewValidationError(
		"handler_id cannot be empty or unnormalized on projections",
		types.ResponseErrorDetail{
			Field: constants.FieldHandlerID,
			Issue: "handler_id must be trimmed and non-empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerProjectionsAggregateTypeCK: appErrors.NewValidationError(
		"aggregate_type cannot be empty or unnormalized on projections",
		types.ResponseErrorDetail{
			Field: constants.FieldAggregateType,
			Issue: "aggregate_type must be trimmed and non-empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerProjectionsAggregateIDCK: appErrors.NewValidationError(
		"aggregate_id cannot be empty or unnormalized on projections",
		types.ResponseErrorDetail{
			Field: constants.FieldAggregateID,
			Issue: "aggregate_id must be trimmed and non-empty",
			Rule:  constants.ValidationRuleRequired,
		},
	),
	constants.ConstraintConsumerProjectionsLastVersionCK: appErrors.NewValidationError(
		"last_version cannot be negative",
		types.ResponseErrorDetail{
			Field: constants.FieldAggregateVersion,
			Issue: "projection last_version must be >= 0",
			Rule:  constants.ValidationRuleMinValue,
		},
	),
	constants.ConstraintConsumerProjectionsPayloadHashCK: appErrors.NewValidationError(
		"last_payload_hash must be empty or 64-character hex",
		types.ResponseErrorDetail{
			Field: constants.FieldPayloadHash,
			Issue: "last_payload_hash must match SHA-256 hex or empty",
			Rule:  constants.ValidationRuleFormat,
		},
	),
}

func MapDatabaseError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return appErrors.NewNotFoundError("requested record not found")
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		if mapped, exists := constraintRegistry[pqErr.Constraint]; exists {
			return mapped
		}

		switch pqErr.Code.Class() {
		case "23":
			return appErrors.NewConflictError("database integrity constraint violation: " + pqErr.Constraint)
		case "08", "53":
			return appErrors.NewTimeoutError("database connection unavailable")
		case "40":
			return appErrors.NewConflictError("concurrent transaction serialization conflict")
		}
	}

	var appErr *appErrors.ApplicationError
	if errors.As(err, &appErr) {
		return appErr
	}

	return appErrors.NewInternalError("internal database error")
}


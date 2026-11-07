package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/google/uuid"

	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
)

type ConsumerIngressRequest struct {
	EventID          string            `json:"eventId"`
	TenantID         string            `json:"tenantId"`
	HandlerID        string            `json:"handlerId"`
	AggregateType    string            `json:"aggregateType"`
	AggregateID      string            `json:"aggregateId"`
	AggregateVersion int64             `json:"aggregateVersion"`
	EventType        string            `json:"eventType"`
	SchemaVersion    string            `json:"schemaVersion"`
	SemanticClass    string            `json:"semanticClass"`
	Payload          any               `json:"payload"`
	Headers          map[string]string `json:"headers,omitempty"`
}

func ComputePayloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func ValidateInboundConsumerEvent(event InboundConsumerEvent) *errors.ApplicationError {
	var details []types.ResponseErrorDetail

	if len(strings.TrimSpace(event.EventID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldEventID,
			Issue: "event_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if trimmed := strings.TrimSpace(event.EventID); len(trimmed) > 0 {
		if _, err := uuid.Parse(trimmed); err != nil {
			details = append(details, types.ResponseErrorDetail{
				Field: constants.FieldEventID,
				Issue: "event_id must be a valid UUID",
				Rule:  constants.ValidationRuleFormat,
			})
		}
	}

	if event.SemanticClass != SemanticState && event.SemanticClass != SemanticDelta {
		details = append(details, types.ResponseErrorDetail{
			Field: "semantic_class",
			Issue: "semantic_class must be STATE or DELTA",
			Rule:  constants.ValidationRuleFormat,
		})
	}

	if len(strings.TrimSpace(event.TenantID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldTenantID,
			Issue: "tenant_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(event.HandlerID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldHandlerID,
			Issue: "handler_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(event.AggregateType)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldAggregateType,
			Issue: "aggregate_type cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(event.AggregateID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldAggregateID,
			Issue: "aggregate_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if event.AggregateVersion < 1 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldAggregateVersion,
			Issue: "aggregate_version must be >= 1",
			Rule:  constants.ValidationRuleMinValue,
		})
	}

	if len(strings.TrimSpace(event.EventType)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldEventType,
			Issue: "event_type cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(event.Payload) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldPayload,
			Issue: "payload cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(details) > 0 {
		return errors.NewValidationError("inbound consumer event validation failed", details...)
	}

	return nil
}


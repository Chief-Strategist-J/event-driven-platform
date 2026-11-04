package rules

import (
	"bytes"
	"encoding/json"
	"strings"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
)

const MaxOutboxPayloadBytes = constants.MaxOutboxPayloadBytes

func CanTransitionOutboxStatus(from contracts.OutboxStatus, to contracts.OutboxStatus) bool {
	switch from {
	case contracts.StatusPending:
		return to == contracts.StatusInFlight || to == contracts.StatusPublished || to == contracts.StatusParked || to == contracts.StatusFailed
	case contracts.StatusInFlight:
		return to == contracts.StatusPublished || to == contracts.StatusFailed || to == contracts.StatusPending || to == contracts.StatusParked
	case contracts.StatusParked:
		return to == contracts.StatusPending || to == contracts.StatusFailed
	default:
		return false
	}
}

func IsPayloadSizeValid(payload []byte) bool {
	return len(payload) > 0 && len(payload) <= MaxOutboxPayloadBytes
}

func HasFloatingPointNumbers(payload []byte) (bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()

	var obj any
	if err := decoder.Decode(&obj); err != nil {
		return false, err
	}

	return containsFloat(obj), nil
}

func containsFloat(val any) bool {
	switch v := val.(type) {
	case json.Number:
		str := v.String()
		if strings.Contains(str, ".") || strings.Contains(str, "e") || strings.Contains(str, "E") {
			return true
		}
	case map[string]any:
		for _, child := range v {
			if containsFloat(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if containsFloat(child) {
				return true
			}
		}
	}
	return false
}

func ValidateOutboxInput(input contracts.CreateOutboxRecordInput) *errors.ApplicationError {
	var details []types.ResponseErrorDetail

	if len(strings.TrimSpace(input.TenantID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldTenantID,
			Issue: "tenant_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(input.SourceService)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldSourceService,
			Issue: "source_service cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(input.AggregateType)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldAggregateType,
			Issue: "aggregate_type cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(input.AggregateID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldAggregateID,
			Issue: "aggregate_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if input.AggregateVersion < 1 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldAggregateVersion,
			Issue: "aggregate_version must be >= 1",
			Rule:  constants.ValidationRuleMinValue,
		})
	}

	if len(strings.TrimSpace(input.EventType)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldEventType,
			Issue: "event_type cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if !IsPayloadSizeValid(input.Payload) {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldPayload,
			Issue: "payload must be non-empty and <= 1MB (claim-check required)",
			Rule:  constants.ValidationRuleSizeLimit,
		})
	} else {
		hasFloat, err := HasFloatingPointNumbers(input.Payload)
		if err != nil {
			details = append(details, types.ResponseErrorDetail{
				Field: constants.FieldPayload,
				Issue: "payload is not valid JSON",
				Rule:  constants.ValidationRuleMalformedJSON,
			})
		} else if hasFloat {
			details = append(details, types.ResponseErrorDetail{
				Field: constants.FieldPayload,
				Issue: "floating-point numbers forbidden in contract (use integer minor units or decimal strings)",
				Rule:  constants.ValidationRuleNoFloats,
			})
		}
	}

	if len(details) > 0 {
		return errors.NewValidationError("outbox input validation failed", details...)
	}

	return nil
}

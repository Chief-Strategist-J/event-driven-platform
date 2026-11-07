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

type QuarantineIngressRequest struct {
	TenantID         string            `json:"tenantId"`
	SourceService    string            `json:"sourceService"`
	ExternalID       string            `json:"externalId"`
	Topic            string            `json:"topic"`
	Partition        int               `json:"partition"`
	SourceOffset     int64             `json:"sourceOffset"`
	ConsumerGroup    string            `json:"consumerGroup"`
	HandlerID        string            `json:"handlerId"`
	EventID          string            `json:"eventId"`
	AggregateType    string            `json:"aggregateType"`
	AggregateID      string            `json:"aggregateId"`
	AggregateVersion int64             `json:"aggregateVersion"`
	EventType        string            `json:"eventType"`
	Payload          any               `json:"payload"`
	Headers          map[string]string `json:"headers,omitempty"`
	FailureReason    string            `json:"failureReason"`
	ErrorCode        string            `json:"errorCode"`
}

type RedriveRequest struct {
	TenantID     string `json:"tenantId"`
	QuarantineID string `json:"quarantineId"`
}

type DiscardRequest struct {
	TenantID     string `json:"tenantId"`
	QuarantineID string `json:"quarantineId"`
	ActorID      string `json:"actorId"`
	Reason       string `json:"reason"`
}

func ComputeErrorFingerprint(errorCode, failureReason string) string {
	raw := strings.TrimSpace(errorCode) + ":" + strings.TrimSpace(failureReason)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func ValidateQuarantineInput(input QuarantineEventInput) *errors.ApplicationError {
	var details []types.ResponseErrorDetail

	if len(strings.TrimSpace(input.TenantID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldTenantID,
			Issue: "tenant_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(input.HandlerID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldHandlerID,
			Issue: "handler_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(strings.TrimSpace(input.EventID)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldEventID,
			Issue: "event_id cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if _, err := uuid.Parse(strings.TrimSpace(input.EventID)); err != nil && len(strings.TrimSpace(input.EventID)) > 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: constants.FieldEventID,
			Issue: "event_id must be a valid UUID",
			Rule:  constants.ValidationRuleFormat,
		})
	}

	if _, err := uuid.Parse(strings.TrimSpace(input.CorrelationID)); err != nil {
		details = append(details, types.ResponseErrorDetail{
			Field: "correlation_id",
			Issue: "correlation_id must be a valid UUID",
			Rule:  constants.ValidationRuleFormat,
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

	if len(strings.TrimSpace(input.FailureReason)) == 0 {
		details = append(details, types.ResponseErrorDetail{
			Field: "failure_reason",
			Issue: "failure_reason cannot be empty",
			Rule:  constants.ValidationRuleRequired,
		})
	}

	if len(details) > 0 {
		return errors.NewValidationError("quarantine input validation failed", details...)
	}

	return nil
}


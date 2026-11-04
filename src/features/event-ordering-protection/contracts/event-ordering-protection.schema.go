package contracts

import (
	"encoding/json"
	"errors"
	"strings"
)

var (
	ErrValidationFailed = errors.New("event ordering schema validation failed")
)

func ValidateIngestEventSequence(in IngestEventSequenceInput) error {
	if strings.TrimSpace(in.TenantID) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.SourceService) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.ExternalID) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.ConsumerGroup) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.HandlerID) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.AggregateType) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.AggregateID) == "" {
		return ErrValidationFailed
	}
	if in.AggregateVersion < 1 {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.EventID) == "" {
		return ErrValidationFailed
	}
	if strings.TrimSpace(in.EventType) == "" {
		return ErrValidationFailed
	}
	if len(in.Payload) == 0 {
		return ErrValidationFailed
	}
	var m map[string]any
	if err := json.Unmarshal(in.Payload, &m); err != nil || m == nil {
		return ErrValidationFailed
	}
	return nil
}


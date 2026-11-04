package client

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/shared/types"
)

var (
	ErrEventIDInvalid       = errors.New("event_id must be a valid UUIDv7")
	ErrEventTypeInvalid      = errors.New("type must follow <domain>.<aggregate>.<fact>.v<N> format")
	ErrSchemaVersionRequired = errors.New("schema_version is required")
	ErrAggregateIDRequired   = errors.New("aggregate_id is required")
	ErrAggregateSeqInvalid   = errors.New("aggregate_seq must be >= 1")
	ErrOccurredAtInvalid     = errors.New("occurred_at must be valid RFC3339 UTC timestamp")
	ErrCausationIDRequired   = errors.New("causation_id is required")
	ErrCorrelationIDRequired = errors.New("correlation_id is required")
	ErrTenantIDRequired      = errors.New("tenant_id is required")
	ErrTraceContextRequired  = errors.New("trace_context must contain valid traceId and spanId")
)

var eventTypeRegex = regexp.MustCompile(`^[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.v[0-9]+$`)

type EnvelopeValidator struct{}

func NewEnvelopeValidator() *EnvelopeValidator {
	return &EnvelopeValidator{}
}

func (v *EnvelopeValidator) Validate(env types.EventEnvelope[any]) error {
	parsedUUID, err := uuid.Parse(env.EventID)
	if err != nil || parsedUUID.Version() != 7 {
		return ErrEventIDInvalid
	}

	if !eventTypeRegex.MatchString(env.Type) {
		return ErrEventTypeInvalid
	}

	if len(env.SchemaVersion) == 0 {
		return ErrSchemaVersionRequired
	}

	if len(env.AggregateID) == 0 {
		return ErrAggregateIDRequired
	}

	if env.AggregateSeq < 1 {
		return ErrAggregateSeqInvalid
	}

	parsedTime, err := time.Parse(time.RFC3339Nano, env.OccurredAt)
	if err != nil {
		parsedTime, err = time.Parse(time.RFC3339, env.OccurredAt)
		if err != nil {
			return ErrOccurredAtInvalid
		}
	}
	if parsedTime.Location() != time.UTC && env.OccurredAt[len(env.OccurredAt)-1] != 'Z' {
		return ErrOccurredAtInvalid
	}

	if len(env.CausationID) == 0 {
		return ErrCausationIDRequired
	}

	if len(env.CorrelationID) == 0 {
		return ErrCorrelationIDRequired
	}

	if len(env.TenantID) == 0 {
		return ErrTenantIDRequired
	}

	if len(env.TraceContext.TraceID) == 0 || len(env.TraceContext.SpanID) == 0 {
		return ErrTraceContextRequired
	}

	return nil
}

func ValidateEnvelope[T any](v *EnvelopeValidator, env types.EventEnvelope[T]) error {
	anyEnv := types.EventEnvelope[any]{
		EventID:       env.EventID,
		Type:          env.Type,
		SchemaVersion: env.SchemaVersion,
		AggregateID:   env.AggregateID,
		AggregateSeq:  env.AggregateSeq,
		OccurredAt:    env.OccurredAt,
		CausationID:   env.CausationID,
		CorrelationID: env.CorrelationID,
		TenantID:      env.TenantID,
		TraceContext:  env.TraceContext,
		Payload:       env.Payload,
	}
	if err := v.Validate(anyEnv); err != nil {
		return fmt.Errorf("envelope validation failed: %w", err)
	}
	return nil
}


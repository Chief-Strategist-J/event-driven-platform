package client

import (
	"encoding/json"
	"fmt"

	"event-driven-platform/src/shared/types"
)

type EnvelopeSerializer[T any] struct {
	validator *EnvelopeValidator
}

func NewEnvelopeSerializer[T any](validator *EnvelopeValidator) *EnvelopeSerializer[T] {
	return &EnvelopeSerializer[T]{
		validator: validator,
	}
}

func (s *EnvelopeSerializer[T]) Serialize(env types.EventEnvelope[T]) ([]byte, error) {
	if s.validator != nil {
		if err := ValidateEnvelope(s.validator, env); err != nil {
			return nil, err
		}
	}
	bytes, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("serialization error: %w", err)
	}
	return bytes, nil
}

func (s *EnvelopeSerializer[T]) Deserialize(data []byte) (*types.EventEnvelope[T], error) {
	var env types.EventEnvelope[T]
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("deserialization error: %w", err)
	}
	if s.validator != nil {
		if err := ValidateEnvelope(s.validator, env); err != nil {
			return nil, err
		}
	}
	return &env, nil
}


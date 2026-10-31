package machines

import (
	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryRules "event-driven-platform/src/features/event-schema-registry/domain/rules"
	"event-driven-platform/src/shared/errors"
)

type SchemaStateMachine struct{}

func NewSchemaStateMachine() *SchemaStateMachine {
	return &SchemaStateMachine{}
}

func (m *SchemaStateMachine) TransitionTo(current registryContracts.SchemaStatus, next registryContracts.SchemaStatus) (registryContracts.SchemaStatus, error) {
	if !registryRules.CanTransitionSchemaStatus(current, next) {
		return current, errors.NewConflictError("invalid schema status transition")
	}
	return next, nil
}

package tests

import (
	"testing"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryRules "event-driven-platform/src/features/event-schema-registry/domain/rules"
)

func TestCanTransitionSchemaStatus(t *testing.T) {
	if !registryRules.CanTransitionSchemaStatus(registryContracts.SchemaStatusDraft, registryContracts.SchemaStatusActive) {
		t.Errorf("expected transition draft -> active to be valid")
	}
	if registryRules.CanTransitionSchemaStatus(registryContracts.SchemaStatusDraft, registryContracts.SchemaStatusDeprecated) {
		t.Errorf("expected transition draft -> deprecated to be invalid")
	}
}

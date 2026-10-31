package rules

import (
	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
)

func CanTransitionSchemaStatus(from registryContracts.SchemaStatus, to registryContracts.SchemaStatus) bool {
	switch from {
	case registryContracts.SchemaStatusDraft:
		return to == registryContracts.SchemaStatusActive || to == registryContracts.SchemaStatusArchived
	case registryContracts.SchemaStatusActive:
		return to == registryContracts.SchemaStatusDeprecated || to == registryContracts.SchemaStatusArchived
	case registryContracts.SchemaStatusDeprecated:
		return to == registryContracts.SchemaStatusArchived
	default:
		return false
	}
}

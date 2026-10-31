package eventschemaregistry

import (
	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryService "event-driven-platform/src/features/event-schema-registry/service"
)

type Service = registryService.SchemaRegistryService
type Schema = registryContracts.SchemaDefinition
type Status = registryContracts.SchemaStatus

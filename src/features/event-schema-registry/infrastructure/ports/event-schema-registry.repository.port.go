package ports

import (
	"context"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
)

type SchemaRegistryRepositoryPort interface {
	GetSchemaByID(ctx context.Context, tenantID string, id string) (*registryContracts.SchemaDefinition, error)
	RegisterSchema(ctx context.Context, schema *registryContracts.SchemaDefinition) error
}

package adapters

import (
	"context"
	"sync"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	"event-driven-platform/src/shared/errors"
)

type MemorySchemaRegistryAdapter struct {
	mu    sync.RWMutex
	store map[string]*registryContracts.SchemaDefinition
}

func NewMemorySchemaRegistryAdapter() *MemorySchemaRegistryAdapter {
	return &MemorySchemaRegistryAdapter{
		store: make(map[string]*registryContracts.SchemaDefinition),
	}
}

func (a *MemorySchemaRegistryAdapter) GetSchemaByID(ctx context.Context, tenantID string, id string) (*registryContracts.SchemaDefinition, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := tenantID + ":" + id
	schema, exists := a.store[key]
	if !exists {
		return nil, errors.NewNotFoundError("schema definition not found")
	}
	return schema, nil
}

func (a *MemorySchemaRegistryAdapter) RegisterSchema(ctx context.Context, schema *registryContracts.SchemaDefinition) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := schema.TenantID + ":" + schema.ID
	if _, exists := a.store[key]; exists {
		return errors.NewConflictError("schema definition already exists")
	}
	a.store[key] = schema
	return nil
}

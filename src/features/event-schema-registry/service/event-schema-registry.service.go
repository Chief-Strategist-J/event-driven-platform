package service

import (
	"context"
	"time"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryPorts "event-driven-platform/src/features/event-schema-registry/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
)

type SchemaRegistryService struct {
	repo registryPorts.SchemaRegistryRepositoryPort
}

func NewSchemaRegistryService(repo registryPorts.SchemaRegistryRepositoryPort) *SchemaRegistryService {
	return &SchemaRegistryService{
		repo: repo,
	}
}

func (s *SchemaRegistryService) RegisterSchema(ctx context.Context, input registryContracts.RegisterSchemaInput) (*registryContracts.SchemaDefinition, error) {
	if err := registryContracts.ValidateRegisterSchemaInput(input); err != nil {
		return nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	format := input.Format
	if format == "" {
		format = "json"
	}
	definition := &registryContracts.SchemaDefinition{
		ID:        input.ID,
		TenantID:  input.TenantID,
		Subject:   input.Subject,
		Version:   1,
		Format:    format,
		Schema:    input.Schema,
		Status:    registryContracts.SchemaStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.repo.RegisterSchema(ctx, definition); err != nil {
		return nil, err
	}
	return definition, nil
}

func (s *SchemaRegistryService) GetSchema(ctx context.Context, tenantID string, id string) (*registryContracts.SchemaDefinition, error) {
	if tenantID == "" || id == "" {
		return nil, errors.NewValidationError("tenantId and id are required")
	}
	return s.repo.GetSchemaByID(ctx, tenantID, id)
}

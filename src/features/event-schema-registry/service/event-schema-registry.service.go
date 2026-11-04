package service

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryPorts "event-driven-platform/src/features/event-schema-registry/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
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
	ctx, span := observability.StartSpan(ctx, "event-schema-registry.register_schema")
	defer span.End()

	span.SetAttributes(
		attribute.String("app.tenant_id", input.TenantID),
		attribute.String("schema.subject", input.Subject),
	)

	if err := registryContracts.ValidateRegisterSchemaInput(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "event-schema-registry", "validation rejected schema registration", input.TenantID, map[string]any{
			"error":   err.Error(),
			"subject": input.Subject,
		})
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
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "event-schema-registry", "failed storing schema in repository", input.TenantID, map[string]any{
			"error":   err.Error(),
			"subject": input.Subject,
			"id":      input.ID,
		})
		return nil, err
	}

	observability.LogInfo(ctx, "event-schema-registry", "schema registered successfully", input.TenantID, map[string]any{
		"id":      definition.ID,
		"subject": definition.Subject,
		"version": definition.Version,
	})

	return definition, nil
}

func (s *SchemaRegistryService) GetSchema(ctx context.Context, tenantID string, id string) (*registryContracts.SchemaDefinition, error) {
	ctx, span := observability.StartSpan(ctx, "event-schema-registry.get_schema")
	defer span.End()

	span.SetAttributes(
		attribute.String("app.tenant_id", tenantID),
		attribute.String("schema.id", id),
	)

	if tenantID == "" || id == "" {
		err := errors.NewValidationError("tenantId and id are required")
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "event-schema-registry", "missing required query parameters", tenantID, map[string]any{
			"error": err.Error(),
			"id":    id,
		})
		return nil, err
	}

	schema, err := s.repo.GetSchemaByID(ctx, tenantID, id)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		observability.LogWarn(ctx, "event-schema-registry", "schema definition not found", tenantID, map[string]any{
			"error": err.Error(),
			"id":    id,
		})
		return nil, err
	}

	return schema, nil
}

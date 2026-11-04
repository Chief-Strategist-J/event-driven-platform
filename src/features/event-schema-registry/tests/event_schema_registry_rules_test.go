package tests

import (
	"context"
	"testing"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryRules "event-driven-platform/src/features/event-schema-registry/domain/rules"
	"event-driven-platform/src/features/event-schema-registry/infrastructure/adapters"
	"event-driven-platform/src/features/event-schema-registry/service"
)

func TestEventSchemaRegistry_Tier0_Rules(t *testing.T) {
	tests := []struct {
		name          string
		fromStatus    registryContracts.SchemaStatus
		toStatus      registryContracts.SchemaStatus
		expectedValid bool
	}{
		{
			name:          "draft to active is valid",
			fromStatus:    registryContracts.SchemaStatusDraft,
			toStatus:      registryContracts.SchemaStatusActive,
			expectedValid: true,
		},
		{
			name:          "draft to deprecated is invalid",
			fromStatus:    registryContracts.SchemaStatusDraft,
			toStatus:      registryContracts.SchemaStatusDeprecated,
			expectedValid: false,
		},
		{
			name:          "active to deprecated is valid",
			fromStatus:    registryContracts.SchemaStatusActive,
			toStatus:      registryContracts.SchemaStatusDeprecated,
			expectedValid: true,
		},
		{
			name:          "deprecated to active is invalid",
			fromStatus:    registryContracts.SchemaStatusDeprecated,
			toStatus:      registryContracts.SchemaStatusActive,
			expectedValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := registryRules.CanTransitionSchemaStatus(tt.fromStatus, tt.toStatus)
			if valid != tt.expectedValid {
				t.Fatalf("expected validity %v for %s -> %s, got %v", tt.expectedValid, tt.fromStatus, tt.toStatus, valid)
			}
		})
	}
}

func TestEventSchemaRegistry_Tier0_Validation(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemorySchemaRegistryAdapter()
	svc := service.NewSchemaRegistryService(repo)

	// Missing tenant ID should trigger 4xx validation error
	_, err := svc.RegisterSchema(ctx, registryContracts.RegisterSchemaInput{
		TenantID: "",
		Subject:  "orders.created",
		Schema:   `{"type":"record"}`,
	})
	if err == nil {
		t.Fatal("expected error on empty tenant_id, got nil")
	}

	// Successful registration
	schema, err := svc.RegisterSchema(ctx, registryContracts.RegisterSchemaInput{
		ID:       "sch_ord_created_01",
		TenantID: "tenant_corp_01",
		Subject:  "orders.created",
		Schema:   `{"type":"record"}`,
	})
	if err != nil {
		t.Fatalf("unexpected error registering valid schema: %v", err)
	}
	if schema.Subject != "orders.created" {
		t.Errorf("expected subject orders.created, got %s", schema.Subject)
	}

	// Query registered schema
	fetched, err := svc.GetSchema(ctx, "tenant_corp_01", schema.ID)
	if err != nil {
		t.Fatalf("unexpected error fetching schema: %v", err)
	}
	if fetched.ID != schema.ID {
		t.Errorf("expected schema id %s, got %s", schema.ID, fetched.ID)
	}
}

package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	"event-driven-platform/src/features/event-schema-registry/infrastructure/adapters"
	"event-driven-platform/src/features/event-schema-registry/service"
)

func TestEventSchemaRegistry_ApicurioIntegration(t *testing.T) {
	apicurioURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if apicurioURL == "" {
		apicurioURL = "http://127.0.0.1:8082/apis/registry/v2"
	}

	adapter := adapters.NewApicurioSchemaRegistryAdapter(apicurioURL)
	svc := service.NewSchemaRegistryService(adapter)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tenantID := fmt.Sprintf("tenant_apicurio_%d", time.Now().UnixNano())
	schemaID := fmt.Sprintf("sch_order_%d", time.Now().UnixNano())
	schemaBody := `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"order_id":{"type":"string"}}}`

	schemaDef := &registryContracts.SchemaDefinition{
		ID:        schemaID,
		TenantID:  tenantID,
		Subject:   "orders.placed",
		Version:   1,
		Type:      "json",
		Content:   schemaBody,
		Status:    registryContracts.StatusActive,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	// 1. Register schema in Apicurio Registry
	err := svc.RegisterSchema(ctx, schemaDef)
	if err != nil {
		t.Fatalf("failed to register schema in real Apicurio Registry: %v", err)
	}

	// 2. Fetch schema back from Apicurio Registry
	retrieved, err := svc.GetSchema(ctx, tenantID, schemaID)
	if err != nil {
		t.Fatalf("failed to retrieve schema from Apicurio Registry: %v", err)
	}

	if retrieved.ID != schemaID {
		t.Errorf("expected schema ID %s, got %s", schemaID, retrieved.ID)
	}
	if retrieved.Content != schemaBody {
		t.Errorf("expected content %s, got %s", schemaBody, retrieved.Content)
	}

	// 3. Duplicate registration conflict check
	errDupe := svc.RegisterSchema(ctx, schemaDef)
	if errDupe == nil {
		t.Fatalf("expected conflict error registering duplicate schema in Apicurio, got nil")
	}
}

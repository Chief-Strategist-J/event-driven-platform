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
		t.Skip("Skipping live Apicurio integration test: SCHEMA_REGISTRY_URL not set")
	}

	adapter := adapters.NewApicurioSchemaRegistryAdapter(apicurioURL)
	svc := service.NewSchemaRegistryService(adapter)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tenantID := fmt.Sprintf("tenant_apicurio_%d", time.Now().UnixNano())
	schemaID := fmt.Sprintf("sch_order_%d", time.Now().UnixNano())
	schemaBody := `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"order_id":{"type":"string"}}}`

	input := registryContracts.RegisterSchemaInput{
		ID:       schemaID,
		TenantID: tenantID,
		Subject:  "orders.placed",
		Format:   "json",
		Schema:   schemaBody,
	}

	// 1. Register schema in real Apicurio Registry
	created, err := svc.RegisterSchema(ctx, input)
	if err != nil {
		t.Fatalf("failed to register schema in real Apicurio Registry: %v", err)
	}
	if created == nil || created.ID != schemaID {
		t.Fatalf("expected created schema ID %s, got %+v", schemaID, created)
	}

	// 2. Fetch schema back from real Apicurio Registry
	retrieved, err := svc.GetSchema(ctx, tenantID, schemaID)
	if err != nil {
		t.Fatalf("failed to retrieve schema from Apicurio Registry: %v", err)
	}

	if retrieved.ID != schemaID {
		t.Errorf("expected schema ID %s, got %s", schemaID, retrieved.ID)
	}
	if retrieved.Schema != schemaBody {
		t.Errorf("expected content %s, got %s", schemaBody, retrieved.Schema)
	}

	// 3. Duplicate registration conflict check
	_, errDupe := svc.RegisterSchema(ctx, input)
	if errDupe == nil {
		t.Fatalf("expected conflict error registering duplicate schema in Apicurio, got nil")
	}
}

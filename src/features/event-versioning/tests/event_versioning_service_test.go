package tests

import (
	"context"
	"encoding/json"
	"testing"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/infrastructure/adapters"
	"event-driven-platform/src/features/event-versioning/service"
)

func TestEventVersioningService_RegisterAndUpcast(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryEventVersioningAdapter()
	svc := service.NewEventVersioningService(repo, nil)

	svc.RegisterTransformer("order_v1_to_v2", func(p map[string]any) (map[string]any, error) {
		res := make(map[string]any)
		for k, v := range p {
			res[k] = v
		}
		if price, ok := res["price"].(float64); ok {
			res["amount_cents"] = int(price*100 + 0.5)
			res["currency"] = "USD"
			delete(res, "price")
		}
		return res, nil
	})

	svc.RegisterTransformer("order_v2_to_v3", func(p map[string]any) (map[string]any, error) {
		res := make(map[string]any)
		for k, v := range p {
			res[k] = v
		}
		res["status"] = "CONFIRMED"
		return res, nil
	})

	schemaV1 := json.RawMessage(`{"type":"object","properties":{"orderId":{"type":"string"},"price":{"type":"number"}},"required":["orderId"]}`)
	v1Input := contracts.RegisterSchemaVersionInput{
		TenantID:          "tenant_test",
		SourceService:     "order-service",
		ExternalID:        "ext_sch_01",
		StreamName:        "orders.order.v1",
		EventType:         "OrderPlaced",
		SchemaVersion:     1,
		SchemaID:          "sch_ord_v1",
		Format:            contracts.FormatJSONSchema,
		CompatibilityMode: contracts.CompatFullTransitive,
		SchemaDefinition:  schemaV1,
	}
	v1Rec, err := svc.RegisterSchemaVersion(ctx, v1Input)
	if err != nil {
		t.Fatalf("unexpected error registering schema v1: %v", err)
	}
	if v1Rec.PublicID[:4] != "sch_" {
		t.Fatalf("expected publicId with sch_ prefix, got %s", v1Rec.PublicID)
	}

	schemaV2Incompatible := json.RawMessage(`{"type":"object","properties":{"orderId":{"type":"string"},"mandatoryField":{"type":"string"}},"required":["orderId","mandatoryField"]}`)
	v2IncompatibleInput := contracts.RegisterSchemaVersionInput{
		TenantID:          "tenant_test",
		SourceService:     "order-service",
		ExternalID:        "ext_sch_02_bad",
		StreamName:        "orders.order.v1",
		EventType:         "OrderPlaced",
		SchemaVersion:     2,
		SchemaID:          "sch_ord_v2_bad",
		Format:            contracts.FormatJSONSchema,
		CompatibilityMode: contracts.CompatBackwardTransitive,
		SchemaDefinition:  schemaV2Incompatible,
	}
	_, err = svc.RegisterSchemaVersion(ctx, v2IncompatibleInput)
	if err == nil {
		t.Fatalf("expected error on incompatible backward schema with new required field, got nil")
	}

	schemaV2Compatible := json.RawMessage(`{"type":"object","properties":{"orderId":{"type":"string"},"amount_cents":{"type":"integer"},"currency":{"type":"string"}},"required":["orderId"]}`)
	v2Input := contracts.RegisterSchemaVersionInput{
		TenantID:          "tenant_test",
		SourceService:     "order-service",
		ExternalID:        "ext_sch_02",
		StreamName:        "orders.order.v1",
		EventType:         "OrderPlaced",
		SchemaVersion:     2,
		SchemaID:          "sch_ord_v2",
		Format:            contracts.FormatJSONSchema,
		CompatibilityMode: contracts.CompatFullTransitive,
		SchemaDefinition:  schemaV2Compatible,
	}
	_, err = svc.RegisterSchemaVersion(ctx, v2Input)
	if err != nil {
		t.Fatalf("unexpected error registering compatible schema v2: %v", err)
	}

	upc1, err := svc.RegisterUpcaster(ctx, contracts.RegisterUpcasterInput{
		TenantID:        "tenant_test",
		SourceService:   "order-service",
		ExternalID:      "ext_upc_01",
		EventType:       "OrderPlaced",
		FromVersion:     1,
		ToVersion:       2,
		TransformerName: "order_v1_to_v2",
	})
	if err != nil {
		t.Fatalf("unexpected error registering upcaster 1->2: %v", err)
	}
	if upc1.PublicID[:4] != "upc_" {
		t.Fatalf("expected publicId with upc_ prefix, got %s", upc1.PublicID)
	}

	_, err = svc.RegisterUpcaster(ctx, contracts.RegisterUpcasterInput{
		TenantID:        "tenant_test",
		SourceService:   "order-service",
		ExternalID:      "ext_upc_02",
		EventType:       "OrderPlaced",
		FromVersion:     2,
		ToVersion:       3,
		TransformerName: "order_v2_to_v3",
	})
	if err != nil {
		t.Fatalf("unexpected error registering upcaster 2->3: %v", err)
	}

	initialPayload := json.RawMessage(`{"orderId":"ord_001","price":19.99}`)
	upcastResult, err := svc.UpcastEvent(ctx, contracts.UpcastStepInput{
		TenantID:      "tenant_test",
		EventType:     "OrderPlaced",
		SourceVersion: 1,
		TargetVersion: 3,
		Payload:       initialPayload,
	})
	if err != nil {
		t.Fatalf("unexpected error upcasting event: %v", err)
	}

	if upcastResult.FinalVersion != 3 {
		t.Fatalf("expected final version 3, got %d", upcastResult.FinalVersion)
	}
	if upcastResult.StepsApplied != 2 {
		t.Fatalf("expected 2 steps applied, got %d", upcastResult.StepsApplied)
	}

	var parsed map[string]any
	if err := json.Unmarshal(upcastResult.Payload, &parsed); err != nil {
		t.Fatalf("failed unmarshaling upcast result: %v", err)
	}

	if parsed["orderId"] != "ord_001" {
		t.Fatalf("expected orderId ord_001, got %v", parsed["orderId"])
	}
	if parsed["amount_cents"] != float64(1999) {
		t.Fatalf("expected amount_cents 1999, got %v", parsed["amount_cents"])
	}
	if parsed["currency"] != "USD" {
		t.Fatalf("expected currency USD, got %v", parsed["currency"])
	}
	if parsed["status"] != "CONFIRMED" {
		t.Fatalf("expected status CONFIRMED, got %v", parsed["status"])
	}
	if _, exists := parsed["price"]; exists {
		t.Fatalf("expected field price to be removed by upcaster")
	}
}

package tests

import (
	"encoding/json"
	"testing"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/domain/rules"
)


func TestEventVersioning_Tier0_RegisterSchemaValidation(t *testing.T) {
	validJSON := json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}}}`)

	tests := []struct {
		name        string
		input       contracts.RegisterSchemaVersionInput
		expectValid bool
		expectedErr error
	}{
		{
			name: "valid registration input",
			input: contracts.RegisterSchemaVersionInput{
				TenantID:          "tenant_01",
				SourceService:     "order_service",
				ExternalID:        "ext_01",
				StreamName:        "orders.stream",
				EventType:         "OrderPlaced",
				SchemaVersion:     1,
				SchemaID:          "sch_01",
				Format:            contracts.FormatJSONSchema,
				CompatibilityMode: contracts.CompatFullTransitive,
				SchemaDefinition:  validJSON,
			},
			expectValid: true,
			expectedErr: nil,
		},
		{
			name: "reject blank tenant_id",
			input: contracts.RegisterSchemaVersionInput{
				TenantID:          "   ",
				SourceService:     "order_service",
				ExternalID:        "ext_01",
				StreamName:        "orders.stream",
				EventType:         "OrderPlaced",
				SchemaVersion:     1,
				SchemaID:          "sch_01",
				Format:            contracts.FormatJSONSchema,
				CompatibilityMode: contracts.CompatFullTransitive,
				SchemaDefinition:  validJSON,
			},
			expectValid: false,
			expectedErr: rules.ErrInvalidTenantID,
		},
		{
			name: "reject non-positive schema version",
			input: contracts.RegisterSchemaVersionInput{
				TenantID:          "tenant_01",
				SourceService:     "order_service",
				ExternalID:        "ext_01",
				StreamName:        "orders.stream",
				EventType:         "OrderPlaced",
				SchemaVersion:     0,
				SchemaID:          "sch_01",
				Format:            contracts.FormatJSONSchema,
				CompatibilityMode: contracts.CompatFullTransitive,
				SchemaDefinition:  validJSON,
			},
			expectValid: false,
			expectedErr: rules.ErrInvalidSchemaVersion,
		},
		{
			name: "reject unsupported wire format",
			input: contracts.RegisterSchemaVersionInput{
				TenantID:          "tenant_01",
				SourceService:     "order_service",
				ExternalID:        "ext_01",
				StreamName:        "orders.stream",
				EventType:         "OrderPlaced",
				SchemaVersion:     1,
				SchemaID:          "sch_01",
				Format:            "XML",
				CompatibilityMode: contracts.CompatFullTransitive,
				SchemaDefinition:  validJSON,
			},
			expectValid: false,
			expectedErr: rules.ErrUnsupportedFormat,
		},
		{
			name: "reject non-json-object schema definition",
			input: contracts.RegisterSchemaVersionInput{
				TenantID:          "tenant_01",
				SourceService:     "order_service",
				ExternalID:        "ext_01",
				StreamName:        "orders.stream",
				EventType:         "OrderPlaced",
				SchemaVersion:     1,
				SchemaID:          "sch_01",
				Format:            contracts.FormatJSONSchema,
				CompatibilityMode: contracts.CompatFullTransitive,
				SchemaDefinition:  json.RawMessage(`"not an object"`),
			},
			expectValid: false,
			expectedErr: rules.ErrEmptySchemaDefinition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := rules.ValidateRegisterSchemaInput(tc.input)
			if tc.expectValid && err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
			if !tc.expectValid {
				if err == nil {
					t.Fatalf("expected error %v, got nil", tc.expectedErr)
				}
				if err != tc.expectedErr {
					t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
				}
			}
		})
	}
}

func TestEventVersioning_Tier0_UpcasterSequentialStepsValidation(t *testing.T) {
	tests := []struct {
		name        string
		input       contracts.RegisterUpcasterInput
		expectValid bool
		expectedErr error
	}{
		{
			name: "valid sequential step v1 to v2",
			input: contracts.RegisterUpcasterInput{
				TenantID:        "tenant_01",
				SourceService:   "order_service",
				ExternalID:      "ext_upc_01",
				EventType:       "OrderPlaced",
				FromVersion:     1,
				ToVersion:       2,
				TransformerName: "order_v1_to_v2",
			},
			expectValid: true,
			expectedErr: nil,
		},
		{
			name: "reject non-sequential step v1 to v3",
			input: contracts.RegisterUpcasterInput{
				TenantID:        "tenant_01",
				SourceService:   "order_service",
				ExternalID:      "ext_upc_01",
				EventType:       "OrderPlaced",
				FromVersion:     1,
				ToVersion:       3,
				TransformerName: "order_v1_to_v3",
			},
			expectValid: false,
			expectedErr: rules.ErrNonSequentialUpcaster,
		},
		{
			name: "reject backward step v2 to v1",
			input: contracts.RegisterUpcasterInput{
				TenantID:        "tenant_01",
				SourceService:   "order_service",
				ExternalID:      "ext_upc_01",
				EventType:       "OrderPlaced",
				FromVersion:     2,
				ToVersion:       1,
				TransformerName: "order_v2_to_v1",
			},
			expectValid: false,
			expectedErr: rules.ErrNonSequentialUpcaster,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := rules.ValidateUpcasterDefinition(tc.input)
			if tc.expectValid && err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
			if !tc.expectValid && err != tc.expectedErr {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestEventVersioning_Tier0_CompatibilityEvaluation(t *testing.T) {
	v1 := json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	v2Backward := json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"email":{"type":"string"}},"required":["id"]}`)
	v2Breaking := json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"email":{"type":"string"}},"required":["id","email"]}`)

	
	resBreaking := rules.EvaluateJSONSchemaCompatibility(v1, v2Breaking, contracts.CompatBackwardTransitive)
	if resBreaking.IsCompatible {
		t.Errorf("expected backward compatibility rejection when adding new required property")
	}

	
	resCompatible := rules.EvaluateJSONSchemaCompatibility(v1, v2Backward, contracts.CompatBackwardTransitive)
	if !resCompatible.IsCompatible {
		t.Errorf("expected backward compatibility success for optional property addition, got: %v", resCompatible.Violations)
	}
}

package tests

import (
	"encoding/json"
	"errors"
	"testing"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/domain/machines"
)

// Tier 0 Domain State Machine Table-Driven Tests (< 1ms, zero I/O)
func TestUpcasterEngine_Tier0_ExecuteChain(t *testing.T) {
	tests := []struct {
		name          string
		transformers  map[string]machines.UpcastTransformerFunc
		chain         []contracts.UpcasterRecord
		input         contracts.UpcastStepInput
		wantErr       bool
		expectedErr   error
		wantSteps     int
		wantFinalVer  int
		assertPayload func(t *testing.T, payload json.RawMessage)
	}{
		{
			name: "No-op when target version equals source version",
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 1,
				Payload:       json.RawMessage(`{"orderId":"101"}`),
			},
			wantSteps:    0,
			wantFinalVer: 1,
			assertPayload: func(t *testing.T, p json.RawMessage) {
				if string(p) != `{"orderId":"101"}` {
					t.Fatalf("expected payload unchanged, got %s", string(p))
				}
			},
		},
		{
			name: "Single-step sequential upcast v1 to v2",
			transformers: map[string]machines.UpcastTransformerFunc{
				"v1_to_v2": func(p map[string]any) (map[string]any, error) {
					res := make(map[string]any)
					for k, v := range p {
						res[k] = v
					}
					res["currency"] = "USD"
					return res, nil
				},
			},
			chain: []contracts.UpcasterRecord{
				{FromVersion: 1, ToVersion: 2, TransformerName: "v1_to_v2", IsActive: true},
			},
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 2,
				Payload:       json.RawMessage(`{"orderId":"102","price":99.5}`),
			},
			wantSteps:    1,
			wantFinalVer: 2,
			assertPayload: func(t *testing.T, p json.RawMessage) {
				var parsed map[string]any
				if err := json.Unmarshal(p, &parsed); err != nil {
					t.Fatalf("failed unmarshaling result: %v", err)
				}
				if parsed["currency"] != "USD" {
					t.Fatalf("expected currency USD, got %v", parsed["currency"])
				}
			},
		},
		{
			name: "Multi-step chained upcast v1 to v3 with field deprecation and translation",
			transformers: map[string]machines.UpcastTransformerFunc{
				"v1_to_v2": func(p map[string]any) (map[string]any, error) {
					res := make(map[string]any)
					for k, v := range p {
						res[k] = v
					}
					if amt, ok := res["amount"].(float64); ok {
						res["amount_cents"] = int64(amt*100 + 0.5)
						delete(res, "amount")
					}
					return res, nil
				},
				"v2_to_v3": func(p map[string]any) (map[string]any, error) {
					res := make(map[string]any)
					for k, v := range p {
						res[k] = v
					}
					res["schema_rev"] = "v3-migrated"
					return res, nil
				},
			},
			chain: []contracts.UpcasterRecord{
				{FromVersion: 1, ToVersion: 2, TransformerName: "v1_to_v2", IsActive: true},
				{FromVersion: 2, ToVersion: 3, TransformerName: "v2_to_v3", IsActive: true},
			},
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 3,
				Payload:       json.RawMessage(`{"orderId":"103","amount":42.50}`),
			},
			wantSteps:    2,
			wantFinalVer: 3,
			assertPayload: func(t *testing.T, p json.RawMessage) {
				var parsed map[string]any
				if err := json.Unmarshal(p, &parsed); err != nil {
					t.Fatalf("failed unmarshaling: %v", err)
				}
				if _, hasOld := parsed["amount"]; hasOld {
					t.Fatalf("expected old field 'amount' to be purged in v1->v2")
				}
				if cents, ok := parsed["amount_cents"].(float64); !ok || cents != 4250 {
					t.Fatalf("expected amount_cents 4250, got %v", parsed["amount_cents"])
				}
				if parsed["schema_rev"] != "v3-migrated" {
					t.Fatalf("expected schema_rev 'v3-migrated', got %v", parsed["schema_rev"])
				}
			},
		},
		{
			name: "Fails when intermediate step is missing in chain",
			transformers: map[string]machines.UpcastTransformerFunc{
				"v1_to_v2": func(p map[string]any) (map[string]any, error) { return p, nil },
			},
			chain: []contracts.UpcasterRecord{
				{FromVersion: 1, ToVersion: 2, TransformerName: "v1_to_v2", IsActive: true},
				// missing step 2 -> 3
			},
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 3,
				Payload:       json.RawMessage(`{"orderId":"104"}`),
			},
			wantErr: true,
		},
		{
			name: "Fails when step transformer is unregistered",
			transformers: map[string]machines.UpcastTransformerFunc{},
			chain: []contracts.UpcasterRecord{
				{FromVersion: 1, ToVersion: 2, TransformerName: "unregistered_step", IsActive: true},
			},
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 2,
				Payload:       json.RawMessage(`{"orderId":"105"}`),
			},
			wantErr:     true,
			expectedErr: machines.ErrTransformerNotFound,
		},
		{
			name: "Fails when transformer returns domain transformation error",
			transformers: map[string]machines.UpcastTransformerFunc{
				"failing_step": func(p map[string]any) (map[string]any, error) {
					return nil, errors.New("corrupt legacy payload")
				},
			},
			chain: []contracts.UpcasterRecord{
				{FromVersion: 1, ToVersion: 2, TransformerName: "failing_step", IsActive: true},
			},
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 2,
				Payload:       json.RawMessage(`{"orderId":"106"}`),
			},
			wantErr: true,
		},
		{
			name: "Skips inactive upcasters in chain",
			transformers: map[string]machines.UpcastTransformerFunc{
				"v1_to_v2": func(p map[string]any) (map[string]any, error) { return p, nil },
			},
			chain: []contracts.UpcasterRecord{
				{FromVersion: 1, ToVersion: 2, TransformerName: "v1_to_v2", IsActive: false},
			},
			input: contracts.UpcastStepInput{
				TenantID:      "tenant_test",
				EventType:     "OrderPlaced",
				SourceVersion: 1,
				TargetVersion: 2,
				Payload:       json.RawMessage(`{"orderId":"107"}`),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine := machines.NewUpcasterEngine()
			for name, fn := range tc.transformers {
				engine.RegisterTransformer(name, fn)
			}

			res, err := engine.ExecuteChain(tc.input, tc.chain)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil result: %+v", res)
				}
				if tc.expectedErr != nil && !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected error wrapping %v, got %v", tc.expectedErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error executing chain: %v", err)
			}
			if res.FinalVersion != tc.wantFinalVer {
				t.Errorf("expected final version %d, got %d", tc.wantFinalVer, res.FinalVersion)
			}
			if res.StepsApplied != tc.wantSteps {
				t.Errorf("expected %d steps applied, got %d", tc.wantSteps, res.StepsApplied)
			}
			if tc.assertPayload != nil {
				tc.assertPayload(t, res.Payload)
			}
		})
	}
}

// Tier 4 Performance Benchmark for Hot-Path Upcast Loop
func BenchmarkUpcasterEngine_ExecuteChain(b *testing.B) {
	engine := machines.NewUpcasterEngine()
	engine.RegisterTransformer("v1_to_v2", func(p map[string]any) (map[string]any, error) {
		p["migrated"] = true
		return p, nil
	})
	engine.RegisterTransformer("v2_to_v3", func(p map[string]any) (map[string]any, error) {
		p["version"] = 3
		return p, nil
	})

	chain := []contracts.UpcasterRecord{
		{FromVersion: 1, ToVersion: 2, TransformerName: "v1_to_v2", IsActive: true},
		{FromVersion: 2, ToVersion: 3, TransformerName: "v2_to_v3", IsActive: true},
	}

	input := contracts.UpcastStepInput{
		TenantID:      "tenant_bench",
		EventType:     "OrderCreated",
		SourceVersion: 1,
		TargetVersion: 3,
		Payload:       json.RawMessage(`{"orderId":"ord_bench_001","amount":199.99,"customer":"alice"}`),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := engine.ExecuteChain(input, chain)
		if err != nil || res.FinalVersion != 3 {
			b.Fatalf("benchmark step failed: %v", err)
		}
	}
}

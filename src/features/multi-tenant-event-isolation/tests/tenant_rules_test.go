package tests

import (
	"testing"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/rules"
)

func TestTenantRules_ValidateEnvelopeConsistency(t *testing.T) {
	t.Run("ValidMatchingEnvelopePasses", func(t *testing.T) {
		input := contracts.VerifyEnvelopeInput{
			HeaderTenant:  "tenant_acme",
			KeyPrefix:     "tenant_acme",
			PayloadTenant: "tenant_acme",
			CurrentRegion: "us-east-1",
		}
		err := rules.ValidateEnvelopeConsistency(input)
		if err != nil {
			t.Fatalf("expected valid envelope to pass, got: %v", err)
		}
	})

	t.Run("HeaderMismatchFails", func(t *testing.T) {
		input := contracts.VerifyEnvelopeInput{
			HeaderTenant:  "tenant_attacker",
			KeyPrefix:     "tenant_acme",
			PayloadTenant: "tenant_acme",
			CurrentRegion: "us-east-1",
		}
		err := rules.ValidateEnvelopeConsistency(input)
		if err != rules.ErrTenantHeaderPayloadMismatch {
			t.Fatalf("expected ErrTenantHeaderPayloadMismatch, got: %v", err)
		}
	})

	t.Run("KeyPrefixMismatchFails", func(t *testing.T) {
		input := contracts.VerifyEnvelopeInput{
			HeaderTenant:  "tenant_acme",
			KeyPrefix:     "tenant_other",
			PayloadTenant: "tenant_acme",
			CurrentRegion: "us-east-1",
		}
		err := rules.ValidateEnvelopeConsistency(input)
		if err != rules.ErrTenantKeyPayloadMismatch {
			t.Fatalf("expected ErrTenantKeyPayloadMismatch, got: %v", err)
		}
	})
}

func TestTenantRules_ValidateResidency(t *testing.T) {
	allowed := []string{"us-east-1", "us-west-2"}

	t.Run("AllowedRegionSucceeds", func(t *testing.T) {
		err := rules.ValidateResidency(allowed, "us-east-1")
		if err != nil {
			t.Fatalf("expected allowed region to pass, got: %v", err)
		}
	})

	t.Run("DisallowedRegionFails", func(t *testing.T) {
		err := rules.ValidateResidency(allowed, "eu-central-1")
		if err == nil {
			t.Fatalf("expected residency failure for unlisted region, got nil")
		}
	})
}

func TestTenantRules_EvaluateQuotaBudget(t *testing.T) {
	t.Run("WithinBudgetPasses", func(t *testing.T) {
		res := rules.EvaluateQuotaBudget(50, 1000, 10, 500)
		if !res.Allowed || res.Throttled {
			t.Fatalf("expected allowed within budget, got throttled: %v", res)
		}
	})

	t.Run("HeldKeysExceededThrottles", func(t *testing.T) {
		res := rules.EvaluateQuotaBudget(1001, 1000, 0, 500)
		if res.Allowed || !res.Throttled {
			t.Fatalf("expected throttled when held keys exceed max, got: %v", res)
		}
	})

	t.Run("QuarantineBudgetExceededThrottles", func(t *testing.T) {
		res := rules.EvaluateQuotaBudget(10, 1000, 501, 500)
		if res.Allowed || !res.Throttled {
			t.Fatalf("expected throttled when quarantine exceeds max, got: %v", res)
		}
	})
}


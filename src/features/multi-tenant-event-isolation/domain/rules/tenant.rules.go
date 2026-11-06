package rules

import (
	"fmt"
	"strings"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrTenantHeaderPayloadMismatch = errors.NewValidationError("tenant header does not match payload tenant (MT-02)")
	ErrTenantKeyPayloadMismatch    = errors.NewValidationError("tenant key prefix does not match payload tenant (MT-02)")
	ErrResidencyViolation          = errors.NewValidationError("record destination violates tenant data residency allowlist (MT-08)")
	ErrTenantOffboarded            = errors.NewConflictError("tenant is offboarded; data resurrected is forbidden (F6.3)")
	ErrTenantQuotaExceeded         = errors.NewConflictError("tenant noisy-neighbor quota exceeded (MT-11)")
)

func ValidateEnvelopeConsistency(input contracts.VerifyEnvelopeInput) error {
	if input.HeaderTenant != input.PayloadTenant {
		return ErrTenantHeaderPayloadMismatch
	}
	if input.KeyPrefix != input.PayloadTenant {
		return ErrTenantKeyPayloadMismatch
	}
	return nil
}

func ValidateResidency(allowedRegions []string, currentRegion string) error {
	target := strings.TrimSpace(currentRegion)
	for _, allowed := range allowedRegions {
		if strings.EqualFold(strings.TrimSpace(allowed), target) {
			return nil
		}
	}
	return fmt.Errorf("%w: current region %s not in allowed list %v", ErrResidencyViolation, currentRegion, allowedRegions)
}

func EvaluateLifecycleState(state contracts.TenantLifecycleState) (canProcess bool, shouldSkip bool, err error) {
	switch state {
	case contracts.LifecycleActive:
		return true, false, nil
	case contracts.LifecycleSuspended:
		return true, false, nil
	case contracts.LifecycleOnboarding:
		return true, false, nil
	case contracts.LifecycleOffboarding, contracts.LifecycleOffboarded:
		return false, true, ErrTenantOffboarded
	default:
		return false, false, errors.NewValidationError(fmt.Sprintf("unrecognized tenant lifecycle state: %s", state))
	}
}

func EvaluateQuotaBudget(heldKeys int, maxHeldKeys int, quarantineCount int, maxQuarantine int) contracts.QuotaCheckResult {
	throttled := false
	reason := ""

	if heldKeys >= maxHeldKeys {
		throttled = true
		reason = fmt.Sprintf("held keys count %d exceeds maximum budget %d", heldKeys, maxHeldKeys)
	} else if quarantineCount >= maxQuarantine {
		throttled = true
		reason = fmt.Sprintf("quarantine items count %d exceeds maximum budget %d", quarantineCount, maxQuarantine)
	}

	return contracts.QuotaCheckResult{
		Allowed:            !throttled,
		Throttled:          throttled,
		CurrentHeldKeys:    heldKeys,
		MaxHeldKeys:        maxHeldKeys,
		QuarantineCount:    quarantineCount,
		MaxQuarantineLimit: maxQuarantine,
		ViolationReason:    reason,
	}
}

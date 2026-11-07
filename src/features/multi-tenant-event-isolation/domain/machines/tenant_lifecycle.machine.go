package machines

import (
	"fmt"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrInvalidLifecycleTransition = errors.NewConflictError("invalid tenant lifecycle transition")
	ErrTombstonedTenantImmutable  = errors.NewConflictError("offboarded tenant is permanently tombstoned (MT-12)")
)

type TenantLifecycleMachine struct{}

func NewTenantLifecycleMachine() *TenantLifecycleMachine {
	return &TenantLifecycleMachine{}
}

func isKnownLifecycleState(state contracts.TenantLifecycleState) bool {
	switch state {
	case contracts.LifecycleOnboarding, contracts.LifecycleActive, contracts.LifecycleSuspended,
		contracts.LifecycleOffboarding, contracts.LifecycleOffboarded:
		return true
	}
	return false
}

func (m *TenantLifecycleMachine) Transition(current contracts.TenantLifecycleState, target contracts.TenantLifecycleState) (contracts.TenantLifecycleState, error) {
	if !isKnownLifecycleState(target) {
		return current, errors.NewValidationError(fmt.Sprintf("unknown target_state: %q", string(target)))
	}

	if current == contracts.LifecycleOffboarded {
		return current, ErrTombstonedTenantImmutable
	}

	if current == target {
		return current, nil
	}

	switch current {
	case contracts.LifecycleOnboarding:
		if target == contracts.LifecycleActive || target == contracts.LifecycleOffboarded {
			return target, nil
		}
	case contracts.LifecycleActive:
		if target == contracts.LifecycleSuspended || target == contracts.LifecycleOffboarding {
			return target, nil
		}
	case contracts.LifecycleSuspended:
		if target == contracts.LifecycleActive || target == contracts.LifecycleOffboarding {
			return target, nil
		}
	case contracts.LifecycleOffboarding:
		if target == contracts.LifecycleOffboarded {
			return target, nil
		}
	}

	return current, fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidLifecycleTransition, current, target)
}

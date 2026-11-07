package tests

import (
	"errors"
	"testing"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/machines"
	errs "event-driven-platform/src/shared/errors"
)

func TestTenantLifecycleMachine_ValidTransitions(t *testing.T) {
	m := machines.NewTenantLifecycleMachine()

	state, err := m.Transition(contracts.LifecycleOnboarding, contracts.LifecycleActive)
	if err != nil || state != contracts.LifecycleActive {
		t.Fatalf("expected onboarding -> active, got state: %s, err: %v", state, err)
	}

	state, err = m.Transition(contracts.LifecycleActive, contracts.LifecycleSuspended)
	if err != nil || state != contracts.LifecycleSuspended {
		t.Fatalf("expected active -> suspended, got state: %s, err: %v", state, err)
	}

	state, err = m.Transition(contracts.LifecycleSuspended, contracts.LifecycleActive)
	if err != nil || state != contracts.LifecycleActive {
		t.Fatalf("expected suspended -> active, got state: %s, err: %v", state, err)
	}

	state, err = m.Transition(contracts.LifecycleActive, contracts.LifecycleOffboarding)
	if err != nil || state != contracts.LifecycleOffboarding {
		t.Fatalf("expected active -> offboarding, got state: %s, err: %v", state, err)
	}

	state, err = m.Transition(contracts.LifecycleOffboarding, contracts.LifecycleOffboarded)
	if err != nil || state != contracts.LifecycleOffboarded {
		t.Fatalf("expected offboarding -> offboarded, got state: %s, err: %v", state, err)
	}
}

func TestTenantLifecycleMachine_TombstonePermanence(t *testing.T) {
	m := machines.NewTenantLifecycleMachine()

	_, err := m.Transition(contracts.LifecycleOffboarded, contracts.LifecycleActive)
	if err != machines.ErrTombstonedTenantImmutable {
		t.Fatalf("expected ErrTombstonedTenantImmutable on offboarded reactivation, got: %v", err)
	}

	_, err = m.Transition(contracts.LifecycleOffboarded, contracts.LifecycleOnboarding)
	if err != machines.ErrTombstonedTenantImmutable {
		t.Fatalf("expected ErrTombstonedTenantImmutable on offboarded onboarding attempt, got: %v", err)
	}
}

func TestTenantLifecycleMachine_InvalidTransitions(t *testing.T) {
	m := machines.NewTenantLifecycleMachine()

	_, err := m.Transition(contracts.LifecycleOnboarding, contracts.LifecycleSuspended)
	if err == nil {
		t.Fatalf("expected error transitioning directly from onboarding to suspended, got nil")
	}

	_, err = m.Transition(contracts.LifecycleSuspended, contracts.LifecycleOffboarded)
	if err == nil {
		t.Fatalf("expected error skipping offboarding state directly to offboarded, got nil")
	}
}


func TestTenantLifecycleMachine_UnknownTargetIsValidationError(t *testing.T) {
	m := machines.NewTenantLifecycleMachine()

	_, err := m.Transition(contracts.LifecycleActive, contracts.TenantLifecycleState("BOGUS"))
	var appErr *errs.ApplicationError
	if !errors.As(err, &appErr) || appErr.StatusCode != 400 {
		t.Fatalf("expected 400 validation error for unknown target, got %v", err)
	}

	_, err = m.Transition(contracts.LifecycleActive, contracts.LifecycleOnboarding)
	if !errors.As(err, &appErr) || appErr.StatusCode != 409 {
		t.Fatalf("expected 409 conflict for invalid known transition, got %v", err)
	}
}

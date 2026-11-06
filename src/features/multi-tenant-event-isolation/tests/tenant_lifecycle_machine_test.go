package tests

import (
	"testing"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/machines"
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


package tests

import (
	"testing"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/machines"
)

func TestCDCConnectorMachine_ValidTransitions(t *testing.T) {
	m := machines.NewCDCConnectorMachine()

	state, err := m.Transition(contracts.StateUnregistered, contracts.StateProvisioning)
	if err != nil || state != contracts.StateProvisioning {
		t.Fatalf("expected unregistered -> provisioning, got: %s, %v", state, err)
	}

	state, err = m.Transition(contracts.StateProvisioning, contracts.StateRunning)
	if err != nil || state != contracts.StateRunning {
		t.Fatalf("expected provisioning -> running, got: %s, %v", state, err)
	}

	state, err = m.Transition(contracts.StateRunning, contracts.StatePaused)
	if err != nil || state != contracts.StatePaused {
		t.Fatalf("expected running -> paused, got: %s, %v", state, err)
	}

	state, err = m.Transition(contracts.StatePaused, contracts.StateRunning)
	if err != nil || state != contracts.StateRunning {
		t.Fatalf("expected paused -> running, got: %s, %v", state, err)
	}

	state, err = m.Transition(contracts.StateRunning, contracts.StateDecommissioned)
	if err != nil || state != contracts.StateDecommissioned {
		t.Fatalf("expected running -> decommissioned, got: %s, %v", state, err)
	}
}

func TestCDCConnectorMachine_DecommissionPermanence(t *testing.T) {
	m := machines.NewCDCConnectorMachine()

	_, err := m.Transition(contracts.StateDecommissioned, contracts.StateRunning)
	if err != machines.ErrDecommissionedImmutable {
		t.Fatalf("expected ErrDecommissionedImmutable, got: %v", err)
	}
}

func TestCDCConnectorMachine_InvalidTransitions(t *testing.T) {
	m := machines.NewCDCConnectorMachine()

	_, err := m.Transition(contracts.StateUnregistered, contracts.StateRunning)
	if err == nil {
		t.Fatalf("expected error on invalid unregistered -> running, got nil")
	}
}


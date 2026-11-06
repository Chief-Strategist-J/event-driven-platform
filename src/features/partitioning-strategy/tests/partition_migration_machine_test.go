package tests

import (
	"testing"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/domain/machines"
)

func TestPartitionMigrationMachine_ValidTransitions(t *testing.T) {
	sm := machines.NewMigrationStateMachine()

	phase, err := sm.Transition(contracts.MigrationPhasePlanned, contracts.MigrationPhaseProvisioningTarget)
	if err != nil || phase != contracts.MigrationPhaseProvisioningTarget {
		t.Fatalf("expected transition to PROVISIONING_TARGET, got %v, err: %v", phase, err)
	}

	phase, err = sm.Transition(phase, contracts.MigrationPhaseDualPublishing)
	if err != nil || phase != contracts.MigrationPhaseDualPublishing {
		t.Fatalf("expected transition to DUAL_PUBLISHING, got %v, err: %v", phase, err)
	}

	phase, err = sm.Transition(phase, contracts.MigrationPhaseDrainingSource)
	if err != nil || phase != contracts.MigrationPhaseDrainingSource {
		t.Fatalf("expected transition to DRAINING_SOURCE, got %v, err: %v", phase, err)
	}

	phase, err = sm.Transition(phase, contracts.MigrationPhaseCutover)
	if err != nil || phase != contracts.MigrationPhaseCutover {
		t.Fatalf("expected transition to CUTOVER, got %v, err: %v", phase, err)
	}

	phase, err = sm.Transition(phase, contracts.MigrationPhaseDecommissioned)
	if err != nil || phase != contracts.MigrationPhaseDecommissioned {
		t.Fatalf("expected transition to DECOMMISSIONED, got %v, err: %v", phase, err)
	}
}

func TestPartitionMigrationMachine_DecommissionPermanence(t *testing.T) {
	sm := machines.NewMigrationStateMachine()

	_, err := sm.Transition(contracts.MigrationPhaseDecommissioned, contracts.MigrationPhasePlanned)
	if err == nil {
		t.Fatalf("expected transition from DECOMMISSIONED to fail")
	}
}

func TestPartitionMigrationMachine_InvalidTransitions(t *testing.T) {
	sm := machines.NewMigrationStateMachine()

	_, err := sm.Transition(contracts.MigrationPhasePlanned, contracts.MigrationPhaseCutover)
	if err == nil {
		t.Fatalf("expected invalid skip to CUTOVER to be rejected")
	}
}


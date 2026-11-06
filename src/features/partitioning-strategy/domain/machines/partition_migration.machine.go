package machines

import (
	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrInvalidMigrationTransition = errors.NewValidationError("invalid migration phase transition")
	ErrMigrationTerminalState     = errors.NewConflictError("migration runbook is in terminal decommissioned state")
)

type MigrationStateMachine struct{}

func NewMigrationStateMachine() *MigrationStateMachine {
	return &MigrationStateMachine{}
}

func (m *MigrationStateMachine) Transition(current contracts.MigrationPhase, target contracts.MigrationPhase) (contracts.MigrationPhase, error) {
	if current == contracts.MigrationPhaseDecommissioned {
		return current, ErrMigrationTerminalState
	}

	validTransitions := map[contracts.MigrationPhase][]contracts.MigrationPhase{
		contracts.MigrationPhasePlanned: {
			contracts.MigrationPhaseProvisioningTarget,
		},
		contracts.MigrationPhaseProvisioningTarget: {
			contracts.MigrationPhaseDualPublishing,
			contracts.MigrationPhaseDecommissioned,
		},
		contracts.MigrationPhaseDualPublishing: {
			contracts.MigrationPhaseDrainingSource,
			contracts.MigrationPhaseDecommissioned,
		},
		contracts.MigrationPhaseDrainingSource: {
			contracts.MigrationPhaseCutover,
			contracts.MigrationPhaseDualPublishing,
		},
		contracts.MigrationPhaseCutover: {
			contracts.MigrationPhaseDecommissioned,
		},
	}

	allowed, exists := validTransitions[current]
	if !exists {
		return current, ErrInvalidMigrationTransition
	}

	for _, next := range allowed {
		if next == target {
			return target, nil
		}
	}

	return current, ErrInvalidMigrationTransition
}

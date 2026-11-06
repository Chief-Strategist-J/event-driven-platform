package machines

import (
	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrInvalidRebuildTransition = errors.NewValidationError("invalid view rebuild phase transition")
	ErrRebuildTerminalState     = errors.NewConflictError("view rebuild is in terminal decommissioned state")
)

type ViewRebuildStateMachine struct{}

func NewViewRebuildStateMachine() *ViewRebuildStateMachine {
	return &ViewRebuildStateMachine{}
}

func (m *ViewRebuildStateMachine) Transition(current contracts.RebuildPhase, target contracts.RebuildPhase) (contracts.RebuildPhase, error) {
	if current == contracts.RebuildPhaseDecommissioned {
		return current, ErrRebuildTerminalState
	}

	validTransitions := map[contracts.RebuildPhase][]contracts.RebuildPhase{
		contracts.RebuildPhaseInitiated: {
			contracts.RebuildPhaseHistoricalReplay,
			contracts.RebuildPhaseDecommissioned,
		},
		contracts.RebuildPhaseHistoricalReplay: {
			contracts.RebuildPhaseStreamCatchup,
			contracts.RebuildPhaseDecommissioned,
		},
		contracts.RebuildPhaseStreamCatchup: {
			contracts.RebuildPhasePromoted,
			contracts.RebuildPhaseDecommissioned,
		},
		contracts.RebuildPhasePromoted: {
			contracts.RebuildPhaseDecommissioned,
		},
	}

	allowed, exists := validTransitions[current]
	if !exists {
		return current, ErrInvalidRebuildTransition
	}

	for _, next := range allowed {
		if next == target {
			return target, nil
		}
	}

	return current, ErrInvalidRebuildTransition
}


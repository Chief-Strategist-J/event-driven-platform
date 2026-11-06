package machines

import (
	"fmt"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrInvalidConnectorTransition = errors.NewConflictError("invalid cdc connector state transition")
	ErrDecommissionedImmutable    = errors.NewConflictError("decommissioned cdc connector cannot be reactivated")
)

type CDCConnectorMachine struct{}

func NewCDCConnectorMachine() *CDCConnectorMachine {
	return &CDCConnectorMachine{}
}

func (m *CDCConnectorMachine) Transition(current contracts.CDCConnectorState, target contracts.CDCConnectorState) (contracts.CDCConnectorState, error) {
	if current == contracts.StateDecommissioned {
		return current, ErrDecommissionedImmutable
	}

	if current == target {
		return current, nil
	}

	switch current {
	case contracts.StateUnregistered:
		if target == contracts.StateProvisioning || target == contracts.StateDecommissioned {
			return target, nil
		}
	case contracts.StateProvisioning:
		if target == contracts.StateRunning || target == contracts.StateFailed || target == contracts.StateDecommissioned {
			return target, nil
		}
	case contracts.StateRunning:
		if target == contracts.StatePaused || target == contracts.StateFailed || target == contracts.StateDecommissioned {
			return target, nil
		}
	case contracts.StatePaused:
		if target == contracts.StateRunning || target == contracts.StateFailed || target == contracts.StateDecommissioned {
			return target, nil
		}
	case contracts.StateFailed:
		if target == contracts.StateRunning || target == contracts.StateProvisioning || target == contracts.StateDecommissioned {
			return target, nil
		}
	}

	return current, fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidConnectorTransition, current, target)
}


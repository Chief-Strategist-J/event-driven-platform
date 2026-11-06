package machines

import (
	"errors"
	"event-driven-platform/src/features/event-operations-dashboard/contracts"
)

var (
	ErrIllegalHopTransition = errors.New("illegal pipeline hop state transition")
)

var AllowedHopTransitions = map[contracts.HopStatus]map[contracts.HopStatus]bool{
	contracts.StatusOk: {
		contracts.StatusLagging: true,
		contracts.StatusPaused:  true,
		contracts.StatusStalled: true,
		contracts.StatusStopped: true,
		contracts.StatusUnknown: true,
	},
	contracts.StatusLagging: {
		contracts.StatusOk:      true,
		contracts.StatusPaused:  true,
		contracts.StatusStalled: true,
		contracts.StatusStopped: true,
		contracts.StatusUnknown: true,
	},
	contracts.StatusPaused: {
		contracts.StatusOk:      true,
		contracts.StatusLagging: true,
		contracts.StatusStopped: true,
		contracts.StatusUnknown: true,
	},
	contracts.StatusStalled: {
		contracts.StatusOk:      true,
		contracts.StatusPaused:  true,
		contracts.StatusStopped: true,
		contracts.StatusUnknown: true,
	},
	contracts.StatusStopped: {
		contracts.StatusOk:      true,
		contracts.StatusLagging: true,
		contracts.StatusUnknown: true,
	},
	contracts.StatusUnknown: {
		contracts.StatusOk:      true,
		contracts.StatusLagging: true,
		contracts.StatusPaused:  true,
		contracts.StatusStalled: true,
		contracts.StatusStopped: true,
	},
}

func CanTransitionHopStatus(current, next contracts.HopStatus) bool {
	if current == next {
		return true
	}
	allowed, exists := AllowedHopTransitions[current]
	if !exists {
		return false
	}
	return allowed[next]
}

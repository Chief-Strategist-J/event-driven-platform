package machines

import (
	"errors"
)

var (
	ErrInvalidStateTransition = errors.New("invalid held-key state transition")
)

type HeldKeyState string

const (
	StateActive     HeldKeyState = "ACTIVE"
	StateHeld       HeldKeyState = "HELD"
	StateDraining   HeldKeyState = "DRAINING"
	StateResyncing  HeldKeyState = "RESYNCING"
)

func TransitionHeldKeyState(from HeldKeyState, to HeldKeyState) (HeldKeyState, error) {
	switch from {
	case StateActive:
		if to == StateHeld {
			return StateHeld, nil
		}
	case StateHeld:
		if to == StateDraining || to == StateResyncing || to == StateActive {
			return to, nil
		}
	case StateDraining:
		if to == StateActive || to == StateHeld {
			return to, nil
		}
	case StateResyncing:
		if to == StateActive {
			return StateActive, nil
		}
	}
	return from, ErrInvalidStateTransition
}

func CanDrainSequence(currentVersion int64, nextEventVersion int64) bool {
	return nextEventVersion == currentVersion+1
}


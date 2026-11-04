package machines

import (
	"errors"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
)

type DeadLetterStateMachine struct{}

func NewDeadLetterStateMachine() *DeadLetterStateMachine {
	return &DeadLetterStateMachine{}
}

func (m *DeadLetterStateMachine) Transition(
	current contracts.QuarantineStatus,
	target contracts.QuarantineStatus,
) (contracts.QuarantineStatus, error) {
	switch current {
	case contracts.StatusQuarantined:
		if target == contracts.StatusRedriving || target == contracts.StatusDiscarded || target == contracts.StatusCompensated {
			return target, nil
		}
	case contracts.StatusRedriving:
		if target == contracts.StatusRedrivenSuccess || target == contracts.StatusQuarantined || target == contracts.StatusDiscarded {
			return target, nil
		}
	case contracts.StatusRedrivenSuccess, contracts.StatusDiscarded, contracts.StatusCompensated:
		return current, errors.New("cannot transition from terminal quarantine state")
	}

	return current, errors.New("illegal quarantine state transition")
}


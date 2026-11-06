package machines

import (
	"errors"

	"event-driven-platform/src/features/event-driven-communication/contracts"
)

var (
	ErrInvalidSagaTransition = errors.New("illegal saga state transition")
)

type SagaStateMachine struct{}

func NewSagaStateMachine() *SagaStateMachine {
	return &SagaStateMachine{}
}

func (m *SagaStateMachine) TransitionStep(
	instance contracts.SagaInstance,
	outcome contracts.CommandOutcome,
	totalSteps int,
) (contracts.SagaInstance, bool, error) {
	if instance.State != contracts.SagaStateInProgress {
		return instance, false, ErrInvalidSagaTransition
	}

	next := instance
	if outcome == contracts.OutcomeSucceeded {
		if instance.CurrentStepIndex+1 >= totalSteps {
			next.State = contracts.SagaStateSucceeded
			return next, false, nil
		}
		next.CurrentStepIndex++
		return next, false, nil
	}

	if instance.CurrentStepIndex == 0 {
		next.State = contracts.SagaStateFailed
		return next, false, nil
	}

	next.State = contracts.SagaStateCompensating
	return next, true, nil
}

package machines

import (
	"errors"

	"event-driven-platform/src/features/backpressure/contracts"
)

var (
	ErrInvalidTransition = errors.New("backpressure state transition is not permitted")
)

type Transition struct {
	From  contracts.BackpressureState
	Event contracts.BackpressureEvent
	To    contracts.BackpressureState
}

var allowedTransitions = []Transition{
	{From: contracts.StateNormal, Event: contracts.EventSaturationDetected, To: contracts.StateSaturated},
	{From: contracts.StateSaturated, Event: contracts.EventRecoveryStarted, To: contracts.StateRecovering},
	{From: contracts.StateRecovering, Event: contracts.EventRecoveryConfirmed, To: contracts.StateNormal},
	{From: contracts.StateRecovering, Event: contracts.EventSaturationDetected, To: contracts.StateSaturated},
}

func TransitionTo(current contracts.BackpressureState, event contracts.BackpressureEvent) (contracts.BackpressureState, error) {
	for _, t := range allowedTransitions {
		if t.From == current && t.Event == event {
			return t.To, nil
		}
	}
	return current, ErrInvalidTransition
}

func IsTerminal(state contracts.BackpressureState) bool {
	return false
}

func AllowedEventsFrom(state contracts.BackpressureState) []contracts.BackpressureEvent {
	events := make([]contracts.BackpressureEvent, 0)
	for _, t := range allowedTransitions {
		if t.From == state {
			events = append(events, t.Event)
		}
	}
	return events
}


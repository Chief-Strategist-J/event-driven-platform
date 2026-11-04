package machines

import (
	"errors"

	"event-driven-platform/src/features/offset-management/contracts"
)

var (
	ErrInvalidLagTransition = errors.New("invalid lag state machine transition")
)

type LagStateMachine struct {
	currentState contracts.LagSeverity
}

func NewLagStateMachine(initial contracts.LagSeverity) *LagStateMachine {
	if initial == "" {
		initial = contracts.LagSeverityHealthy
	}
	return &LagStateMachine{currentState: initial}
}

func (m *LagStateMachine) CurrentState() contracts.LagSeverity {
	return m.currentState
}

func (m *LagStateMachine) Transition(next contracts.LagSeverity) error {
	switch m.currentState {
	case contracts.LagSeverityHealthy:
		if next == contracts.LagSeveritySurge || next == contracts.LagSeverityStalled || next == contracts.LagSeverityHealthy {
			m.currentState = next
			return nil
		}
	case contracts.LagSeveritySurge:
		if next == contracts.LagSeverityHealthy || next == contracts.LagSeverityStalled || next == contracts.LagSeveritySurge {
			m.currentState = next
			return nil
		}
	case contracts.LagSeverityStalled:
		if next == contracts.LagSeveritySurge || next == contracts.LagSeverityHealthy || next == contracts.LagSeverityStalled {
			m.currentState = next
			return nil
		}
	}
	return ErrInvalidLagTransition
}

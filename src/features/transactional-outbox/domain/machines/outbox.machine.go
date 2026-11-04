package machines

import (
	"fmt"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/domain/rules"
)

type OutboxStateMachine struct{}

func NewOutboxStateMachine() *OutboxStateMachine {
	return &OutboxStateMachine{}
}

func (m *OutboxStateMachine) TransitionTo(current contracts.OutboxStatus, target contracts.OutboxStatus) (contracts.OutboxStatus, error) {
	if !rules.CanTransitionOutboxStatus(current, target) {
		return current, fmt.Errorf("illegal state transition from %s to %s", current, target)
	}
	return target, nil
}

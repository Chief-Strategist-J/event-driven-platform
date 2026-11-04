package machines

import (
	"fmt"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/domain/rules"
)

type ConsumerStateMachine struct{}

func NewConsumerStateMachine() *ConsumerStateMachine {
	return &ConsumerStateMachine{}
}

func (m *ConsumerStateMachine) DecideAction(
	currentState *contracts.AggregateProjectionState,
	event contracts.InboundConsumerEvent,
) (contracts.ConsumerProcessAction, error) {
	if currentState != nil && currentState.IsParked {
		return contracts.ActionParkPoison, fmt.Errorf("aggregate is parked: poison pill isolation active")
	}

	var currentVersion int64 = 0
	if currentState != nil {
		currentVersion = currentState.LastVersion
	}

	action := rules.EvaluateVersionRule(currentVersion, event.AggregateVersion, event.SemanticClass)
	return action, nil
}


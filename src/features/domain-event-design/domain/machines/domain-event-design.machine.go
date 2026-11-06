package machines

import (
	"errors"

	"event-driven-platform/src/features/domain-event-design/contracts"
	"event-driven-platform/src/features/domain-event-design/domain/rules"
)

var (
	ErrInvalidVersionIncrement = errors.New("version increment must be strictly positive")
)

type AggregateLifecycleState string

const (
	LifecycleStateInitial  AggregateLifecycleState = "INITIAL"
	LifecycleStateActive   AggregateLifecycleState = "ACTIVE"
	LifecycleStateTerminal AggregateLifecycleState = "TERMINAL"
)

type AggregateStateMachine struct{}

func NewAggregateStateMachine() *AggregateStateMachine {
	return &AggregateStateMachine{}
}

func (m *AggregateStateMachine) Transition(
	current contracts.AggregateRootVersion,
	cmd contracts.ReserveAggregateVersionCommand,
) (contracts.ReserveAggregateVersionResult, error) {
	if current.IsTerminal {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrAggregateTerminal
	}

	if current.CurrentVersion != cmd.ExpectedVersion {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrOccConflict
	}

	inc := cmd.VersionIncrement
	if inc <= 0 {
		inc = 1
	}

	nextVersion := current.CurrentVersion + inc

	return contracts.ReserveAggregateVersionResult{
		AggregateType:   cmd.AggregateType,
		AggregateID:     cmd.AggregateID,
		PreviousVersion: current.CurrentVersion,
		ReservedVersion: nextVersion,
		IsTerminal:      cmd.MarkTerminal,
	}, nil
}

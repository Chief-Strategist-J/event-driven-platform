package machines

import (
	"errors"

	"event-driven-platform/src/features/replay-mechanism/contracts"
)

var (
	ErrInvalidReplayTransition = errors.New("invalid replay state transition")
)

type Transition struct {
	From  contracts.ReplayState
	Event contracts.ReplayEvent
	To    contracts.ReplayState
}

var allowedTransitions = []Transition{
	{From: contracts.StateInitializing, Event: contracts.EventStartStreaming, To: contracts.StateStreaming},
	{From: contracts.StateStreaming, Event: contracts.EventFollowLive, To: contracts.StateFollowing},
	{From: contracts.StateStreaming, Event: contracts.EventCatchUpReached, To: contracts.StateCaughtUp},
	{From: contracts.StateFollowing, Event: contracts.EventCatchUpReached, To: contracts.StateCaughtUp},
	{From: contracts.StateCaughtUp, Event: contracts.EventPromoteWriter, To: contracts.StatePromoted},
	{From: contracts.StatePromoted, Event: contracts.EventCompleteCutover, To: contracts.StateCompleted},
	{From: contracts.StateInitializing, Event: contracts.EventAbortRun, To: contracts.StateAborted},
	{From: contracts.StateStreaming, Event: contracts.EventAbortRun, To: contracts.StateAborted},
	{From: contracts.StateFollowing, Event: contracts.EventAbortRun, To: contracts.StateAborted},
	{From: contracts.StateCaughtUp, Event: contracts.EventAbortRun, To: contracts.StateAborted},
	{From: contracts.StatePromoted, Event: contracts.EventAbortRun, To: contracts.StateAborted},
}

func TransitionTo(current contracts.ReplayState, event contracts.ReplayEvent) (contracts.ReplayState, error) {
	for _, t := range allowedTransitions {
		if t.From == current && t.Event == event {
			return t.To, nil
		}
	}
	return current, ErrInvalidReplayTransition
}

func IsTerminal(state contracts.ReplayState) bool {
	return state == contracts.StateCompleted || state == contracts.StateAborted
}

func AllowedEventsFrom(state contracts.ReplayState) []contracts.ReplayEvent {
	events := make([]contracts.ReplayEvent, 0)
	for _, t := range allowedTransitions {
		if t.From == state {
			events = append(events, t.Event)
		}
	}
	return events
}


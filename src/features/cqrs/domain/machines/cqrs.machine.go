package machines

import (
	"event-driven-platform/src/features/cqrs/contracts"
)

func CanTransitionCommand(current contracts.CommandStatus, target contracts.CommandStatus) bool {
	switch current {
	case contracts.CommandStatusInProgress:
		return target == contracts.CommandStatusCompleted ||
			target == contracts.CommandStatusRejected ||
			target == contracts.CommandStatusConflict
	case contracts.CommandStatusCompleted, contracts.CommandStatusRejected, contracts.CommandStatusConflict:
		return false
	default:
		return false
	}
}

func DetermineReplayOutcome(existing contracts.CommandRecord, incomingFingerprint string) (contracts.CommandStatus, bool) {
	if existing.RequestFingerprint != incomingFingerprint {
		return contracts.CommandStatusRejected, false
	}
	return existing.Status, true
}


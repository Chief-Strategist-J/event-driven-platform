package rules

import (
	"event-driven-platform/src/features/idempotent-consumers/contracts"
)

func EvaluateVersionRule(
	currentVersion int64,
	incomingVersion int64,
	semanticClass contracts.SemanticClass,
) contracts.ConsumerProcessAction {
	if incomingVersion <= currentVersion {
		return contracts.ActionSkip
	}

	if semanticClass == contracts.SemanticState {
		return contracts.ActionApply
	}

	if incomingVersion == currentVersion+1 {
		return contracts.ActionApply
	}

	return contracts.ActionHoldGap
}

func IsContentHashConsistent(existingHash, newHash string) bool {
	if existingHash == "" || newHash == "" {
		return true
	}
	return existingHash == newHash
}


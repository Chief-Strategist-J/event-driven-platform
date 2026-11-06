package machines

import (
	"errors"

	"event-driven-platform/src/features/encryption/contracts"
	"event-driven-platform/src/features/encryption/domain/rules"
)

func CanTransitionDEKState(current string, next string) bool {
	switch current {
	case "ACTIVE":
		return next == "ROTATED" || next == "COMPROMISED" || next == "SHREDDED"
	case "ROTATED":
		return next == "COMPROMISED" || next == "SHREDDED"
	case "COMPROMISED":
		return next == "SHREDDED"
	case "SHREDDED":
		return false
	default:
		return false
	}
}

func EvaluateDecryptOutcome(dekState string, isEntitled bool, decryptErr error) contracts.DecryptOutcome {
	if !isEntitled {
		return contracts.OutcomeUnauthorized
	}
	if dekState == "SHREDDED" {
		return contracts.OutcomeErased
	}
	if decryptErr != nil {
		if errors.Is(decryptErr, rules.ErrTagMismatch) {
			return contracts.OutcomeTampered
		}
		return contracts.OutcomeMalformed
	}
	return contracts.OutcomeDecrypted
}

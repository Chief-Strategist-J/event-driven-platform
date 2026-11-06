package machines

import (
	"event-driven-platform/src/features/audit-trail/contracts"
)

func CanTransitionCheckpointStatus(current string, next string) bool {
	switch current {
	case "ANCHORED":
		return next == "VERIFIED" || next == "TAMPERED"
	case "VERIFIED":
		return next == "TAMPERED"
	case "TAMPERED":
		return false
	default:
		return false
	}
}

func ClassifyVerificationOutcome(computedHead string, anchoredHead string, actualSeq int64, expectedSeq int64) contracts.VerificationStatus {
	if actualSeq < expectedSeq {
		return contracts.StatusTampered
	}
	if computedHead == anchoredHead {
		return contracts.StatusIntact
	}
	return contracts.StatusMismatch
}

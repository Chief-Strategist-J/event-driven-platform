package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
)

var (
	ErrFencedEpoch = errors.New("operation rejected: incoming owner epoch is stale")
)

func ComputePayloadHash(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func EvaluateSequenceProgression(
	currentVersion int64,
	currentHash string,
	incomingVersion int64,
	incomingHash string,
	isAlreadyHeld bool,
	currentHoldReason contracts.HoldReason,
) contracts.SequenceEvaluationResult {
	if isAlreadyHeld {
		if incomingVersion == currentVersion+1 && currentHoldReason == contracts.HoldReasonGap {
			return contracts.SequenceEvaluationResult{
				Action:          contracts.ActionApply,
				CurrentVersion:  currentVersion,
				IncomingVersion: incomingVersion,
				IsHeld:          true,
				HoldReason:      currentHoldReason,
			}
		}
		return contracts.SequenceEvaluationResult{
			Action:           contracts.ActionGapHeld,
			CurrentVersion:   currentVersion,
			IncomingVersion:  incomingVersion,
			IsHeld:           true,
			HoldReason:       currentHoldReason,
			RequiresDLQAlert: false,
		}
	}

	if currentVersion == 0 && incomingVersion == 1 {
		return contracts.SequenceEvaluationResult{
			Action:          contracts.ActionApply,
			CurrentVersion:  0,
			IncomingVersion: 1,
			IsHeld:          false,
		}
	}

	if incomingVersion == currentVersion+1 {
		return contracts.SequenceEvaluationResult{
			Action:          contracts.ActionApply,
			CurrentVersion:  currentVersion,
			IncomingVersion: incomingVersion,
			IsHeld:          false,
		}
	}

	if incomingVersion <= currentVersion {
		if incomingVersion == currentVersion {
			if incomingHash == currentHash {
				return contracts.SequenceEvaluationResult{
					Action:          contracts.ActionDuplicateSkip,
					CurrentVersion:  currentVersion,
					IncomingVersion: incomingVersion,
					IsHeld:          false,
				}
			}
			return contracts.SequenceEvaluationResult{
				Action:           contracts.ActionIntegrityMismatch,
				CurrentVersion:   currentVersion,
				IncomingVersion:  incomingVersion,
				IsHeld:           true,
				HoldReason:       contracts.HoldReasonIntegrity,
				RequiresDLQAlert: true,
			}
		}
		return contracts.SequenceEvaluationResult{
			Action:          contracts.ActionStaleSkip,
			CurrentVersion:  currentVersion,
			IncomingVersion: incomingVersion,
			IsHeld:          false,
		}
	}

	missing := make([]int64, 0, incomingVersion-(currentVersion+1))
	for v := currentVersion + 1; v < incomingVersion; v++ {
		missing = append(missing, v)
	}

	return contracts.SequenceEvaluationResult{
		Action:           contracts.ActionGapHeld,
		CurrentVersion:   currentVersion,
		IncomingVersion:  incomingVersion,
		GapClass:         contracts.GapClassDefect,
		MissingVersions:  missing,
		IsHeld:           true,
		HoldReason:       contracts.HoldReasonGap,
		RequiresDLQAlert: true,
	}
}


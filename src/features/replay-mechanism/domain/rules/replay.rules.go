package rules

import (
	"errors"
	"fmt"
	"time"

	"event-driven-platform/src/features/replay-mechanism/contracts"
)

var (
	ErrReplayOffsetRegression     = errors.New("currentOffset cannot regress backwards")
	ErrReplayExceedsEndOffset     = errors.New("currentOffset cannot exceed run endOffset")
	ErrCannotPromoteBeforeCatchUp = errors.New("cannot promote replay run before caught-up state")
	ErrCannotCutoverBeforePromoted = errors.New("cannot complete cutover before writer is promoted")
	ErrUnrecognizedReplayMode     = errors.New("unrecognized replay run mode")
	ErrReplayNotAdvanceable       = errors.New("replay run cannot advance offsets in its current state")
	ErrUnrecognizedEffectType     = errors.New("unrecognized effect type")
)

func GenerateReplayConsumerGroup(targetProjection string, timestamp time.Time) string {
	return fmt.Sprintf("replay.%s.%d", targetProjection, timestamp.Unix())
}

func ResolveEffectMode(mode contracts.ReplayRunMode, effectType contracts.EffectType) (contracts.EffectMode, error) {
	switch mode {
	case contracts.ModeLive:
		return contracts.EffectModeAllow, nil
	case contracts.ModeRecover:
		return contracts.EffectModeReconcile, nil
	case contracts.ModeRebuild, contracts.ModeHydrate, contracts.ModeForensic, contracts.ModeRepair:
		switch effectType {
		case contracts.EffectSearchIndex, contracts.EffectCacheWrite:
			return contracts.EffectModeAllow, nil
		case contracts.EffectExternalPayment, contracts.EffectExternalNotification, contracts.EffectOutboxEvent:
			return contracts.EffectModeSuppress, nil
		default:
			return contracts.EffectModeSuppress, ErrUnrecognizedEffectType
		}
	default:
		return contracts.EffectModeSuppress, ErrUnrecognizedReplayMode
	}
}

func EvaluateEffectDecision(mode contracts.EffectMode) contracts.EffectDecision {
	switch mode {
	case contracts.EffectModeAllow:
		return contracts.DecisionExecuted
	case contracts.EffectModeReconcile:
		return contracts.DecisionReconciled
	default:
		return contracts.DecisionSuppressed
	}
}

func ValidateOffsetAdvance(currentOffset, newOffset, endOffset int64) error {
	if newOffset < currentOffset {
		return ErrReplayOffsetRegression
	}
	if endOffset > 0 && newOffset > endOffset {
		return ErrReplayExceedsEndOffset
	}
	return nil
}

func IsCaughtUp(currentOffset, endOffset int64) bool {
	if endOffset <= 0 {
		return true
	}
	return currentOffset >= endOffset
}

// CanAdvanceOffset reports whether a run in the given state may move its
// offset: only after streaming started and before it is promoted or terminal.
func CanAdvanceOffset(state contracts.ReplayState) bool {
	switch state {
	case contracts.StateStreaming, contracts.StateFollowing, contracts.StateCaughtUp:
		return true
	}
	return false
}

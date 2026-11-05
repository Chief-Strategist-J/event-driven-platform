package rules

import (
	"errors"
	"fmt"
	"time"

	"event-driven-platform/src/features/replay-mechanism/contracts"
)

var (
	ErrReplayOffsetRegression = errors.New("currentOffset cannot regress backwards")
	ErrReplayExceedsEndOffset = errors.New("currentOffset cannot exceed run endOffset")
	ErrCannotPromoteBeforeCatchUp = errors.New("cannot promote replay run before caught-up state")
	ErrCannotCutoverBeforePromoted = errors.New("cannot complete cutover before writer is promoted")
)

func GenerateReplayConsumerGroup(targetProjection string, timestamp time.Time) string {
	return fmt.Sprintf("replay.%s.%d", targetProjection, timestamp.Unix())
}

func ResolveEffectMode(mode contracts.ReplayRunMode, effectType contracts.EffectType) contracts.EffectMode {
	if mode == contracts.ModeLive {
		return contracts.EffectModeAllow
	}
	if mode == contracts.ModeRecover {
		return contracts.EffectModeReconcile
	}
	if effectType == contracts.EffectSearchIndex || effectType == contracts.EffectCacheWrite {
		return contracts.EffectModeAllow
	}
	return contracts.EffectModeSuppress
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


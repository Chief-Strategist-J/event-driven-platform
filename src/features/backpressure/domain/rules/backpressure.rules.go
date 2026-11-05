package rules

import (
	"errors"

	"event-driven-platform/src/features/backpressure/contracts"
)

var (
	ErrQueueingDelayTooHigh    = errors.New("queueing delay exceeds saturation threshold")
	ErrLatencyGradientBreached = errors.New("dependency latency gradient exceeds 2x baseline")
	ErrMemoryBudgetExceeded    = errors.New("live heap exceeds 70% of soft memory limit")
	ErrLaneQueueHighWatermark  = errors.New("lane queue bytes exceed 80% high watermark")
	ErrRejectionRateHigh       = errors.New("dependency rejection rate is non-zero")
	ErrNegativeConcurrency     = errors.New("concurrency limit cannot be negative")
)

const (
	LatencyBaselineMultiplier    = 2.0
	MemoryBudgetFraction         = 0.70
	LaneQueueHighWatermarkRatio  = 0.80
	LaneQueueLowWatermarkRatio   = 0.40
	MinPauseDurationSeconds      = 5
	EscalationWarnSeconds        = 120
	EscalationPageSeconds        = 600
	ResumeRampFraction           = 0.25
	ConcurrencyFloor             = int64(1)
	ConcurrencyMultiplicativeDecrement = 0.5
)

func EvaluateSaturation(signal contracts.SaturationSignal) contracts.SaturationEvalResult {
	if signal.MemoryLimitBytes > 0 {
		heapFraction := float64(signal.LiveHeapBytes) / float64(signal.MemoryLimitBytes)
		if heapFraction >= MemoryBudgetFraction {
			return contracts.SaturationEvalResult{
				IsSaturated:      true,
				SaturationReason: contracts.PauseReasonMemoryBudget,
				Recommendation:   "PAUSE_PARTITIONS_DRAIN_QUEUE",
			}
		}
	}

	if signal.RejectionCount > 0 {
		return contracts.SaturationEvalResult{
			IsSaturated:      true,
			SaturationReason: contracts.PauseReasonRejections,
			Recommendation:   "DECREASE_CONCURRENCY_MULTIPLICATIVELY",
		}
	}

	if signal.BaselineLatencyMs > 0 && signal.LatencyP99Ms > LatencyBaselineMultiplier*signal.BaselineLatencyMs {
		return contracts.SaturationEvalResult{
			IsSaturated:      true,
			SaturationReason: contracts.PauseReasonDBLatency,
			Recommendation:   "DECREASE_CONCURRENCY_MULTIPLICATIVELY",
		}
	}

	if signal.ConcurrencyLimit > 0 && signal.InFlight >= signal.ConcurrencyLimit && signal.QueueingDelayMs > 0 {
		return contracts.SaturationEvalResult{
			IsSaturated:      true,
			SaturationReason: contracts.PauseReasonQueueFull,
			Recommendation:   "PAUSE_PARTITIONS_DRAIN_QUEUE",
		}
	}

	return contracts.SaturationEvalResult{
		IsSaturated:    false,
		Recommendation: "STEADY_STATE_CONTINUE",
	}
}

func ComputeDecreasedConcurrency(current int64) int64 {
	next := int64(float64(current) * ConcurrencyMultiplicativeDecrement)
	if next < ConcurrencyFloor {
		return ConcurrencyFloor
	}
	return next
}

func ComputeRampConcurrency(prePause int64) int64 {
	ramp := int64(float64(prePause) * ResumeRampFraction)
	if ramp < ConcurrencyFloor {
		return ConcurrencyFloor
	}
	return ramp
}

func IsRecoveryReadyForConfirmation(signal contracts.SaturationSignal, healthyForSeconds int64) bool {
	if healthyForSeconds < 30 {
		return false
	}
	if signal.MemoryLimitBytes > 0 {
		heapFraction := float64(signal.LiveHeapBytes) / float64(signal.MemoryLimitBytes)
		if heapFraction >= LaneQueueLowWatermarkRatio {
			return false
		}
	}
	if signal.RejectionCount > 0 {
		return false
	}
	if signal.BaselineLatencyMs > 0 && signal.LatencyP99Ms > LatencyBaselineMultiplier*signal.BaselineLatencyMs {
		return false
	}
	return true
}

func CanCoalesce(ordering contracts.OrderingClass) bool {
	return ordering == contracts.OrderingState
}

func MustNeverShed(ordering contracts.OrderingClass) bool {
	return ordering == contracts.OrderingStrict
}

func ValidateConcurrencyLimit(limit int64) error {
	if limit < 0 {
		return ErrNegativeConcurrency
	}
	return nil
}


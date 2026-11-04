package rules

import (
	"errors"
	"time"

	"event-driven-platform/src/features/offset-management/contracts"
)

var (
	ErrAutoCommitBanned         = errors.New("auto-commit strategy is strictly banned per Tier 1 rules")
	ErrNegativeOffsetDisallowed = errors.New("partition offset must be non-negative")
	ErrOffsetRegression         = errors.New("commit offset cannot regress below currently stored offset without administrative rewind")
	ErrConsumerGroupMustBeStopped = errors.New("administrative offset reset requires consumer group to be stopped")
)

const (
	LagThresholdHealthy int64 = 1000
	LagThresholdSurge   int64 = 10000
)

func VerifyCommitStrategy(strategy contracts.OffsetCommitStrategy) error {
	if strategy == contracts.CommitStrategyAuto {
		return ErrAutoCommitBanned
	}
	return nil
}

func ValidateOffsetCommit(currentOffset, newOffset int64) error {
	if newOffset < 0 {
		return ErrNegativeOffsetDisallowed
	}
	if currentOffset > 0 && newOffset < currentOffset {
		return ErrOffsetRegression
	}
	return nil
}

func EvaluateLagSeverity(storedOffset, logEndOffset int64, duration time.Duration, rate float64) contracts.LagSeverity {
	lag := logEndOffset - storedOffset
	if lag < 0 {
		lag = 0
	}

	if lag > LagThresholdSurge && rate == 0 {
		return contracts.LagSeverityStalled
	}

	if lag > LagThresholdSurge {
		return contracts.LagSeveritySurge
	}

	if lag >= LagThresholdHealthy && duration > 5*time.Minute {
		return contracts.LagSeveritySurge
	}

	return contracts.LagSeverityHealthy
}

func ComputeFastForwardOffset(currentOffset, shiftBy, logEndOffset int64) (int64, error) {
	if shiftBy <= 0 {
		return 0, errors.New("shiftBy must be positive")
	}
	target := currentOffset + shiftBy
	if logEndOffset > 0 && target > logEndOffset {
		target = logEndOffset
	}
	return target, nil
}

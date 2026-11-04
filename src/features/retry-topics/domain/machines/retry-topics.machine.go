package machines

import (
	"errors"

	"event-driven-platform/src/features/retry-topics/contracts"
)

var (
	ErrInvalidStateTransition = errors.New("invalid retry attempt state transition")
)

func TransitionRetryStatus(current contracts.RetryAttemptStatus, next contracts.RetryAttemptStatus) (contracts.RetryAttemptStatus, error) {
	switch current {
	case contracts.StatusScheduled:
		if next == contracts.StatusRetrying || next == contracts.StatusExpired {
			return next, nil
		}
	case contracts.StatusRetrying:
		if next == contracts.StatusResolved || next == contracts.StatusScheduled || next == contracts.StatusExhausted || next == contracts.StatusExpired {
			return next, nil
		}
	}
	return current, ErrInvalidStateTransition
}

package rules

import (
	"errors"
	"strings"

	"event-driven-platform/src/features/event-driven-communication/contracts"
)

var (
	ErrInvalidStepOrder     = errors.New("invalid step ordering: compensable steps must precede pivot, and pivot must precede retriable")
	ErrEmptySagaSteps       = errors.New("saga definition must contain at least one step")
	ErrInvalidTimeout       = errors.New("timeout seconds must be strictly positive")
	ErrSagaAlreadyConcluded = errors.New("saga instance is already in terminal state")
	ErrStepIndexOutOfBounds = errors.New("step index out of bounds for saga instance")
)

func ValidateSagaStepOrdering(steps []contracts.SagaStepDefinition) error {
	if len(steps) == 0 {
		return ErrEmptySagaSteps
	}

	seenPivot := false
	for _, step := range steps {
		switch step.StepType {
		case contracts.StepTypeCompensable:
			if seenPivot {
				return ErrInvalidStepOrder
			}
			if strings.TrimSpace(step.CompensationName) == "" {
				return errors.New("compensable step must have non-empty compensation_name")
			}
		case contracts.StepTypePivot:
			if seenPivot {
				return errors.New("saga cannot contain more than one pivot step")
			}
			seenPivot = true
		case contracts.StepTypeRetriable:
			if !seenPivot {
				return errors.New("retriable steps must be placed after pivot step")
			}
		}
	}
	return nil
}

func ValidateTimeoutSeconds(sec int) error {
	if sec <= 0 {
		return ErrInvalidTimeout
	}
	return nil
}

func ClassifyLateReply(current contracts.SagaState, stepState contracts.StepState) bool {
	if current == contracts.SagaStateCompensating || current == contracts.SagaStateCompensated || current == contracts.SagaStateFailed {
		return true
	}
	if stepState == contracts.StepStateTimedOut || stepState == contracts.StepStateCompensating || stepState == contracts.StepStateCompensated {
		return true
	}
	return false
}

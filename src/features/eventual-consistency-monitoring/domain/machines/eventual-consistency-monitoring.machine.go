package machines

import (
	"time"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
)

func CanTransitionIncident(current contracts.IncidentStatus, target contracts.IncidentStatus) bool {
	switch current {
	case contracts.IncidentDetected:
		return target == contracts.IncidentInTolerance || target == contracts.IncidentConfirmed || target == contracts.IncidentResolved
	case contracts.IncidentInTolerance:
		return target == contracts.IncidentConfirmed || target == contracts.IncidentResolved
	case contracts.IncidentConfirmed:
		return target == contracts.IncidentAutoHealing || target == contracts.IncidentManualIntervention || target == contracts.IncidentResolved
	case contracts.IncidentAutoHealing:
		return target == contracts.IncidentResolved || target == contracts.IncidentManualIntervention || target == contracts.IncidentConfirmed
	case contracts.IncidentManualIntervention:
		return target == contracts.IncidentResolved
	case contracts.IncidentResolved:
		return target == contracts.IncidentDetected
	default:
		return false
	}
}

func EvaluateIncidentLifecycle(
	current contracts.IncidentStatus,
	elapsed time.Duration,
	toleranceWindow time.Duration,
	isMismatch bool,
	healAttempts int,
) contracts.IncidentStatus {
	if !isMismatch {
		return contracts.IncidentResolved
	}

	switch current {
	case contracts.IncidentDetected:
		if elapsed < toleranceWindow {
			return contracts.IncidentInTolerance
		}
		if healAttempts >= 2 {
			return contracts.IncidentManualIntervention
		}
		return contracts.IncidentConfirmed

	case contracts.IncidentInTolerance:
		if elapsed >= toleranceWindow {
			if healAttempts >= 2 {
				return contracts.IncidentManualIntervention
			}
			return contracts.IncidentConfirmed
		}
		return contracts.IncidentInTolerance

	case contracts.IncidentConfirmed:
		if healAttempts >= 2 {
			return contracts.IncidentManualIntervention
		}
		return contracts.IncidentAutoHealing

	case contracts.IncidentAutoHealing:
		if healAttempts >= 2 {
			return contracts.IncidentManualIntervention
		}
		return contracts.IncidentConfirmed

	case contracts.IncidentManualIntervention:
		return contracts.IncidentManualIntervention

	default:
		return contracts.IncidentDetected
	}
}


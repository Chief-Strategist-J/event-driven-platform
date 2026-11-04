package rules

import (
	"strings"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
)

func IsMessageDefect(errorCode string) bool {
	code := strings.ToUpper(strings.TrimSpace(errorCode))
	if code == "TIMEOUT" || code == "DB_CONNECTION_LOST" || code == "503_SERVICE_UNAVAILABLE" {
		return false
	}
	return true
}

func CanRedrive(record contracts.DeadLetterQuarantineRecord) bool {
	if record.Status != contracts.StatusQuarantined && record.Status != contracts.StatusRedriving {
		return false
	}
	if record.RedriveCount >= record.MaxRedriveAttempts {
		return false
	}
	return true
}

func DetermineNextQuarantineStatus(success bool, currentRedrives, maxRedrives int) contracts.QuarantineStatus {
	if success {
		return contracts.StatusRedrivenSuccess
	}
	if currentRedrives >= maxRedrives {
		return contracts.StatusQuarantined
	}
	return contracts.StatusQuarantined
}


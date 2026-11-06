package rules

import (
	"fmt"
	"strings"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrOutboxSnapshotForbidden = errors.NewValidationError("outbox mode strictly forbids table snapshot signals (CDC-01, CDC-03)")
	ErrWildcardPublication     = errors.NewValidationError("wildcard publication scope is forbidden; explicit table names required (CDC-04)")
	ErrWALDiskFullThreat       = errors.NewConflictError("wal retention threat: time-to-disk-full below emergency threshold (CDC-11)")
	ErrInvalidSlotNaming       = errors.NewValidationError("slot name must follow cdc_slot_<service> naming convention (CDC-02)")
)

func ValidateConnectorConfiguration(input contracts.RegisterConnectorInput) error {
	if !strings.HasPrefix(input.SlotName, "cdc_slot_") {
		return fmt.Errorf("%w: got %s", ErrInvalidSlotNaming, input.SlotName)
	}
	if strings.Contains(input.PublicationName, "*") || strings.EqualFold(input.PublicationName, "all_tables") {
		return ErrWildcardPublication
	}
	return nil
}

func ValidateSignalForMode(mode contracts.CDCMode, sig contracts.CDCSignalType) error {
	if mode == contracts.ModeOutbox && sig == contracts.SignalExecuteSnapshot {
		return ErrOutboxSnapshotForbidden
	}
	return nil
}

type WALCircuitBreakerResult struct {
	Tripped            bool
	TimeToDiskFullHrs  float64
	RetainedBytes      int64
	DiskFreeBytes      int64
	RecommendedAction  string
}

func EvaluateWALCircuitBreaker(retainedBytes int64, genRateBytesPerSec int64, diskFreeBytes int64) WALCircuitBreakerResult {
	if genRateBytesPerSec <= 0 {
		genRateBytesPerSec = 1024 * 1024
	}

	remainingBytes := diskFreeBytes - retainedBytes
	if remainingBytes <= 0 {
		return WALCircuitBreakerResult{
			Tripped:           true,
			TimeToDiskFullHrs: 0.0,
			RetainedBytes:     retainedBytes,
			DiskFreeBytes:     diskFreeBytes,
			RecommendedAction: "EMERGENCY_PAUSE_CONNECTOR",
		}
	}

	secondsToFull := float64(remainingBytes) / float64(genRateBytesPerSec)
	hoursToFull := secondsToFull / 3600.0

	tripped := hoursToFull < 2.0
	action := "ALLOW"
	if hoursToFull < 1.0 {
		action = "EMERGENCY_PAUSE_CONNECTOR"
	} else if hoursToFull < 2.0 {
		action = "WARN_AND_THROTTLE"
	}

	return WALCircuitBreakerResult{
		Tripped:           tripped,
		TimeToDiskFullHrs: hoursToFull,
		RetainedBytes:     retainedBytes,
		DiskFreeBytes:     diskFreeBytes,
		RecommendedAction: action,
	}
}

func EvaluateCanarySLA(sentLatencyMs int64, maxAllowedMs int64) contracts.CanaryCheckResult {
	if maxAllowedMs <= 0 {
		maxAllowedMs = 5000
	}

	withinSLO := sentLatencyMs <= maxAllowedMs
	return contracts.CanaryCheckResult{
		Reconciled:   true,
		LatencyMs:    sentLatencyMs,
		WithinSLO:    withinSLO,
		MaxAllowedMs: maxAllowedMs,
	}
}


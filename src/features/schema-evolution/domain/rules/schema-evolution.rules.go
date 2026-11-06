package rules

import (
	"crypto/sha256"
	"encoding/hex"

	"event-driven-platform/src/features/schema-evolution/contracts"
)

func ComputeSchemaFingerprint(definition string) string {
	h := sha256.Sum256([]byte(definition))
	return hex.EncodeToString(h[:])
}

func EvaluateTransitiveCompatibility(candidateFingerprint string, existingLineage []contracts.SchemaVersionRecord) bool {
	for _, past := range existingLineage {
		if past.SchemaFingerprint == candidateFingerprint {
			return false
		}
	}
	return true
}

func EvaluateConsumerReadiness(consumers []contracts.ConsumerSupportRecord, targetVersion int) contracts.ConsumerReadinessOutput {
	var unready []string
	activeCount := 0

	for _, c := range consumers {
		if c.Status != "ACTIVE" {
			continue
		}
		activeCount++
		if c.MaxSupportedVersion < targetVersion {
			unready = append(unready, c.ConsumerGroup)
		}
	}

	return contracts.ConsumerReadinessOutput{
		IsReady:              len(unready) == 0,
		TargetVersion:        targetVersion,
		UnreadyConsumers:     unready,
		TotalActiveConsumers: activeCount,
	}
}

func EvaluateDecommissionCheck(in contracts.EvaluateDecommissionInput) contracts.DecommissionCheckOutput {
	var reasons []string

	if in.ActiveConsumerCount > 0 {
		reasons = append(reasons, "Active consumers still registered on legacy topic")
	}
	if in.ZeroLagDays < 7 {
		reasons = append(reasons, "Legacy topic has not sustained zero lag for at least 7 consecutive days")
	}
	if in.TelemetryReadCount > 0 {
		reasons = append(reasons, "Telemetry detects active read operations on legacy events")
	}
	if !in.DecommissionAttested {
		reasons = append(reasons, "Consumer owners have not attested formal migration completion")
	}

	return contracts.DecommissionCheckOutput{
		CanDecommission: len(reasons) == 0,
		BlockingReasons: reasons,
	}
}

func ClassifyDeserializationFailure(occurrencesInWindow int) contracts.IncidentClass {
	if occurrencesInWindow >= 5 {
		return contracts.IncidentSystemicFailure
	}
	return contracts.IncidentSingleCorrupt
}


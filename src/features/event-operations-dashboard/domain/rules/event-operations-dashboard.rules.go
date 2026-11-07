package rules

import (
	"fmt"
	"strings"
	"time"

	"event-driven-platform/src/features/event-operations-dashboard/contracts"
)

const MetricStalenessThreshold = 60 * time.Second

var HopEvaluationOrder = []contracts.HopCode{
	contracts.HopH1Outbox,
	contracts.HopH2Relay,
	contracts.HopH3Kafka,
	contracts.HopH4Consumer,
	contracts.HopH5AuthView,
	contracts.HopH6DerivedStore,
}

func EvaluateHopStaleness(reportedAt time.Time, now time.Time, currentStatus contracts.HopStatus) contracts.HopStatus {
	if now.Sub(reportedAt) > MetricStalenessThreshold {
		return contracts.StatusUnknown
	}
	return currentStatus
}

func AssertZeroCustomerPayload(fields map[string]interface{}) error {
	forbiddenKeys := []string{"email", "payload", "credit_card", "ssn", "password", "token", "national_id"}
	for k := range fields {
		lower := strings.ToLower(k)
		for _, f := range forbiddenKeys {
			if strings.Contains(lower, f) {
				return fmt.Errorf("%w: sensitive field %s prohibited in telemetry", contracts.ErrCustomerPayloadDetected, k)
			}
		}
	}
	return nil
}

// HealthyCCTLActionLink is the non-empty read-only control-plane link returned when
// no degraded hop exists; the triage log requires a non-empty c_ctl_action_link.
const HealthyCCTLActionLink = "/control-plane/consumers/status"

func LocalizeBrokenHop(hops []*contracts.PipelineHopRecord, now time.Time) (*contracts.PipelineHopRecord, contracts.HopStatus, contracts.IncidentSeverity, string, string) {
	hopMap := make(map[contracts.HopCode]*contracts.PipelineHopRecord, len(hops))
	for _, h := range hops {
		hopMap[h.HopCode] = h
	}

	for _, code := range HopEvaluationOrder {
		h, exists := hopMap[code]
		if !exists {
			continue
		}

		effectiveStatus := EvaluateHopStaleness(h.LastReportedAt, now, h.Status)
		if effectiveStatus == contracts.StatusUnknown {
			action := "Telemetric scrape down; trigger external dead-man switch and probe host"
			cctl := fmt.Sprintf("/control-plane/telemetry/probes?hop=%s&flow=%s", h.HopCode, h.FlowName)
			return h, contracts.StatusUnknown, contracts.Sev1, action, cctl
		}

		if effectiveStatus == contracts.StatusPaused {
			var action string
			switch h.PauseReason {
			case contracts.PauseReasonBreakerOpen:
				action = "Breaker open on downstream dependency; DO NOT restart; DO NOT scale"
			case contracts.PauseReasonBackpressure:
				action = "Backpressure active; freeze autoscaler override via C-CTL; verify pool"
			case contracts.PauseReasonCorruptHead:
				action = "Poison event held at head; initiate dead-letter quarantine triage"
			default:
				action = "Partition paused by operator or rebalance; inspect consumer group state"
			}
			cctl := fmt.Sprintf("/control-plane/consumers/override?hop=%s&flow=%s&reason=%s", h.HopCode, h.FlowName, h.PauseReason)
			sev := contracts.Sev1
			if h.Tier == contracts.Tier0 {
				sev = contracts.Sev0
			}
			return h, contracts.StatusPaused, sev, action, cctl
		}

		if effectiveStatus == contracts.StatusStalled || effectiveStatus == contracts.StatusStopped {
			action := "Pipeline hop stopped or stalled; evaluate crash-loop watchdog"
			cctl := fmt.Sprintf("/control-plane/consumers/status?hop=%s&flow=%s", h.HopCode, h.FlowName)
			sev := contracts.Sev1
			if h.Tier == contracts.Tier0 {
				sev = contracts.Sev0
			}
			return h, effectiveStatus, sev, action, cctl
		}

		if effectiveStatus == contracts.StatusLagging {
			action := "Lagging detected; check single-key head lag vs broad partition lag before scaling"
			cctl := fmt.Sprintf("/control-plane/consumers/scale?hop=%s&flow=%s", h.HopCode, h.FlowName)
			sev := contracts.Sev2
			if h.Tier == contracts.Tier0 && h.LagSeconds > 300 {
				sev = contracts.Sev1
			}
			return h, contracts.StatusLagging, sev, action, cctl
		}
	}

	return nil, contracts.StatusOk, contracts.SevInfo, "All pipeline hops reporting healthy", HealthyCCTLActionLink
}

package tests

import (
	"testing"
	"time"

	"event-driven-platform/src/features/event-operations-dashboard/contracts"
	"event-driven-platform/src/features/event-operations-dashboard/domain/machines"
	"event-driven-platform/src/features/event-operations-dashboard/domain/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventOpsDashboard_EvaluateHopStaleness(t *testing.T) {
	now := time.Now().UTC()
	fresh := now.Add(-10 * time.Second)
	stale := now.Add(-90 * time.Second)

	assert.Equal(t, contracts.StatusOk, rules.EvaluateHopStaleness(fresh, now, contracts.StatusOk))
	assert.Equal(t, contracts.StatusUnknown, rules.EvaluateHopStaleness(stale, now, contracts.StatusOk), "OD-02: Stale telemetry must yield UNKNOWN")
}

func TestEventOpsDashboard_CustomerPayloadRejection(t *testing.T) {
	safeFields := map[string]interface{}{
		"topic": "orders.v1",
		"lag":   25.5,
		"tier":  "T0",
	}
	assert.NoError(t, rules.AssertZeroCustomerPayload(safeFields))

	piiFields := map[string]interface{}{
		"topic": "orders.v1",
		"email": "customer@confidential.com",
	}
	assert.ErrorIs(t, rules.AssertZeroCustomerPayload(piiFields), contracts.ErrCustomerPayloadDetected)
}

func TestEventOpsDashboard_HopLocalizationAndWorstCase(t *testing.T) {
	now := time.Now().UTC()

	h1 := &contracts.PipelineHopRecord{
		HopCode:        contracts.HopH1Outbox,
		Status:         contracts.StatusOk,
		Tier:           contracts.Tier0,
		LastReportedAt: now,
	}
	h2 := &contracts.PipelineHopRecord{
		HopCode:        contracts.HopH2Relay,
		Status:         contracts.StatusOk,
		Tier:           contracts.Tier0,
		LastReportedAt: now,
	}
	h3 := &contracts.PipelineHopRecord{
		HopCode:        contracts.HopH3Kafka,
		Status:         contracts.StatusOk,
		Tier:           contracts.Tier0,
		LastReportedAt: now,
	}
	h4 := &contracts.PipelineHopRecord{
		HopCode:        contracts.HopH4Consumer,
		Status:         contracts.StatusPaused,
		PauseReason:    contracts.PauseReasonBackpressure,
		Tier:           contracts.Tier0,
		LagSeconds:     120,
		RunbookURL:     "https://wiki.platform.local/ops/backpressure",
		LastReportedAt: now,
	}

	hops := []*contracts.PipelineHopRecord{h1, h2, h3, h4}

	broken, status, sev, action, cctl := rules.LocalizeBrokenHop(hops, now)
	require.NotNil(t, broken)
	assert.Equal(t, contracts.HopH4Consumer, broken.HopCode)
	assert.Equal(t, contracts.StatusPaused, status)
	assert.Equal(t, contracts.Sev0, sev)
	assert.Contains(t, action, "Backpressure active")
	assert.Contains(t, cctl, "/control-plane/consumers/override")
}

func TestEventOpsDashboard_StateTransitions(t *testing.T) {
	assert.True(t, machines.CanTransitionHopStatus(contracts.StatusOk, contracts.StatusLagging))
	assert.True(t, machines.CanTransitionHopStatus(contracts.StatusLagging, contracts.StatusPaused))
	assert.True(t, machines.CanTransitionHopStatus(contracts.StatusPaused, contracts.StatusOk))
	assert.True(t, machines.CanTransitionHopStatus(contracts.StatusOk, contracts.StatusUnknown))
}

func TestEventOpsDashboard_ContractsNormalization(t *testing.T) {
	_, err := contracts.NormalizeReportHopTelemetryInput(nil)
	assert.Error(t, err)

	_, err = contracts.NormalizeReportHopTelemetryInput(&contracts.ReportHopTelemetryInput{
		TenantID:   "  ",
		FlowName:   "checkout",
		HopCode:    contracts.HopH1Outbox,
		Tier:       contracts.Tier0,
		Status:     contracts.StatusOk,
		LagSeconds: 10,
		RunbookURL: "https://wiki/runbook",
	})
	assert.ErrorIs(t, err, contracts.ErrInvalidTenantID)

	rec, err := contracts.NormalizeReportHopTelemetryInput(&contracts.ReportHopTelemetryInput{
		TenantID:      "tenant_alpha",
		SourceService: "checkout-service",
		ExternalID:    "hop_ext_01",
		FlowName:      "checkout",
		HopCode:       contracts.HopH1Outbox,
		Tier:          contracts.Tier0,
		Status:        contracts.StatusOk,
		LagSeconds:    10.5,
		RunbookURL:    "https://wiki/runbook",
	})
	require.NoError(t, err)
	assert.True(t, contracts.ValidateHopPublicID(rec.PublicID))
}

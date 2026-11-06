package tests

import (
	"testing"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/rules"
)

func TestCDCRules_ValidateConnectorConfiguration(t *testing.T) {
	t.Run("ValidConfigurationPasses", func(t *testing.T) {
		input := contracts.RegisterConnectorInput{
			TenantID:        "tenant_platform",
			SourceService:   "cdc-service",
			ExternalID:      "ext-001",
			CorrelationID:   "corr-001",
			ConnectorName:   "debezium_orders_outbox",
			SlotName:        "cdc_slot_orders",
			PublicationName: "cdc_pub_orders",
			Mode:            contracts.ModeOutbox,
		}
		err := rules.ValidateConnectorConfiguration(input)
		if err != nil {
			t.Fatalf("expected valid config to pass, got: %v", err)
		}
	})

	t.Run("InvalidSlotNamingRejected", func(t *testing.T) {
		input := contracts.RegisterConnectorInput{
			ConnectorName:   "debezium_orders",
			SlotName:        "invalid_slot_name",
			PublicationName: "cdc_pub_orders",
			Mode:            contracts.ModeOutbox,
		}
		err := rules.ValidateConnectorConfiguration(input)
		if err == nil {
			t.Fatalf("expected error on invalid slot naming, got nil")
		}
	})

	t.Run("WildcardPublicationRejected", func(t *testing.T) {
		input := contracts.RegisterConnectorInput{
			ConnectorName:   "debezium_orders",
			SlotName:        "cdc_slot_orders",
			PublicationName: "all_tables",
			Mode:            contracts.ModeOutbox,
		}
		err := rules.ValidateConnectorConfiguration(input)
		if err != rules.ErrWildcardPublication {
			t.Fatalf("expected ErrWildcardPublication, got: %v", err)
		}
	})
}

func TestCDCRules_ValidateSignalForMode(t *testing.T) {
	t.Run("OutboxModeRejectsSnapshotSignal", func(t *testing.T) {
		err := rules.ValidateSignalForMode(contracts.ModeOutbox, contracts.SignalExecuteSnapshot)
		if err != rules.ErrOutboxSnapshotForbidden {
			t.Fatalf("expected ErrOutboxSnapshotForbidden, got: %v", err)
		}
	})

	t.Run("RawTableModeAllowsSnapshotSignal", func(t *testing.T) {
		err := rules.ValidateSignalForMode(contracts.ModeRawTable, contracts.SignalExecuteSnapshot)
		if err != nil {
			t.Fatalf("expected raw table mode to allow snapshot, got: %v", err)
		}
	})

	t.Run("OutboxModeAllowsCheckpointSignal", func(t *testing.T) {
		err := rules.ValidateSignalForMode(contracts.ModeOutbox, contracts.SignalCheckpoint)
		if err != nil {
			t.Fatalf("expected outbox mode to allow checkpoint signal, got: %v", err)
		}
	})
}

func TestCDCRules_EvaluateWALCircuitBreaker(t *testing.T) {
	t.Run("AmpleHeadroomAllowsStreaming", func(t *testing.T) {
		res := rules.EvaluateWALCircuitBreaker(10*1024*1024, 1024*1024, 100*1024*1024*1024)
		if res.Tripped {
			t.Fatalf("expected breaker not tripped with ample disk headroom")
		}
	})

	t.Run("ThreatHoursBelowOneTripsEmergencyPause", func(t *testing.T) {
		genRate := int64(10 * 1024 * 1024)
		diskFree := int64(20 * 1024 * 1024)
		retained := int64(15 * 1024 * 1024)

		res := rules.EvaluateWALCircuitBreaker(retained, genRate, diskFree)
		if !res.Tripped {
			t.Fatalf("expected breaker tripped when time-to-full < 1hr")
		}
		if res.RecommendedAction != "EMERGENCY_PAUSE_CONNECTOR" {
			t.Fatalf("expected EMERGENCY_PAUSE_CONNECTOR, got: %s", res.RecommendedAction)
		}
	})
}

func TestCDCRules_EvaluateCanarySLA(t *testing.T) {
	t.Run("WithinSLAMarksSuccessful", func(t *testing.T) {
		res := rules.EvaluateCanarySLA(150, 5000)
		if !res.WithinSLO {
			t.Fatalf("expected canary within SLO for 150ms")
		}
	})

	t.Run("ExceededSLAMarksBreached", func(t *testing.T) {
		res := rules.EvaluateCanarySLA(6500, 5000)
		if res.WithinSLO {
			t.Fatalf("expected canary breached for 6500ms > 5000ms")
		}
	})
}


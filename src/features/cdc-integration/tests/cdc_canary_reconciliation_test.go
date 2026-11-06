package tests

import (
	"context"
	"testing"
	"time"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/machines"
	"event-driven-platform/src/features/cdc-integration/infrastructure/adapters"
	"event-driven-platform/src/features/cdc-integration/service"
	"event-driven-platform/src/shared/utils"
)

type memoryCDCRepo struct {
	connectors map[string]*contracts.ConnectorInventoryRecord
	canaries   map[string]time.Time
}

func newMemoryCDCRepo() *memoryCDCRepo {
	return &memoryCDCRepo{
		connectors: make(map[string]*contracts.ConnectorInventoryRecord),
		canaries:   make(map[string]time.Time),
	}
}

func (m *memoryCDCRepo) RegisterConnector(_ context.Context, record contracts.ConnectorInventoryRecord) error {
	m.connectors[record.ConnectorName] = &record
	return nil
}

func (m *memoryCDCRepo) GetConnector(_ context.Context, connectorName string) (*contracts.ConnectorInventoryRecord, error) {
	rec, ok := m.connectors[connectorName]
	if !ok {
		return nil, nil
	}
	return rec, nil
}

func (m *memoryCDCRepo) UpdateConnectorState(_ context.Context, connectorName string, state contracts.CDCConnectorState, version int64) error {
	rec, ok := m.connectors[connectorName]
	if !ok {
		return nil
	}
	rec.State = state
	rec.Version = version + 1
	return nil
}

func (m *memoryCDCRepo) UpdateWALLag(_ context.Context, connectorName string, retainedBytes int64, lagBytes int64) error {
	rec, ok := m.connectors[connectorName]
	if !ok {
		return nil
	}
	rec.WALRetainedBytes = retainedBytes
	rec.WALLagBytes = lagBytes
	return nil
}

func (m *memoryCDCRepo) RecordSignal(_ context.Context, _ contracts.SignalRecord) error {
	return nil
}

func (m *memoryCDCRepo) MarkSignalProcessed(_ context.Context, _ string) error {
	return nil
}

func (m *memoryCDCRepo) RecordHeartbeat(_ context.Context, _ contracts.HeartbeatTickInput) error {
	return nil
}

func (m *memoryCDCRepo) RecordCanarySent(_ context.Context, input contracts.CanaryCheckInput) error {
	m.canaries[input.CanaryEventID] = time.Now()
	return nil
}

func (m *memoryCDCRepo) RecordCanaryReceived(_ context.Context, canaryEventID string, _ int64) error {
	delete(m.canaries, canaryEventID)
	return nil
}

func TestCDCCanaryReconciliation_EndToEnd(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryCDCRepo()
	connect := adapters.NewMockKafkaConnectAdapter()
	machine := machines.NewCDCConnectorMachine()
	svc := service.NewCDCService(repo, connect, machine)

	connName := "debezium_canary_test"
	_ = repo.RegisterConnector(ctx, contracts.ConnectorInventoryRecord{
		ConnectorName: connName,
		SlotName:      "cdc_slot_canary",
		State:         contracts.StateRunning,
	})

	canaryUUID, _ := utils.GenerateUUIDv7()
	canaryInput := contracts.CanaryCheckInput{
		TenantID:      "tenant_platform",
		SourceService: "canary-cron",
		ExternalID:    "canary-01",
		CorrelationID: canaryUUID,
		ConnectorName: connName,
		CanaryEventID: canaryUUID,
	}

	err := svc.SendCanary(ctx, canaryInput)
	if err != nil {
		t.Fatalf("failed to record canary sent: %v", err)
	}

	sentTime := time.Now().Add(-120 * time.Millisecond)
	res, recErr := svc.ReconcileCanary(ctx, canaryUUID, sentTime, 5000)
	if recErr != nil {
		t.Fatalf("failed to reconcile canary: %v", recErr)
	}

	if !res.Reconciled {
		t.Fatalf("expected canary reconciled = true")
	}
	if !res.WithinSLO {
		t.Fatalf("expected canary within SLO, latency: %d ms", res.LatencyMs)
	}
}


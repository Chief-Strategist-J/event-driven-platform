package ports

import (
	"context"

	"event-driven-platform/src/features/cdc-integration/contracts"
)

type CDCRepositoryPort interface {
	RegisterConnector(ctx context.Context, record contracts.ConnectorInventoryRecord) error
	GetConnector(ctx context.Context, connectorName string) (*contracts.ConnectorInventoryRecord, error)
	UpdateConnectorState(ctx context.Context, connectorName string, state contracts.CDCConnectorState, version int64) error
	UpdateWALLag(ctx context.Context, connectorName string, retainedBytes int64, lagBytes int64) error
	RecordSignal(ctx context.Context, signal contracts.SignalRecord) error
	MarkSignalProcessed(ctx context.Context, signalID string) error
	RecordHeartbeat(ctx context.Context, input contracts.HeartbeatTickInput) error
	RecordCanarySent(ctx context.Context, input contracts.CanaryCheckInput) error
	RecordCanaryReceived(ctx context.Context, canaryEventID string, latencyMs int64) error
}


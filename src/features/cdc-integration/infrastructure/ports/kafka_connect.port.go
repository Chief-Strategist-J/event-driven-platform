package ports

import (
	"context"

	"event-driven-platform/src/features/cdc-integration/contracts"
)

type KafkaConnectPort interface {
	CreateConnector(ctx context.Context, input contracts.RegisterConnectorInput) error
	PauseConnector(ctx context.Context, connectorName string) error
	ResumeConnector(ctx context.Context, connectorName string) error
	RestartConnector(ctx context.Context, connectorName string) error
	DeleteConnector(ctx context.Context, connectorName string) error
}


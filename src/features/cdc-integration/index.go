package cdcintegration

import (
	"database/sql"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/machines"
	"event-driven-platform/src/features/cdc-integration/infrastructure/adapters"
	"event-driven-platform/src/features/cdc-integration/infrastructure/ports"
	"event-driven-platform/src/features/cdc-integration/service"
)

type CDCIntegrationFeature struct {
	Service *service.CDCService
	Repo    ports.CDCRepositoryPort
	Connect ports.KafkaConnectPort
}

func NewCDCIntegrationFeature(db *sql.DB) *CDCIntegrationFeature {
	repo := adapters.NewPostgresCDCRepositoryAdapter(db)
	connect := adapters.NewMockKafkaConnectAdapter()
	machine := machines.NewCDCConnectorMachine()
	svc := service.NewCDCService(repo, connect, machine)

	return &CDCIntegrationFeature{
		Service: svc,
		Repo:    repo,
		Connect: connect,
	}
}

type (
	RegisterConnectorInput   = contracts.RegisterConnectorInput
	DispatchSignalInput      = contracts.DispatchSignalInput
	HeartbeatTickInput       = contracts.HeartbeatTickInput
	CanaryCheckInput         = contracts.CanaryCheckInput
	ConnectorInventoryRecord = contracts.ConnectorInventoryRecord
	SignalRecord             = contracts.SignalRecord
	CanaryCheckResult        = contracts.CanaryCheckResult
)


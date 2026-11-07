package cdcintegration

import (
	"database/sql"
	"log/slog"
	"os"
	"strings"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/machines"
	"event-driven-platform/src/features/cdc-integration/infrastructure/adapters"
	"event-driven-platform/src/features/cdc-integration/infrastructure/ports"
	"event-driven-platform/src/features/cdc-integration/service"
)

// newKafkaConnectFromEnv returns the real Kafka Connect REST adapter when
// KAFKA_CONNECT_URL is set, otherwise the in-memory mock.
func newKafkaConnectFromEnv() ports.KafkaConnectPort {
	if u := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL")); u != "" {
		return adapters.NewKafkaConnectRESTAdapter(u, adapters.LoadDebeziumDBConfigFromEnv())
	}
	slog.Warn("CDC: KAFKA_CONNECT_URL not set, using in-memory mock connector adapter (no Debezium)")
	return adapters.NewMockKafkaConnectAdapter()
}

type CDCIntegrationFeature struct {
	Service *service.CDCService
	Repo    ports.CDCRepositoryPort
	Connect ports.KafkaConnectPort
}

func NewCDCIntegrationFeature(db *sql.DB) *CDCIntegrationFeature {
	repo := adapters.NewPostgresCDCRepositoryAdapter(db)
	connect := newKafkaConnectFromEnv()
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


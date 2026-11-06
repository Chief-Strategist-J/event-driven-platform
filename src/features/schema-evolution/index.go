package schemaevolution

import (
	"database/sql"

	"event-driven-platform/src/features/schema-evolution/contracts"
	"event-driven-platform/src/features/schema-evolution/infrastructure/adapters"
	"event-driven-platform/src/features/schema-evolution/infrastructure/ports"
	"event-driven-platform/src/features/schema-evolution/service"
)

type SchemaEvolutionService = service.SchemaEvolutionService
type SchemaEvolutionRepositoryPort = ports.SchemaEvolutionRepositoryPort

type SchemaVersionRecord = contracts.SchemaVersionRecord
type ConsumerSupportRecord = contracts.ConsumerSupportRecord
type DualPublishRecord = contracts.DualPublishRecord
type EvolutionIncidentRecord = contracts.EvolutionIncidentRecord

type RegisterSchemaInput = contracts.RegisterSchemaInput
type CheckConsumerReadinessInput = contracts.CheckConsumerReadinessInput
type ConsumerReadinessOutput = contracts.ConsumerReadinessOutput
type InitiateDualPublishInput = contracts.InitiateDualPublishInput
type EvaluateDecommissionInput = contracts.EvaluateDecommissionInput
type DecommissionCheckOutput = contracts.DecommissionCheckOutput
type RecordIncidentInput = contracts.RecordIncidentInput

func NewSchemaEvolutionService(repo ports.SchemaEvolutionRepositoryPort) *service.SchemaEvolutionService {
	return service.NewSchemaEvolutionService(repo)
}

func NewService(db *sql.DB) *service.SchemaEvolutionService {
	repo := adapters.NewPostgresSchemaEvolutionRepositoryAdapter(db)
	return service.NewSchemaEvolutionService(repo)
}

func NewPostgresSchemaEvolutionRepositoryAdapter(db *sql.DB) ports.SchemaEvolutionRepositoryPort {
	return adapters.NewPostgresSchemaEvolutionRepositoryAdapter(db)
}


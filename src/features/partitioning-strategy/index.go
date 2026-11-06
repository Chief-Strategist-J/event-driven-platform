package partitioning

import (
	"database/sql"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/adapters"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/ports"
	"event-driven-platform/src/features/partitioning-strategy/service"
)

type (
	TopicClass               = contracts.TopicClass
	MigrationPhase           = contracts.MigrationPhase
	TopicTopology            = contracts.TopicTopology
	RegisterTopicTopologyInput = contracts.RegisterTopicTopologyInput
	CalculatePartitionInput  = contracts.CalculatePartitionInput
	CalculatePartitionOutput = contracts.CalculatePartitionOutput
	PartitionThroughput      = contracts.PartitionThroughput
	EvaluateSkewInput        = contracts.EvaluateSkewInput
	PartitionSkewMetrics     = contracts.PartitionSkewMetrics
	SkewEvaluationOutput     = contracts.SkewEvaluationOutput
	InitiateMigrationInput   = contracts.InitiateMigrationInput
	AdvanceMigrationInput    = contracts.AdvanceMigrationInput
	MigrationRunbookRecord   = contracts.MigrationRunbookRecord
	PartitionRepositoryPort  = ports.PartitionRepositoryPort
	HasherPort               = ports.HasherPort
	PartitioningService      = service.PartitioningService
)

const (
	TopicClassOrdered   = contracts.TopicClassOrdered
	TopicClassCompacted = contracts.TopicClassCompacted
	TopicClassCopart    = contracts.TopicClassCopart
	TopicClassUnordered = contracts.TopicClassUnordered

	MigrationPhasePlanned            = contracts.MigrationPhasePlanned
	MigrationPhaseProvisioningTarget = contracts.MigrationPhaseProvisioningTarget
	MigrationPhaseDualPublishing     = contracts.MigrationPhaseDualPublishing
	MigrationPhaseDrainingSource     = contracts.MigrationPhaseDrainingSource
	MigrationPhaseCutover            = contracts.MigrationPhaseCutover
	MigrationPhaseDecommissioned     = contracts.MigrationPhaseDecommissioned
)

func NewService(db *sql.DB) *service.PartitioningService {
	repo := adapters.NewPartitionPostgresAdapter(db)
	hasher := adapters.NewMurmur2HasherAdapter()
	return service.NewPartitioningService(repo, hasher)
}


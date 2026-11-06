package ports

import (
	"context"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
)

type PartitionRepositoryPort interface {
	SaveTopicTopology(ctx context.Context, topology contracts.TopicTopology) error
	GetTopicTopology(ctx context.Context, tenantID, topicName string) (*contracts.TopicTopology, error)
	RecordSkewMetrics(ctx context.Context, metrics contracts.PartitionSkewMetrics) error
	CreateMigrationRunbook(ctx context.Context, record contracts.MigrationRunbookRecord) error
	GetMigrationRunbook(ctx context.Context, tenantID, migrationID string) (*contracts.MigrationRunbookRecord, error)
	UpdateMigrationPhase(ctx context.Context, tenantID, migrationID string, targetPhase contracts.MigrationPhase, expectedVersion int64) error
}

package adapters

import (
	"context"
	"database/sql"
	"errors"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/ports"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/queries"
	platformErrors "event-driven-platform/src/shared/errors"
)

type PartitionPostgresAdapter struct {
	db *sql.DB
}

func NewPartitionPostgresAdapter(db *sql.DB) ports.PartitionRepositoryPort {
	return &PartitionPostgresAdapter{db: db}
}

func (a *PartitionPostgresAdapter) SaveTopicTopology(ctx context.Context, top contracts.TopicTopology) error {
	_, err := a.db.ExecContext(ctx, queries.InsertTopicTopology,
		top.ID,
		top.PublicID,
		top.TenantID,
		top.SourceService,
		top.ExternalID,
		top.CorrelationID,
		top.TopicName,
		string(top.TopicClass),
		top.PartitionCount,
		top.ReplicationFactor,
		top.MinInSyncReplicas,
		top.AllowInPlaceExpansion,
		top.Version,
		top.CreatedAt,
		top.UpdatedAt,
	)
	if err != nil {
		return platformErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PartitionPostgresAdapter) GetTopicTopology(ctx context.Context, tenantID, topicName string) (*contracts.TopicTopology, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectTopicTopology, tenantID, topicName)

	var top contracts.TopicTopology
	var topicClassStr string

	err := row.Scan(
		&top.ID,
		&top.PublicID,
		&top.TenantID,
		&top.SourceService,
		&top.ExternalID,
		&top.CorrelationID,
		&top.TopicName,
		&topicClassStr,
		&top.PartitionCount,
		&top.ReplicationFactor,
		&top.MinInSyncReplicas,
		&top.AllowInPlaceExpansion,
		&top.Version,
		&top.CreatedAt,
		&top.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, platformErrors.NewNotFoundError("topic topology not found")
		}
		return nil, platformErrors.NewInternalError(err.Error())
	}

	top.TopicClass = contracts.TopicClass(topicClassStr)
	return &top, nil
}

func (a *PartitionPostgresAdapter) RecordSkewMetrics(ctx context.Context, metrics contracts.PartitionSkewMetrics) error {
	_, err := a.db.ExecContext(ctx, queries.InsertSkewMetrics,
		metrics.ID,
		metrics.PublicID,
		metrics.TenantID,
		metrics.SourceService,
		metrics.ExternalID,
		metrics.CorrelationID,
		metrics.TopicName,
		metrics.MaxThroughputEPS,
		metrics.MedianThroughputEPS,
		metrics.SkewRatio,
		metrics.HotPartitionID,
		metrics.IsAlertBreached,
		metrics.MeasuredAt,
	)
	if err != nil {
		return platformErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PartitionPostgresAdapter) CreateMigrationRunbook(ctx context.Context, rec contracts.MigrationRunbookRecord) error {
	_, err := a.db.ExecContext(ctx, queries.InsertMigrationRunbook,
		rec.ID,
		rec.PublicID,
		rec.TenantID,
		rec.SourceService,
		rec.ExternalID,
		rec.CorrelationID,
		rec.MigrationID,
		rec.SourceTopic,
		rec.TargetTopic,
		rec.SourcePartitions,
		rec.TargetPartitions,
		string(rec.Phase),
		rec.Version,
		rec.CreatedAt,
		rec.UpdatedAt,
	)
	if err != nil {
		return platformErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PartitionPostgresAdapter) GetMigrationRunbook(ctx context.Context, tenantID, migrationID string) (*contracts.MigrationRunbookRecord, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectMigrationRunbook, tenantID, migrationID)

	var rec contracts.MigrationRunbookRecord
	var phaseStr string

	err := row.Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.SourceService,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.MigrationID,
		&rec.SourceTopic,
		&rec.TargetTopic,
		&rec.SourcePartitions,
		&rec.TargetPartitions,
		&phaseStr,
		&rec.Version,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, platformErrors.NewNotFoundError("migration runbook not found")
		}
		return nil, platformErrors.NewInternalError(err.Error())
	}

	rec.Phase = contracts.MigrationPhase(phaseStr)
	return &rec, nil
}

func (a *PartitionPostgresAdapter) UpdateMigrationPhase(ctx context.Context, tenantID, migrationID string, targetPhase contracts.MigrationPhase, expectedVersion int64) error {
	res, err := a.db.ExecContext(ctx, queries.UpdateMigrationPhase, string(targetPhase), tenantID, migrationID, expectedVersion)
	if err != nil {
		return platformErrors.NewInternalError(err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return platformErrors.NewInternalError(err.Error())
	}
	if rows == 0 {
		return platformErrors.NewConflictError("migration phase update failed due to optimistic concurrency conflict")
	}
	return nil
}

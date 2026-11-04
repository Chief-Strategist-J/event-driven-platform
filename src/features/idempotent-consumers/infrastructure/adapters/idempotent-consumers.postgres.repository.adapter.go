package adapters

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/ports"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/queries"
	infraDb "event-driven-platform/src/infra/database"
)

type PostgresConsumerRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresConsumerRepositoryAdapter(db *sql.DB) ports.ConsumerRepositoryPort {
	return &PostgresConsumerRepositoryAdapter{db: db}
}

func (p *PostgresConsumerRepositoryAdapter) RecordInbox(ctx context.Context, record contracts.ConsumerInboxRecord) (bool, error) {
	res, err := p.db.ExecContext(
		ctx,
		queries.FlowInsertConsumerInboxRecord,
		record.ID,
		record.TenantID,
		record.HandlerID,
		record.EventID,
		record.AggregateType,
		record.AggregateID,
		record.AggregateVersion,
		record.EventType,
		record.PayloadHash,
	)
	if err != nil {
		return false, infraDb.MapDatabaseError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, infraDb.MapDatabaseError(err)
	}
	return rows > 0, nil
}

func (p *PostgresConsumerRepositoryAdapter) GetProjection(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (*contracts.AggregateProjectionState, error) {
	var proj contracts.AggregateProjectionState
	err := p.db.QueryRowContext(ctx, queries.FlowGetAggregateProjection, tenantID, handlerID, aggregateType, aggregateID).Scan(
		&proj.TenantID,
		&proj.HandlerID,
		&proj.AggregateType,
		&proj.AggregateID,
		&proj.LastVersion,
		&proj.LastEventID,
		&proj.LastPayloadHash,
		&proj.IsParked,
		&proj.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, infraDb.MapDatabaseError(err)
	}
	return &proj, nil
}

func (p *PostgresConsumerRepositoryAdapter) ApplyProjectionEffect(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string, version int64, eventID, payloadHash string) error {
	_, err := p.db.ExecContext(ctx, queries.FlowUpsertAggregateProjection, tenantID, handlerID, aggregateType, aggregateID, version, eventID, payloadHash)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresConsumerRepositoryAdapter) ParkAggregate(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string, _ string) error {
	_, err := p.db.ExecContext(ctx, queries.FlowParkAggregateProjection, tenantID, handlerID, aggregateType, aggregateID)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresConsumerRepositoryAdapter) RecordPartitionOffset(ctx context.Context, topic string, partition int, offset int64) error {
	_, err := p.db.ExecContext(ctx, queries.FlowStorePartitionOffset, topic, partition, offset)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

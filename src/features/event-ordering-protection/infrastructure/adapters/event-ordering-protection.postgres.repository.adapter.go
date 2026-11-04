package adapters

import (
	"context"
	"database/sql"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
	"event-driven-platform/src/features/event-ordering-protection/infrastructure/ports"
	"event-driven-platform/src/features/event-ordering-protection/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresEventOrderingAdapter struct {
	db *sql.DB
}

func NewPostgresEventOrderingAdapter(db *sql.DB) *PostgresEventOrderingAdapter {
	return &PostgresEventOrderingAdapter{db: db}
}

var _ ports.EventOrderingRepositoryPort = (*PostgresEventOrderingAdapter)(nil)

func (p *PostgresEventOrderingAdapter) GetSequenceTracker(ctx context.Context, tenantID, group, handler, aggType, aggID string) (*contracts.AggregateSequenceTrackerRecord, error) {
	row := p.db.QueryRowContext(ctx, queries.FlowGetSequenceTrackerForUpdate, tenantID, group, handler, aggType, aggID)
	var rec contracts.AggregateSequenceTrackerRecord
	var holdReason sql.NullString
	var blockingVer sql.NullInt64
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&rec.ConsumerGroup, &rec.HandlerID, &rec.AggregateType, &rec.AggregateID,
		&rec.LastAppliedVersion, &rec.LastPayloadHash, &rec.OwnerEpoch,
		&rec.IsHeld, &holdReason, &blockingVer, &rec.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, database.MapDatabaseError(err)
	}
	if holdReason.Valid {
		rec.HoldReason = contracts.HoldReason(holdReason.String)
	}
	if blockingVer.Valid {
		val := blockingVer.Int64
		rec.BlockingVersion = &val
	}
	return &rec, nil
}

func (p *PostgresEventOrderingAdapter) ApplyEventSequence(ctx context.Context, r contracts.AggregateSequenceTrackerRecord) error {
	var holdReason sql.NullString
	if r.HoldReason != "" {
		holdReason = sql.NullString{String: string(r.HoldReason), Valid: true}
	}
	_, err := p.db.ExecContext(ctx, queries.FlowUpsertSequenceTrackerApplied,
		r.ID, r.PublicID, r.TenantID, r.SourceService, r.ExternalID, r.CorrelationID,
		r.ConsumerGroup, r.HandlerID, r.AggregateType, r.AggregateID,
		r.LastAppliedVersion, r.LastPayloadHash, r.OwnerEpoch,
		r.IsHeld, holdReason,
	)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresEventOrderingAdapter) HoldAggregateKey(ctx context.Context, r contracts.AggregateSequenceTrackerRecord) error {
	var holdReason sql.NullString
	if r.HoldReason != "" {
		holdReason = sql.NullString{String: string(r.HoldReason), Valid: true}
	}
	var blockingVer sql.NullInt64
	if r.BlockingVersion != nil {
		blockingVer = sql.NullInt64{Int64: *r.BlockingVersion, Valid: true}
	}
	_, err := p.db.ExecContext(ctx, queries.FlowHoldAggregateSequence,
		r.ID, r.PublicID, r.TenantID, r.SourceService, r.ExternalID, r.CorrelationID,
		r.ConsumerGroup, r.HandlerID, r.AggregateType, r.AggregateID,
		r.LastPayloadHash, r.OwnerEpoch, holdReason, blockingVer,
	)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresEventOrderingAdapter) StoreHeldEvent(ctx context.Context, r contracts.SequenceHeldEventRecord) error {
	headers := r.Headers
	if len(headers) == 0 {
		headers = []byte("{}")
	}
	_, err := p.db.ExecContext(ctx, queries.FlowInsertHeldEvent,
		r.ID, r.PublicID, r.TenantID, r.SourceService, r.ExternalID, r.CorrelationID,
		r.ConsumerGroup, r.HandlerID, r.AggregateType, r.AggregateID, r.AggregateVersion,
		r.EventID, r.EventType, r.Payload, r.PayloadHash, headers, r.SourceOffset,
	)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresEventOrderingAdapter) GetHeldEventsForDrain(ctx context.Context, tenantID, group, handler, aggType, aggID string) ([]contracts.SequenceHeldEventRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowGetHeldEventsForDrain, tenantID, group, handler, aggType, aggID)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var list []contracts.SequenceHeldEventRecord
	for rows.Next() {
		var rec contracts.SequenceHeldEventRecord
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.ConsumerGroup, &rec.HandlerID, &rec.AggregateType, &rec.AggregateID, &rec.AggregateVersion,
			&rec.EventID, &rec.EventType, &rec.Payload, &rec.PayloadHash, &rec.Headers, &rec.SourceOffset, &rec.ArrivedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		list = append(list, rec)
	}
	return list, nil
}

func (p *PostgresEventOrderingAdapter) DeleteHeldEvent(ctx context.Context, tenantID, group, handler, aggType, aggID string, version int64) error {
	_, err := p.db.ExecContext(ctx, queries.FlowDeleteHeldEvent, tenantID, group, handler, aggType, aggID, version)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresEventOrderingAdapter) ListHeldTrackers(ctx context.Context, tenantID, group, handler string, limit, offset int) ([]contracts.AggregateSequenceTrackerRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowListHeldTrackers, tenantID, group, handler, limit, offset)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var list []contracts.AggregateSequenceTrackerRecord
	for rows.Next() {
		var rec contracts.AggregateSequenceTrackerRecord
		var holdReason sql.NullString
		var blockingVer sql.NullInt64
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.ConsumerGroup, &rec.HandlerID, &rec.AggregateType, &rec.AggregateID,
			&rec.LastAppliedVersion, &rec.LastPayloadHash, &rec.OwnerEpoch,
			&rec.IsHeld, &holdReason, &blockingVer, &rec.UpdatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		if holdReason.Valid {
			rec.HoldReason = contracts.HoldReason(holdReason.String)
		}
		if blockingVer.Valid {
			val := blockingVer.Int64
			rec.BlockingVersion = &val
		}
		list = append(list, rec)
	}
	return list, nil
}


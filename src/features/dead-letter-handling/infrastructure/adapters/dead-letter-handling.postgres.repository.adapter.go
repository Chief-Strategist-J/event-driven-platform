package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/ports"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/queries"
	infraDb "event-driven-platform/src/infra/database"
)

type PostgresDeadLetterRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresDeadLetterRepositoryAdapter(db *sql.DB) ports.DeadLetterRepositoryPort {
	return &PostgresDeadLetterRepositoryAdapter{db: db}
}

func (p *PostgresDeadLetterRepositoryAdapter) Quarantine(ctx context.Context, record contracts.DeadLetterQuarantineRecord) error {
	headersJSON, err := json.Marshal(record.Headers)
	if err != nil {
		headersJSON = []byte("{}")
	}

	_, err = p.db.ExecContext(
		ctx,
		queries.FlowInsertDeadLetterQuarantine,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.Topic,
		record.Partition,
		record.SourceOffset,
		record.ConsumerGroup,
		record.HandlerID,
		record.EventID,
		record.AggregateType,
		record.AggregateID,
		record.AggregateVersion,
		record.EventType,
		record.Payload,
		headersJSON,
		record.FailureReason,
		record.ErrorCode,
		record.ErrorFingerprint,
		record.Status,
		record.RedriveCount,
		record.MaxRedriveAttempts,
	)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) GetQuarantinedByID(ctx context.Context, tenantID, id string) (*contracts.DeadLetterQuarantineRecord, error) {
	var rec contracts.DeadLetterQuarantineRecord
	var headersRaw []byte

	err := p.db.QueryRowContext(ctx, queries.FlowGetQuarantinedByID, tenantID, id).Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.SourceService,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.Topic,
		&rec.Partition,
		&rec.SourceOffset,
		&rec.ConsumerGroup,
		&rec.HandlerID,
		&rec.EventID,
		&rec.AggregateType,
		&rec.AggregateID,
		&rec.AggregateVersion,
		&rec.EventType,
		&rec.Payload,
		&headersRaw,
		&rec.FailureReason,
		&rec.ErrorCode,
		&rec.ErrorFingerprint,
		&rec.Status,
		&rec.RedriveCount,
		&rec.MaxRedriveAttempts,
		&rec.QuarantinedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, infraDb.MapDatabaseError(err)
	}

	if len(headersRaw) > 0 {
		_ = json.Unmarshal(headersRaw, &rec.Headers)
	}

	return &rec, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) ListQuarantined(ctx context.Context, tenantID, handlerID string, status contracts.QuarantineStatus, limit, offset int) ([]contracts.DeadLetterQuarantineRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowListQuarantined, tenantID, handlerID, string(status), limit, offset)
	if err != nil {
		return nil, infraDb.MapDatabaseError(err)
	}
	defer rows.Close()

	var results []contracts.DeadLetterQuarantineRecord
	for rows.Next() {
		var rec contracts.DeadLetterQuarantineRecord
		var headersRaw []byte

		if err := rows.Scan(
			&rec.ID,
			&rec.PublicID,
			&rec.TenantID,
			&rec.SourceService,
			&rec.ExternalID,
			&rec.CorrelationID,
			&rec.Topic,
			&rec.Partition,
			&rec.SourceOffset,
			&rec.ConsumerGroup,
			&rec.HandlerID,
			&rec.EventID,
			&rec.AggregateType,
			&rec.AggregateID,
			&rec.AggregateVersion,
			&rec.EventType,
			&rec.Payload,
			&headersRaw,
			&rec.FailureReason,
			&rec.ErrorCode,
			&rec.ErrorFingerprint,
			&rec.Status,
			&rec.RedriveCount,
			&rec.MaxRedriveAttempts,
			&rec.QuarantinedAt,
			&rec.UpdatedAt,
		); err != nil {
			return nil, infraDb.MapDatabaseError(err)
		}

		if len(headersRaw) > 0 {
			_ = json.Unmarshal(headersRaw, &rec.Headers)
		}
		results = append(results, rec)
	}

	return results, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) UpdateDisposition(ctx context.Context, tenantID, id string, status contracts.QuarantineStatus, redriveCount int) error {
	_, err := p.db.ExecContext(ctx, queries.FlowUpdateQuarantineDisposition, tenantID, id, string(status), redriveCount)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) ParkKey(ctx context.Context, park contracts.AggregateParkingState) error {
	_, err := p.db.ExecContext(
		ctx,
		queries.FlowParkAggregateKey,
		park.TenantID,
		park.HandlerID,
		park.AggregateType,
		park.AggregateID,
		park.ParkedReason,
		park.QuarantineID,
	)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) UnparkKey(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) error {
	_, err := p.db.ExecContext(ctx, queries.FlowUnparkAggregateKey, tenantID, handlerID, aggregateType, aggregateID)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) IsKeyParked(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error) {
	var isParked bool
	err := p.db.QueryRowContext(ctx, queries.FlowCheckKeyParked, tenantID, handlerID, aggregateType, aggregateID).Scan(&isParked)
	if err != nil {
		return false, infraDb.MapDatabaseError(err)
	}
	return isParked, nil
}


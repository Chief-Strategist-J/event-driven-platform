package adapters

import (
	"context"
	"database/sql"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/infrastructure/ports"
	"event-driven-platform/src/features/retry-topics/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresRetryTopicsAdapter struct {
	db *sql.DB
}

func NewPostgresRetryTopicsAdapter(db *sql.DB) *PostgresRetryTopicsAdapter {
	return &PostgresRetryTopicsAdapter{db: db}
}

var _ ports.RetryTopicsRepositoryPort = (*PostgresRetryTopicsAdapter)(nil)

func (p *PostgresRetryTopicsAdapter) RecordRetryAttempt(ctx context.Context, rec contracts.RetryAttemptLedgerRecord) (*contracts.RetryAttemptLedgerRecord, error) {
	row := p.db.QueryRowContext(ctx, queries.FlowInsertRetryAttempt,
		rec.ID, rec.PublicID, rec.TenantID, rec.SourceService, rec.ExternalID, rec.CorrelationID,
		rec.ConsumerGroup, rec.HandlerID, rec.EventID, rec.AggregateType, rec.AggregateID, rec.AggregateVersion,
		rec.AttemptNumber, rec.MaxAttempts, rec.BackoffMs, rec.DueAt, string(rec.Status),
		string(rec.FailureClassification), rec.IdempotencyKey, rec.ErrorMessage, rec.ErrorCode, rec.ValidUntil,
	)

	err := row.Scan(&rec.CreatedAt, &rec.UpdatedAt)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	return &rec, nil
}

func (p *PostgresRetryTopicsAdapter) GetDueRetryAttempts(ctx context.Context, tenantID string, asOf time.Time, limit int) ([]contracts.RetryAttemptLedgerRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowGetDueRetryAttempts, tenantID, asOf, limit)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var records []contracts.RetryAttemptLedgerRecord
	for rows.Next() {
		var rec contracts.RetryAttemptLedgerRecord
		var statusStr, classStr string
		var validUntil sql.NullTime

		err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.ConsumerGroup, &rec.HandlerID, &rec.EventID, &rec.AggregateType, &rec.AggregateID, &rec.AggregateVersion,
			&rec.AttemptNumber, &rec.MaxAttempts, &rec.BackoffMs, &rec.DueAt, &statusStr,
			&classStr, &rec.IdempotencyKey, &rec.ErrorMessage, &rec.ErrorCode, &validUntil,
			&rec.CreatedAt, &rec.UpdatedAt,
		)
		if err != nil {
			return nil, database.MapDatabaseError(err)
		}
		rec.Status = contracts.RetryAttemptStatus(statusStr)
		rec.FailureClassification = contracts.FailureClassification(classStr)
		if validUntil.Valid {
			tVal := validUntil.Time
			rec.ValidUntil = &tVal
		}
		records = append(records, rec)
	}

	return records, nil
}

func (p *PostgresRetryTopicsAdapter) UpdateRetryStatus(ctx context.Context, tenantID, id string, status contracts.RetryAttemptStatus) error {
	_, err := p.db.ExecContext(ctx, queries.FlowUpdateRetryStatus, tenantID, id, string(status))
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}


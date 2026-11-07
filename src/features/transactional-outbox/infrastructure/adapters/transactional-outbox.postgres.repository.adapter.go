package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/lib/pq"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/ports"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/queries"
	appErrors "event-driven-platform/src/shared/errors"
)

// mapInsertError converts unique violations on outbox inserts into 409 conflicts.
func mapInsertError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		switch pqErr.Constraint {
		case "outbox_external_uk":
			return appErrors.NewConflictError("an outbox event with the same externalId already exists for this tenant and source service")
		case "outbox_aggregate_version_uk":
			return appErrors.NewConflictError("an outbox event for this aggregate version already exists")
		default:
			return appErrors.NewConflictError("duplicate outbox event")
		}
	}
	return err
}

type PostgresOutboxRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresOutboxRepositoryAdapter(db *sql.DB) ports.OutboxRepositoryPort {
	return &PostgresOutboxRepositoryAdapter{db: db}
}

func (p *PostgresOutboxRepositoryAdapter) ClaimIdempotency(ctx context.Context, tenantID, idempotencyKey, recordID string) (bool, string, error) {
	var returnedID string
	err := p.db.QueryRowContext(ctx, queries.FlowClaimIdempotencyKey, tenantID, idempotencyKey, recordID).Scan(&returnedID)
	if err == nil {
		return true, returnedID, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		queryExisting := `SELECT record_id FROM outbox_idempotency_keys WHERE tenant_id = $1 AND idempotency_key = $2`
		if queryErr := p.db.QueryRowContext(ctx, queryExisting, tenantID, idempotencyKey).Scan(&returnedID); queryErr != nil {
			return false, "", queryErr
		}
		return false, returnedID, nil
	}
	return false, "", err
}

func (p *PostgresOutboxRepositoryAdapter) Save(ctx context.Context, record contracts.OutboxRecord) error {
	headersJSON, err := json.Marshal(record.Headers)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(
		ctx,
		queries.FlowInsertOutboxRecord,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.CausationID,
		record.AggregateType,
		record.AggregateID,
		record.AggregateVersion,
		record.EventType,
		record.SchemaVersion,
		record.Payload,
		headersJSON,
	)
	return mapInsertError(err)
}

func (p *PostgresOutboxRepositoryAdapter) SaveAndImmediateDelete(ctx context.Context, record contracts.OutboxRecord) error {
	headersJSON, err := json.Marshal(record.Headers)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(
		ctx,
		queries.FlowInsertAndDeleteOutboxRecordCDC,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.CausationID,
		record.AggregateType,
		record.AggregateID,
		record.AggregateVersion,
		record.EventType,
		record.SchemaVersion,
		record.Payload,
		headersJSON,
	)
	return mapInsertError(err)
}

func (p *PostgresOutboxRepositoryAdapter) GetByID(ctx context.Context, tenantID string, id string) (*contracts.OutboxRecord, error) {
	query := `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
    aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
    payload, headers, status, worker_id, lease_expires_at, retry_count, last_error, created_at, updated_at, processed_at
FROM outbox
WHERE tenant_id = $1 AND id = $2;`

	var rec contracts.OutboxRecord
	var workerID sql.NullString
	var lastError sql.NullString
	var leaseExpiresAt sql.NullTime
	var processedAt sql.NullTime
	var headersJSON []byte

	err := p.db.QueryRowContext(ctx, query, tenantID, id).Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.SourceService,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.CausationID,
		&rec.AggregateType,
		&rec.AggregateID,
		&rec.AggregateVersion,
		&rec.EventType,
		&rec.SchemaVersion,
		&rec.Payload,
		&headersJSON,
		&rec.Status,
		&workerID,
		&leaseExpiresAt,
		&rec.RetryCount,
		&lastError,
		&rec.CreatedAt,
		&rec.UpdatedAt,
		&processedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	if workerID.Valid {
		rec.WorkerID = workerID.String
	}
	if lastError.Valid {
		rec.LastError = lastError.String
	}
	if leaseExpiresAt.Valid {
		rec.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if processedAt.Valid {
		rec.ProcessedAt = &processedAt.Time
	}
	if len(headersJSON) > 0 {
		_ = json.Unmarshal(headersJSON, &rec.Headers)
	}

	return &rec, nil
}

func (p *PostgresOutboxRepositoryAdapter) FetchPending(ctx context.Context, limit int) ([]contracts.OutboxRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowFetchPendingOutboxForUpdateSkipLocked, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return p.scanRows(rows)
}

func (p *PostgresOutboxRepositoryAdapter) ClaimInFlight(ctx context.Context, workerID string, leaseDuration time.Duration, limit int) ([]contracts.OutboxRecord, error) {
	leaseSeconds := int(leaseDuration.Seconds())
	rows, err := p.db.QueryContext(ctx, queries.FlowClaimInFlightOutboxRecords, workerID, leaseSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return p.scanRows(rows)
}

func (p *PostgresOutboxRepositoryAdapter) MarkPublished(ctx context.Context, tenantID string, id string) error {
	res, err := p.db.ExecContext(ctx, queries.FlowMarkOutboxPublished, tenantID, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrRecordNotFound
	}
	return nil
}

func (p *PostgresOutboxRepositoryAdapter) MarkPublishedByWorker(ctx context.Context, tenantID string, id string, workerID string) error {
	res, err := p.db.ExecContext(ctx, queries.FlowMarkOutboxPublishedByWorker, tenantID, id, workerID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrWorkerOwnershipLost
	}
	return nil
}

func (p *PostgresOutboxRepositoryAdapter) MarkFailed(ctx context.Context, tenantID string, id string, errMessage string) error {
	res, err := p.db.ExecContext(ctx, queries.FlowMarkOutboxFailed, tenantID, id, errMessage)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrRecordNotFound
	}
	return nil
}

func (p *PostgresOutboxRepositoryAdapter) PurgeExpired(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, queries.FlowPurgeExpiredPublishedRecords)
	return err
}

func (p *PostgresOutboxRepositoryAdapter) scanRows(rows *sql.Rows) ([]contracts.OutboxRecord, error) {
	var results []contracts.OutboxRecord
	for rows.Next() {
		var rec contracts.OutboxRecord
		var workerID sql.NullString
		var lastError sql.NullString
		var leaseExpiresAt sql.NullTime
		var processedAt sql.NullTime
		var headersJSON []byte

		err := rows.Scan(
			&rec.ID,
			&rec.PublicID,
			&rec.TenantID,
			&rec.SourceService,
			&rec.ExternalID,
			&rec.CorrelationID,
			&rec.CausationID,
			&rec.AggregateType,
			&rec.AggregateID,
			&rec.AggregateVersion,
			&rec.EventType,
			&rec.SchemaVersion,
			&rec.Payload,
			&headersJSON,
			&rec.Status,
			&workerID,
			&leaseExpiresAt,
			&rec.RetryCount,
			&lastError,
			&rec.CreatedAt,
			&rec.UpdatedAt,
			&processedAt,
		)
		if err != nil {
			return nil, err
		}

		if workerID.Valid {
			rec.WorkerID = workerID.String
		}
		if lastError.Valid {
			rec.LastError = lastError.String
		}
		if leaseExpiresAt.Valid {
			rec.LeaseExpiresAt = &leaseExpiresAt.Time
		}
		if processedAt.Valid {
			rec.ProcessedAt = &processedAt.Time
		}
		if len(headersJSON) > 0 {
			_ = json.Unmarshal(headersJSON, &rec.Headers)
		}

		results = append(results, rec)
	}
	return results, rows.Err()
}


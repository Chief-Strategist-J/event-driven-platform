package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"event-driven-platform/src/features/backpressure/contracts"
	"event-driven-platform/src/features/backpressure/infrastructure/ports"
	"event-driven-platform/src/features/backpressure/infrastructure/queries"
	"event-driven-platform/src/infra/observability"
)

type BackpressurePostgresRepositoryAdapter struct {
	db *sql.DB
}

func NewBackpressurePostgresRepositoryAdapter(db *sql.DB) ports.BackpressureRepositoryPort {
	return &BackpressurePostgresRepositoryAdapter{db: db}
}

func (a *BackpressurePostgresRepositoryAdapter) UpsertBackpressureRecord(ctx context.Context, record contracts.BackpressureRecord) (*contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.postgres.upsert_record")
	defer span.End()

	var createdAt, updatedAt time.Time
	var returnedID, returnedPublicID string
	var version int64

	err := a.db.QueryRowContext(
		ctx,
		queries.FlowUpsertBackpressureRecord,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.HandlerID,
		record.DependencyID,
		string(record.State),
		string(record.PauseReason),
		record.ConcurrencyLimit,
		record.PausedAt,
		record.ResumedAt,
	).Scan(&returnedID, &returnedPublicID, &version, &createdAt, &updatedAt)

	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("backpressure upsert failed: %w", err)
	}

	record.ID = returnedID
	record.PublicID = returnedPublicID
	record.Version = version
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	return &record, nil
}

func (a *BackpressurePostgresRepositoryAdapter) GetBackpressureRecord(ctx context.Context, tenantID, handlerID, dependencyID string) (*contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.postgres.get_record")
	defer span.End()

	var rec contracts.BackpressureRecord
	var stateStr, pauseReasonStr string

	err := a.db.QueryRowContext(
		ctx,
		queries.FlowGetBackpressureRecord,
		tenantID,
		handlerID,
		dependencyID,
	).Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.SourceService,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.HandlerID,
		&rec.DependencyID,
		&stateStr,
		&pauseReasonStr,
		&rec.ConcurrencyLimit,
		&rec.Version,
		&rec.PausedAt,
		&rec.ResumedAt,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("backpressure record not found for handler=%s dependency=%s", handlerID, dependencyID)
	}
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("backpressure get failed: %w", err)
	}

	rec.State = contracts.BackpressureState(stateStr)
	rec.PauseReason = contracts.PauseReason(pauseReasonStr)
	return &rec, nil
}

func (a *BackpressurePostgresRepositoryAdapter) ListBackpressureByTenant(ctx context.Context, tenantID string) ([]contracts.BackpressureRecord, error) {
	ctx, span := observability.StartSpan(ctx, "backpressure.postgres.list_by_tenant")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowListBackpressureByTenant, tenantID)
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("backpressure list failed: %w", err)
	}
	defer rows.Close()

	result := make([]contracts.BackpressureRecord, 0)
	for rows.Next() {
		var rec contracts.BackpressureRecord
		var stateStr, pauseReasonStr string
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService,
			&rec.ExternalID, &rec.CorrelationID, &rec.HandlerID, &rec.DependencyID,
			&stateStr, &pauseReasonStr, &rec.ConcurrencyLimit, &rec.Version,
			&rec.PausedAt, &rec.ResumedAt, &rec.CreatedAt, &rec.UpdatedAt,
		); err != nil {
			observability.RecordError(span, err)
			return nil, fmt.Errorf("backpressure row scan failed: %w", err)
		}
		rec.State = contracts.BackpressureState(stateStr)
		rec.PauseReason = contracts.PauseReason(pauseReasonStr)
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("backpressure rows iteration failed: %w", err)
	}
	return result, nil
}

func (a *BackpressurePostgresRepositoryAdapter) RecordAuditLog(ctx context.Context, audit contracts.BackpressureAuditRecord) error {
	ctx, span := observability.StartSpan(ctx, "backpressure.postgres.record_audit")
	defer span.End()

	var createdAt time.Time
	err := a.db.QueryRowContext(
		ctx,
		queries.FlowInsertBackpressureAudit,
		audit.ID,
		audit.PublicID,
		audit.TenantID,
		audit.SourceService,
		audit.ExternalID,
		audit.CorrelationID,
		audit.HandlerID,
		audit.DependencyID,
		string(audit.FromState),
		string(audit.ToState),
		string(audit.Event),
		string(audit.PauseReason),
		audit.Reason,
	).Scan(&createdAt)

	if err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("backpressure audit insert failed: %w", err)
	}
	return nil
}


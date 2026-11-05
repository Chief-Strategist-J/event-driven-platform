package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"event-driven-platform/src/features/replay-mechanism/contracts"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/ports"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/queries"
	"event-driven-platform/src/infra/observability"
)

type ReplayPostgresRepositoryAdapter struct {
	db *sql.DB
}

func NewReplayPostgresRepositoryAdapter(db *sql.DB) ports.ReplayRepositoryPort {
	return &ReplayPostgresRepositoryAdapter{db: db}
}

func (a *ReplayPostgresRepositoryAdapter) CreateReplayRun(ctx context.Context, run contracts.ReplayRunRecord) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.create_run")
	defer span.End()

	var createdAt, updatedAt time.Time
	var returnedID, returnedPublicID string
	var version int64

	err := a.db.QueryRowContext(
		ctx,
		queries.FlowInsertReplayRun,
		run.ID,
		run.PublicID,
		run.TenantID,
		run.SourceService,
		run.ExternalID,
		run.CorrelationID,
		run.TargetProjection,
		string(run.Mode),
		string(run.State),
		string(run.Scope),
		run.StartOffset,
		run.CurrentOffset,
		run.EndOffset,
		run.ReplayConsumerGroup,
		run.Version,
	).Scan(&returnedID, &returnedPublicID, &version, &createdAt, &updatedAt)

	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("replay run creation failed: %w", err)
	}

	run.ID = returnedID
	run.PublicID = returnedPublicID
	run.Version = version
	run.CreatedAt = createdAt
	run.UpdatedAt = updatedAt
	return &run, nil
}

func (a *ReplayPostgresRepositoryAdapter) UpdateReplayRunState(ctx context.Context, tenantID, runID string, state contracts.ReplayState) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.update_state")
	defer span.End()

	var returnedID, returnedPublicID, stateStr string
	var version int64
	var updatedAt time.Time

	err := a.db.QueryRowContext(
		ctx,
		queries.FlowUpdateReplayRunState,
		string(state),
		tenantID,
		runID,
	).Scan(&returnedID, &returnedPublicID, &stateStr, &version, &updatedAt)

	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("replay state update failed: %w", err)
	}

	rec, err := a.GetReplayRun(ctx, tenantID, runID)
	if err != nil {
		return nil, err
	}
	rec.State = contracts.ReplayState(stateStr)
	rec.Version = version
	rec.UpdatedAt = updatedAt
	return rec, nil
}

func (a *ReplayPostgresRepositoryAdapter) UpdateReplayRunOffset(ctx context.Context, tenantID, runID string, currentOffset int64, state contracts.ReplayState) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.update_offset")
	defer span.End()

	var returnedID, returnedPublicID, stateStr string
	var returnedOffset, version int64
	var updatedAt time.Time

	err := a.db.QueryRowContext(
		ctx,
		queries.FlowUpdateReplayRunOffset,
		currentOffset,
		string(state),
		tenantID,
		runID,
	).Scan(&returnedID, &returnedPublicID, &returnedOffset, &stateStr, &version, &updatedAt)

	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("replay offset update failed: %w", err)
	}

	rec, err := a.GetReplayRun(ctx, tenantID, runID)
	if err != nil {
		return nil, err
	}
	rec.CurrentOffset = returnedOffset
	rec.State = contracts.ReplayState(stateStr)
	rec.Version = version
	rec.UpdatedAt = updatedAt
	return rec, nil
}

func (a *ReplayPostgresRepositoryAdapter) GetReplayRun(ctx context.Context, tenantID, runID string) (*contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.get_run")
	defer span.End()

	var rec contracts.ReplayRunRecord
	var modeStr, stateStr, scopeStr string

	err := a.db.QueryRowContext(
		ctx,
		queries.FlowGetReplayRun,
		tenantID,
		runID,
	).Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.SourceService,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.TargetProjection,
		&modeStr,
		&stateStr,
		&scopeStr,
		&rec.StartOffset,
		&rec.CurrentOffset,
		&rec.EndOffset,
		&rec.ReplayConsumerGroup,
		&rec.Version,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("replay run not found: %s", runID)
	}
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("replay get failed: %w", err)
	}

	rec.Mode = contracts.ReplayRunMode(modeStr)
	rec.State = contracts.ReplayState(stateStr)
	rec.Scope = contracts.ReplayScope(scopeStr)
	return &rec, nil
}

func (a *ReplayPostgresRepositoryAdapter) ListReplayRunsByTenant(ctx context.Context, tenantID string) ([]contracts.ReplayRunRecord, error) {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.list_runs")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowListReplayRunsByTenant, tenantID)
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("replay list failed: %w", err)
	}
	defer rows.Close()

	res := make([]contracts.ReplayRunRecord, 0)
	for rows.Next() {
		var rec contracts.ReplayRunRecord
		var modeStr, stateStr, scopeStr string
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService,
			&rec.ExternalID, &rec.CorrelationID, &rec.TargetProjection,
			&modeStr, &stateStr, &scopeStr, &rec.StartOffset,
			&rec.CurrentOffset, &rec.EndOffset, &rec.ReplayConsumerGroup,
			&rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
		); err != nil {
			observability.RecordError(span, err)
			return nil, fmt.Errorf("replay row scan failed: %w", err)
		}
		rec.Mode = contracts.ReplayRunMode(modeStr)
		rec.State = contracts.ReplayState(stateStr)
		rec.Scope = contracts.ReplayScope(scopeStr)
		res = append(res, rec)
	}
	if err := rows.Err(); err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("replay rows iteration failed: %w", err)
	}
	return res, nil
}

func (a *ReplayPostgresRepositoryAdapter) RecordEffectLog(ctx context.Context, effect contracts.ReplayEffectRecord) error {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.record_effect")
	defer span.End()

	var createdAt time.Time
	err := a.db.QueryRowContext(
		ctx,
		queries.FlowInsertReplayEffectLog,
		effect.ID,
		effect.PublicID,
		effect.TenantID,
		effect.SourceService,
		effect.ExternalID,
		effect.CorrelationID,
		effect.RunID,
		string(effect.EffectType),
		string(effect.EffectMode),
		string(effect.Decision),
		effect.IdempotencyKey,
		effect.PayloadDigest,
	).Scan(&createdAt)

	if err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("replay effect log insert failed: %w", err)
	}
	return nil
}

func (a *ReplayPostgresRepositoryAdapter) RecordAuditLog(ctx context.Context, audit contracts.ReplayAuditRecord) error {
	ctx, span := observability.StartSpan(ctx, "replay.postgres.record_audit")
	defer span.End()

	var createdAt time.Time
	err := a.db.QueryRowContext(
		ctx,
		queries.FlowInsertReplayAuditLog,
		audit.ID,
		audit.PublicID,
		audit.TenantID,
		audit.SourceService,
		audit.ExternalID,
		audit.CorrelationID,
		audit.RunID,
		string(audit.FromState),
		string(audit.ToState),
		string(audit.Event),
		audit.Reason,
	).Scan(&createdAt)

	if err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("replay audit log insert failed: %w", err)
	}
	return nil
}


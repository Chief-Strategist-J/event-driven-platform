package adapters

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/ports"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresConsistencyRepositoryAdapter struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewPostgresConsistencyRepositoryAdapter(db *sql.DB) ports.ConsistencyRepositoryPort {
	return &PostgresConsistencyRepositoryAdapter{
		db:     db,
		tracer: otel.Tracer("PostgresConsistencyRepositoryAdapter"),
	}
}

func (a *PostgresConsistencyRepositoryAdapter) UpsertWatermark(ctx context.Context, record contracts.WatermarkRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.UpsertWatermark")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpsertWatermark,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.ViewName,
		record.StreamName,
		record.PartitionID,
		record.HighWatermarkOffset,
		record.ConsumerOffset,
		record.OffsetLag,
		record.FreshnessLagMs,
		record.WatermarkStatus,
		record.ObservedAt,
		record.Version,
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresConsistencyRepositoryAdapter) GetWatermarksByView(ctx context.Context, tenantID string, viewName string) ([]contracts.WatermarkRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.GetWatermarksByView")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowSelectWatermarksByView, tenantID, viewName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var records []contracts.WatermarkRecord
	for rows.Next() {
		var r contracts.WatermarkRecord
		if err := rows.Scan(
			&r.ID,
			&r.PublicID,
			&r.TenantID,
			&r.SourceService,
			&r.ExternalID,
			&r.CorrelationID,
			&r.ViewName,
			&r.StreamName,
			&r.PartitionID,
			&r.HighWatermarkOffset,
			&r.ConsumerOffset,
			&r.OffsetLag,
			&r.FreshnessLagMs,
			&r.WatermarkStatus,
			&r.ObservedAt,
			&r.Version,
			&r.CreatedAt,
			&r.UpdatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		records = append(records, r)
	}
	return records, nil
}

func (a *PostgresConsistencyRepositoryAdapter) RecordDriftIncident(ctx context.Context, record contracts.DriftIncidentRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.RecordDriftIncident")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertDriftIncident,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.ViewName,
		record.AggregateType,
		record.AggregateID,
		string(record.DriftClass),
		record.SourceVersion,
		record.ViewVersion,
		record.DifferencePayload,
		string(record.Status),
		record.HealAttempts,
		record.QuarantinedUntil,
		record.DetectedAt,
		record.ResolvedAt,
		record.Version,
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresConsistencyRepositoryAdapter) GetActiveDriftIncidents(ctx context.Context, tenantID string, viewName string) ([]contracts.DriftIncidentRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.GetActiveDriftIncidents")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowSelectActiveDriftIncidents, tenantID, viewName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var records []contracts.DriftIncidentRecord
	for rows.Next() {
		var r contracts.DriftIncidentRecord
		var driftClass string
		var status string
		if err := rows.Scan(
			&r.ID,
			&r.PublicID,
			&r.TenantID,
			&r.SourceService,
			&r.ExternalID,
			&r.CorrelationID,
			&r.ViewName,
			&r.AggregateType,
			&r.AggregateID,
			&driftClass,
			&r.SourceVersion,
			&r.ViewVersion,
			&r.DifferencePayload,
			&status,
			&r.HealAttempts,
			&r.QuarantinedUntil,
			&r.DetectedAt,
			&r.ResolvedAt,
			&r.Version,
			&r.CreatedAt,
			&r.UpdatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		r.DriftClass = contracts.DriftClass(driftClass)
		r.Status = contracts.IncidentStatus(status)
		records = append(records, r)
	}
	return records, nil
}

func (a *PostgresConsistencyRepositoryAdapter) UpdateDriftIncidentStatus(ctx context.Context, id uuid.UUID, tenantID string, status contracts.IncidentStatus, healAttempts int) error {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.UpdateDriftIncidentStatus")
	defer span.End()

	res, err := a.db.ExecContext(ctx, queries.FlowUpdateDriftIncidentStatus, id, tenantID, string(status), healAttempts)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ports.ErrIncidentNotFound
	}
	return nil
}

func (a *PostgresConsistencyRepositoryAdapter) GetDriftIncident(ctx context.Context, id uuid.UUID, tenantID string) (*contracts.DriftIncidentRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.GetDriftIncident")
	defer span.End()

	var r contracts.DriftIncidentRecord
	var driftClass, status string
	err := a.db.QueryRowContext(ctx, queries.FlowSelectDriftIncidentByID, id, tenantID).Scan(
		&r.ID, &r.PublicID, &r.TenantID, &r.SourceService, &r.ExternalID, &r.CorrelationID,
		&r.ViewName, &r.AggregateType, &r.AggregateID, &driftClass, &r.SourceVersion,
		&r.ViewVersion, &r.DifferencePayload, &status, &r.HealAttempts,
		&r.QuarantinedUntil, &r.DetectedAt, &r.ResolvedAt, &r.Version, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ports.ErrIncidentNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	r.DriftClass = contracts.DriftClass(driftClass)
	r.Status = contracts.IncidentStatus(status)
	return &r, nil
}

func (a *PostgresConsistencyRepositoryAdapter) RecordCheckRun(ctx context.Context, record contracts.CheckRunRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.RecordCheckRun")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertCheckRun,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.ViewName,
		string(record.CheckType),
		record.EvaluatedKeys,
		record.MatchCount,
		record.MismatchCount,
		record.UnknownCount,
		record.CoverageRatio,
		record.ExecutionDurationMs,
		record.Status,
		record.CreatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresConsistencyRepositoryAdapter) ListCheckRuns(ctx context.Context, tenantID string, viewName string, limit int) ([]contracts.CheckRunRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.ListCheckRuns")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowSelectCheckRuns, tenantID, viewName, limit)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var records []contracts.CheckRunRecord
	for rows.Next() {
		var r contracts.CheckRunRecord
		var checkType string
		if err := rows.Scan(
			&r.ID,
			&r.PublicID,
			&r.TenantID,
			&r.SourceService,
			&r.ExternalID,
			&r.CorrelationID,
			&r.ViewName,
			&checkType,
			&r.EvaluatedKeys,
			&r.MatchCount,
			&r.MismatchCount,
			&r.UnknownCount,
			&r.CoverageRatio,
			&r.ExecutionDurationMs,
			&r.Status,
			&r.CreatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		r.CheckType = contracts.CheckType(checkType)
		records = append(records, r)
	}
	return records, nil
}

func (a *PostgresConsistencyRepositoryAdapter) RegisterSilence(ctx context.Context, record contracts.SilenceRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.RegisterSilence")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertSilence,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.ViewName,
		record.OwnerID,
		record.Reason,
		record.ExpiresAt,
		string(record.Status),
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresConsistencyRepositoryAdapter) GetActiveSilence(ctx context.Context, tenantID string, viewName string) (*contracts.SilenceRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresConsistencyRepositoryAdapter.GetActiveSilence")
	defer span.End()

	var r contracts.SilenceRecord
	var status string
	err := a.db.QueryRowContext(ctx, queries.FlowSelectActiveSilence, tenantID, viewName).Scan(
		&r.ID,
		&r.PublicID,
		&r.TenantID,
		&r.SourceService,
		&r.ExternalID,
		&r.CorrelationID,
		&r.ViewName,
		&r.OwnerID,
		&r.Reason,
		&r.ExpiresAt,
		&status,
		&r.CreatedAt,
		&r.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	r.Status = contracts.SilenceStatus(status)
	return &r, nil
}


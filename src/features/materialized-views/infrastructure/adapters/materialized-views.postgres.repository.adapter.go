package adapters

import (
	"context"
	"database/sql"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/infrastructure/ports"
	"event-driven-platform/src/features/materialized-views/infrastructure/queries"
	"event-driven-platform/src/infra/database"
	"event-driven-platform/src/shared/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type PostgresViewRepositoryAdapter struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewPostgresViewRepositoryAdapter(db *sql.DB) ports.ViewRepositoryPort {
	return &PostgresViewRepositoryAdapter{
		db:     db,
		tracer: otel.Tracer("PostgresViewRepositoryAdapter"),
	}
}

func (a *PostgresViewRepositoryAdapter) RegisterView(ctx context.Context, reg contracts.ViewRegistryRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.RegisterView")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertViewRegistry,
		reg.ID,
		reg.PublicID,
		reg.TenantID,
		reg.SourceService,
		reg.ExternalID,
		reg.CorrelationID,
		reg.ViewName,
		string(reg.ViewClass),
		string(reg.PrimaryStore),
		reg.ActiveTableName,
		reg.FreshnessSLOMs,
		string(reg.ConsistencyMode),
		string(reg.Status),
		reg.Version,
		reg.CreatedAt,
		reg.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) GetViewByName(ctx context.Context, tenantID string, viewName string) (*contracts.ViewRegistryRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.GetViewByName")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.FlowSelectViewRegistryByTenantAndViewName, tenantID, viewName)
	var reg contracts.ViewRegistryRecord
	var viewClass, primaryStore, consistencyMode, status string

	err := row.Scan(
		&reg.ID,
		&reg.PublicID,
		&reg.TenantID,
		&reg.SourceService,
		&reg.ExternalID,
		&reg.CorrelationID,
		&reg.ViewName,
		&viewClass,
		&primaryStore,
		&reg.ActiveTableName,
		&reg.FreshnessSLOMs,
		&consistencyMode,
		&status,
		&reg.Version,
		&reg.CreatedAt,
		&reg.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("materialized view registry record not found")
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}

	reg.ViewClass = contracts.ViewClass(viewClass)
	reg.PrimaryStore = contracts.StorageTarget(primaryStore)
	reg.ConsistencyMode = contracts.ConsistencyMode(consistencyMode)
	reg.Status = contracts.ViewStatus(status)

	return &reg, nil
}

func (a *PostgresViewRepositoryAdapter) GetViewByPublicID(ctx context.Context, publicID string) (*contracts.ViewRegistryRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.GetViewByPublicID")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.FlowSelectViewRegistryByPublicID, publicID)
	var reg contracts.ViewRegistryRecord
	var viewClass, primaryStore, consistencyMode, status string

	err := row.Scan(
		&reg.ID,
		&reg.PublicID,
		&reg.TenantID,
		&reg.SourceService,
		&reg.ExternalID,
		&reg.CorrelationID,
		&reg.ViewName,
		&viewClass,
		&primaryStore,
		&reg.ActiveTableName,
		&reg.FreshnessSLOMs,
		&consistencyMode,
		&status,
		&reg.Version,
		&reg.CreatedAt,
		&reg.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("materialized view record not found")
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}

	reg.ViewClass = contracts.ViewClass(viewClass)
	reg.PrimaryStore = contracts.StorageTarget(primaryStore)
	reg.ConsistencyMode = contracts.ConsistencyMode(consistencyMode)
	reg.Status = contracts.ViewStatus(status)

	return &reg, nil
}

func (a *PostgresViewRepositoryAdapter) UpdateViewStatus(ctx context.Context, tenantID string, viewName string, status contracts.ViewStatus) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.UpdateViewStatus")
	defer span.End()

	res, err := a.db.ExecContext(ctx, queries.FlowUpdateViewRegistryStatus, tenantID, viewName, string(status))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return database.MapDatabaseError(err)
	}
	if rows == 0 {
		return errors.NewNotFoundError("materialized view not found")
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) GetProjectionState(
	ctx context.Context,
	tenantID string,
	viewName string,
	aggType string,
	aggID string,
) (*contracts.ProjectionStateRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.GetProjectionState")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.FlowSelectProjectionStateByAggregate, tenantID, viewName, aggType, aggID)
	var state contracts.ProjectionStateRecord

	err := row.Scan(
		&state.ID,
		&state.PublicID,
		&state.TenantID,
		&state.SourceService,
		&state.ExternalID,
		&state.CorrelationID,
		&state.ViewName,
		&state.AggregateType,
		&state.AggregateID,
		&state.CurrentVersion,
		&state.LastEventID,
		&state.PayloadHash,
		&state.MaterializedAt,
		&state.Version,
		&state.CreatedAt,
		&state.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("projection state not found")
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}

	return &state, nil
}

func (a *PostgresViewRepositoryAdapter) UpsertProjectionState(ctx context.Context, state contracts.ProjectionStateRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.UpsertProjectionState")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpsertProjectionState,
		state.ID,
		state.PublicID,
		state.TenantID,
		state.SourceService,
		state.ExternalID,
		state.CorrelationID,
		state.ViewName,
		state.AggregateType,
		state.AggregateID,
		state.CurrentVersion,
		state.LastEventID,
		state.PayloadHash,
		state.MaterializedAt,
		state.Version,
		state.CreatedAt,
		state.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) QuarantineHeldEvent(ctx context.Context, event contracts.HeldEventRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.QuarantineHeldEvent")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertHeldEvent,
		event.ID,
		event.PublicID,
		event.TenantID,
		event.SourceService,
		event.ExternalID,
		event.CorrelationID,
		event.ViewName,
		event.AggregateType,
		event.AggregateID,
		event.ExpectedSeq,
		event.ReceivedSeq,
		event.EventPayload,
		event.HeldUntil,
		event.CreatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) ListHeldEvents(
	ctx context.Context,
	tenantID string,
	viewName string,
	aggType string,
	aggID string,
) ([]contracts.HeldEventRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.ListHeldEvents")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowSelectHeldEventsByAggregate, tenantID, viewName, aggType, aggID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var events []contracts.HeldEventRecord
	for rows.Next() {
		var e contracts.HeldEventRecord
		if err := rows.Scan(
			&e.ID,
			&e.PublicID,
			&e.TenantID,
			&e.SourceService,
			&e.ExternalID,
			&e.CorrelationID,
			&e.ViewName,
			&e.AggregateType,
			&e.AggregateID,
			&e.ExpectedSeq,
			&e.ReceivedSeq,
			&e.EventPayload,
			&e.HeldUntil,
			&e.CreatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, database.MapDatabaseError(err)
	}

	return events, nil
}

func (a *PostgresViewRepositoryAdapter) DeleteHeldEvent(ctx context.Context, tenantID string, eventID string) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.DeleteHeldEvent")
	defer span.End()

	parsedID, err := uuid.Parse(eventID)
	if err != nil {
		return errors.NewValidationError("invalid event_id UUID")
	}

	_, err = a.db.ExecContext(ctx, queries.FlowDeleteHeldEventByID, tenantID, parsedID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) InitiateRebuild(ctx context.Context, log contracts.RebuildLogRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.InitiateRebuild")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertRebuildLog,
		log.ID,
		log.PublicID,
		log.TenantID,
		log.SourceService,
		log.ExternalID,
		log.CorrelationID,
		log.RebuildID,
		log.ViewName,
		string(log.SourceStore),
		log.TargetShadowTable,
		log.TargetLSN,
		log.CurrentCursorSeq,
		string(log.Phase),
		log.Version,
		log.CreatedAt,
		log.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) GetRebuildLog(ctx context.Context, tenantID string, rebuildID string) (*contracts.RebuildLogRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.GetRebuildLog")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.FlowSelectRebuildLogByRebuildID, tenantID, rebuildID)
	var log contracts.RebuildLogRecord
	var sourceStore, phase string

	err := row.Scan(
		&log.ID,
		&log.PublicID,
		&log.TenantID,
		&log.SourceService,
		&log.ExternalID,
		&log.CorrelationID,
		&log.RebuildID,
		&log.ViewName,
		&sourceStore,
		&log.TargetShadowTable,
		&log.TargetLSN,
		&log.CurrentCursorSeq,
		&phase,
		&log.Version,
		&log.CreatedAt,
		&log.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("rebuild log record not found")
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}

	log.SourceStore = contracts.StorageTarget(sourceStore)
	log.Phase = contracts.RebuildPhase(phase)

	return &log, nil
}

func (a *PostgresViewRepositoryAdapter) UpdateRebuildPhase(
	ctx context.Context,
	tenantID string,
	rebuildID string,
	phase contracts.RebuildPhase,
	cursor int64,
) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.UpdateRebuildPhase")
	defer span.End()

	res, err := a.db.ExecContext(ctx, queries.FlowUpdateRebuildLogPhaseAndCursor, tenantID, rebuildID, string(phase), cursor)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return database.MapDatabaseError(err)
	}
	if rows == 0 {
		return errors.NewNotFoundError("rebuild log not found")
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) RecordContribution(ctx context.Context, contrib contracts.AggContributionRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.RecordContribution")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertAggContribution,
		contrib.ID,
		contrib.PublicID,
		contrib.TenantID,
		contrib.SourceService,
		contrib.ExternalID,
		contrib.CorrelationID,
		contrib.ViewName,
		contrib.AggregateType,
		contrib.AggregateID,
		contrib.SourceEventID,
		contrib.SourceSeq,
		contrib.DeltaAmount,
		contrib.IsReversal,
		contrib.Version,
		contrib.CreatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) GetAggregateContributionTotal(
	ctx context.Context,
	tenantID string,
	viewName string,
	aggType string,
	aggID string,
) (float64, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.GetAggregateContributionTotal")
	defer span.End()

	var total float64
	row := a.db.QueryRowContext(ctx, queries.FlowSumAggContributionsByAggregate, tenantID, viewName, aggType, aggID)
	if err := row.Scan(&total); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, database.MapDatabaseError(err)
	}

	return total, nil
}

func (a *PostgresViewRepositoryAdapter) UpsertPeriodLedger(ctx context.Context, period contracts.PeriodLedgerRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.UpsertPeriodLedger")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpsertPeriodLedger,
		period.ID,
		period.PublicID,
		period.TenantID,
		period.SourceService,
		period.ExternalID,
		period.CorrelationID,
		period.ViewName,
		period.PeriodKey,
		string(period.PeriodStatus),
		period.BaseAmount,
		period.AdjustmentAmount,
		period.ClosingPositionLSN,
		period.Version,
		period.CreatedAt,
		period.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	return nil
}

func (a *PostgresViewRepositoryAdapter) GetPeriodLedger(
	ctx context.Context,
	tenantID string,
	viewName string,
	periodKey string,
) (*contracts.PeriodLedgerRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.GetPeriodLedger")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.FlowSelectPeriodLedgerByPeriodKey, tenantID, viewName, periodKey)
	var p contracts.PeriodLedgerRecord
	var status string

	err := row.Scan(
		&p.ID,
		&p.PublicID,
		&p.TenantID,
		&p.SourceService,
		&p.ExternalID,
		&p.CorrelationID,
		&p.ViewName,
		&p.PeriodKey,
		&status,
		&p.BaseAmount,
		&p.AdjustmentAmount,
		&p.ClosingPositionLSN,
		&p.Version,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError("period ledger record not found")
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}

	p.PeriodStatus = contracts.PeriodStatus(status)
	return &p, nil
}

func (a *PostgresViewRepositoryAdapter) ApplyPeriodAdjustment(
	ctx context.Context,
	tenantID string,
	viewName string,
	periodKey string,
	adjustment float64,
) error {
	ctx, span := a.tracer.Start(ctx, "PostgresViewRepositoryAdapter.ApplyPeriodAdjustment")
	defer span.End()

	res, err := a.db.ExecContext(ctx, queries.FlowUpdatePeriodLedgerAdjustment, tenantID, viewName, periodKey, adjustment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return database.MapDatabaseError(err)
	}
	if rows == 0 {
		return errors.NewNotFoundError("period ledger record not found")
	}

	return nil
}

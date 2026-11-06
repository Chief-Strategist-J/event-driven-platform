package adapters

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/schema-evolution/contracts"
	"event-driven-platform/src/features/schema-evolution/infrastructure/ports"
	"event-driven-platform/src/features/schema-evolution/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresSchemaEvolutionRepositoryAdapter struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewPostgresSchemaEvolutionRepositoryAdapter(db *sql.DB) ports.SchemaEvolutionRepositoryPort {
	return &PostgresSchemaEvolutionRepositoryAdapter{
		db:     db,
		tracer: otel.Tracer("PostgresSchemaEvolutionRepositoryAdapter"),
	}
}

func (a *PostgresSchemaEvolutionRepositoryAdapter) RegisterSchemaVersion(ctx context.Context, record contracts.SchemaVersionRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.RegisterSchemaVersion")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertSchemaVersion,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.SubjectName,
		record.SchemaVersion,
		string(record.CompatibilityMode),
		record.SchemaFingerprint,
		record.SchemaDefinition,
		string(record.Status),
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

func (a *PostgresSchemaEvolutionRepositoryAdapter) GetSchemaLineage(ctx context.Context, tenantID string, subjectName string) ([]contracts.SchemaVersionRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.GetSchemaLineage")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowSelectSchemaLineage, tenantID, subjectName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var records []contracts.SchemaVersionRecord
	for rows.Next() {
		var r contracts.SchemaVersionRecord
		var mode string
		var status string
		if err := rows.Scan(
			&r.ID,
			&r.PublicID,
			&r.TenantID,
			&r.SourceService,
			&r.ExternalID,
			&r.CorrelationID,
			&r.SubjectName,
			&r.SchemaVersion,
			&mode,
			&r.SchemaFingerprint,
			&r.SchemaDefinition,
			&status,
			&r.Version,
			&r.CreatedAt,
			&r.UpdatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		r.CompatibilityMode = contracts.CompatibilityMode(mode)
		r.Status = contracts.EvolutionStatus(status)
		records = append(records, r)
	}
	return records, nil
}

func (a *PostgresSchemaEvolutionRepositoryAdapter) GetActiveConsumersForSubject(ctx context.Context, tenantID string, subjectName string) ([]contracts.ConsumerSupportRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.GetActiveConsumersForSubject")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.FlowSelectConsumersForSubject, tenantID, subjectName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var records []contracts.ConsumerSupportRecord
	for rows.Next() {
		var r contracts.ConsumerSupportRecord
		if err := rows.Scan(
			&r.ID,
			&r.PublicID,
			&r.TenantID,
			&r.SourceService,
			&r.ExternalID,
			&r.CorrelationID,
			&r.ConsumerGroup,
			&r.SubjectName,
			&r.MinSupportedVersion,
			&r.MaxSupportedVersion,
			&r.RunCadence,
			&r.Status,
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

func (a *PostgresSchemaEvolutionRepositoryAdapter) RegisterConsumerSupport(ctx context.Context, record contracts.ConsumerSupportRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.RegisterConsumerSupport")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpsertConsumerSupport,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.ConsumerGroup,
		record.SubjectName,
		record.MinSupportedVersion,
		record.MaxSupportedVersion,
		record.RunCadence,
		record.Status,
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

func (a *PostgresSchemaEvolutionRepositoryAdapter) CreateDualPublishEntry(ctx context.Context, record contracts.DualPublishRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.CreateDualPublishEntry")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertDualPublish,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.SubjectName,
		record.LegacyTopic,
		record.ModernTopic,
		record.SunsetDeadline,
		record.Status,
		record.DecommissionAttested,
		record.ZeroLagDays,
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

func (a *PostgresSchemaEvolutionRepositoryAdapter) GetDualPublishEntry(ctx context.Context, tenantID string, subjectName string) (*contracts.DualPublishRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.GetDualPublishEntry")
	defer span.End()

	var r contracts.DualPublishRecord
	err := a.db.QueryRowContext(ctx, queries.FlowSelectDualPublish, tenantID, subjectName).Scan(
		&r.ID,
		&r.PublicID,
		&r.TenantID,
		&r.SourceService,
		&r.ExternalID,
		&r.CorrelationID,
		&r.SubjectName,
		&r.LegacyTopic,
		&r.ModernTopic,
		&r.SunsetDeadline,
		&r.Status,
		&r.DecommissionAttested,
		&r.ZeroLagDays,
		&r.Version,
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
	return &r, nil
}

func (a *PostgresSchemaEvolutionRepositoryAdapter) UpdateDualPublishStatus(
	ctx context.Context,
	id uuid.UUID,
	tenantID string,
	status string,
	attested bool,
	zeroLagDays int,
) error {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.UpdateDualPublishStatus")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpdateDualPublish, id, tenantID, status, attested, zeroLagDays)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresSchemaEvolutionRepositoryAdapter) RecordIncident(ctx context.Context, record contracts.EvolutionIncidentRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresSchemaEvolutionRepositoryAdapter.RecordIncident")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertEvolutionIncident,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.SubjectName,
		record.ConsumerGroup,
		string(record.IncidentClass),
		record.PayloadFingerprint,
		record.ErrorMessage,
		record.Status,
		record.OccurredAt,
		record.CreatedAt,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}


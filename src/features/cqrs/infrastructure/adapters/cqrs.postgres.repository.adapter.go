package adapters

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/features/cqrs/infrastructure/ports"
	"event-driven-platform/src/features/cqrs/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresCQRSRepositoryAdapter struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewPostgresCQRSRepositoryAdapter(db *sql.DB) ports.CQRSRepositoryPort {
	return &PostgresCQRSRepositoryAdapter{
		db:     db,
		tracer: otel.Tracer("PostgresCQRSRepositoryAdapter"),
	}
}

func (a *PostgresCQRSRepositoryAdapter) GetIdempotencyRecord(ctx context.Context, tenantID string, idempotencyKey string) (*contracts.CommandRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.GetIdempotencyRecord")
	defer span.End()

	var r contracts.CommandRecord
	var status string
	err := a.db.QueryRowContext(ctx, queries.FlowSelectIdempotencyRecord, tenantID, idempotencyKey).Scan(
		&r.ID,
		&r.PublicID,
		&r.TenantID,
		&r.SourceService,
		&r.ExternalID,
		&r.CorrelationID,
		&r.IdempotencyKey,
		&r.CommandName,
		&r.AggregateType,
		&r.AggregateID,
		&r.RequestFingerprint,
		&status,
		&r.ResponsePayload,
		&r.ConsistencyToken,
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
	r.Status = contracts.CommandStatus(status)
	return &r, nil
}

func (a *PostgresCQRSRepositoryAdapter) CreateIdempotencyRecord(ctx context.Context, record contracts.CommandRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.CreateIdempotencyRecord")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertIdempotencyRecord,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.IdempotencyKey,
		record.CommandName,
		record.AggregateType,
		record.AggregateID,
		record.RequestFingerprint,
		string(record.Status),
		record.ResponsePayload,
		record.ConsistencyToken,
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

func (a *PostgresCQRSRepositoryAdapter) UpdateIdempotencyRecord(
	ctx context.Context,
	id uuid.UUID,
	tenantID string,
	status contracts.CommandStatus,
	responsePayload string,
	token string,
) error {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.UpdateIdempotencyRecord")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpdateIdempotencyRecord, id, tenantID, string(status), responsePayload, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresCQRSRepositoryAdapter) GetAggregateForUpdate(
	ctx context.Context,
	tenantID string,
	aggregateType string,
	aggregateID string,
) (*contracts.AggregateRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.GetAggregateForUpdate")
	defer span.End()

	var r contracts.AggregateRecord
	err := a.db.QueryRowContext(ctx, queries.FlowSelectAggregateForUpdate, tenantID, aggregateType, aggregateID).Scan(
		&r.ID,
		&r.PublicID,
		&r.TenantID,
		&r.SourceService,
		&r.ExternalID,
		&r.CorrelationID,
		&r.AggregateType,
		&r.AggregateID,
		&r.CurrentVersion,
		&r.StatePayload,
		&r.IsDeleted,
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

func (a *PostgresCQRSRepositoryAdapter) UpsertAggregateMutation(ctx context.Context, record contracts.AggregateRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.UpsertAggregateMutation")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowUpsertAggregateMutation,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.AggregateType,
		record.AggregateID,
		record.CurrentVersion,
		record.StatePayload,
		record.IsDeleted,
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

func (a *PostgresCQRSRepositoryAdapter) GetActiveSigningKey(ctx context.Context, tenantID string) (*contracts.TokenSigningKeyRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.GetActiveSigningKey")
	defer span.End()

	var r contracts.TokenSigningKeyRecord
	err := a.db.QueryRowContext(ctx, queries.FlowSelectActiveSigningKey, tenantID).Scan(
		&r.ID,
		&r.PublicID,
		&r.TenantID,
		&r.SourceService,
		&r.ExternalID,
		&r.CorrelationID,
		&r.KeyVersion,
		&r.SecretKeyHash,
		&r.RawSecretValue,
		&r.Status,
		&r.ExpiresAt,
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

func (a *PostgresCQRSRepositoryAdapter) RegisterSigningKey(ctx context.Context, record contracts.TokenSigningKeyRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresCQRSRepositoryAdapter.RegisterSigningKey")
	defer span.End()

	_, err := a.db.ExecContext(ctx, queries.FlowInsertSigningKey,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.KeyVersion,
		record.SecretKeyHash,
		record.RawSecretValue,
		record.Status,
		record.ExpiresAt,
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


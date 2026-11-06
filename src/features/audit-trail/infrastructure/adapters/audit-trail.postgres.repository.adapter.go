package adapters

import (
	"context"
	"database/sql"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/audit-trail/contracts"
	"event-driven-platform/src/features/audit-trail/infrastructure/ports"
	"event-driven-platform/src/features/audit-trail/infrastructure/queries"
)

type PostgresAuditTrailRepositoryAdapter struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewPostgresAuditTrailRepositoryAdapter(db *sql.DB) ports.AuditTrailRepositoryPort {
	return &PostgresAuditTrailRepositoryAdapter{
		db:     db,
		tracer: otel.Tracer("PostgresAuditTrailRepositoryAdapter"),
	}
}

func (a *PostgresAuditTrailRepositoryAdapter) GetLatestRecord(ctx context.Context, tenantID string, shardID string) (*contracts.AuditRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.GetLatestAuditRecord")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.GetLatestRecordQuery, tenantID, shardID)
	var rec contracts.AuditRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&rec.ShardID, &rec.SequenceNumber, &rec.ActorID, &rec.ActorType, &rec.ActionName,
		&rec.TargetType, &rec.TargetID, &rec.Outcome, &rec.ComplianceClass, &rec.PrevRecordHash,
		&rec.RecordHash, &rec.CanonicalPayload, &rec.OccurredAt, &rec.IngestedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &rec, nil
}

func (a *PostgresAuditTrailRepositoryAdapter) AppendAuditRecord(ctx context.Context, record contracts.AuditRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.InsertAuditRecord")
	defer span.End()

	_, err := a.db.ExecContext(
		ctx,
		queries.InsertAuditRecordQuery,
		record.ID, record.PublicID, record.TenantID, record.SourceService, record.ExternalID, record.CorrelationID,
		record.ShardID, record.SequenceNumber, record.ActorID, record.ActorType, record.ActionName,
		record.TargetType, record.TargetID, record.Outcome, record.ComplianceClass, record.PrevRecordHash,
		record.RecordHash, record.CanonicalPayload, record.OccurredAt, record.IngestedAt,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (a *PostgresAuditTrailRepositoryAdapter) GetShardRecordsRange(ctx context.Context, in contracts.VerifyShardIntegrityInput, startSeq int64, endSeq int64) ([]contracts.AuditRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.GetShardRecordsRange")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.GetShardRecordsRangeQuery, in.TenantID, in.ShardID, startSeq, endSeq)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	var records []contracts.AuditRecord
	for rows.Next() {
		var rec contracts.AuditRecord
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.ShardID, &rec.SequenceNumber, &rec.ActorID, &rec.ActorType, &rec.ActionName,
			&rec.TargetType, &rec.TargetID, &rec.Outcome, &rec.ComplianceClass, &rec.PrevRecordHash,
			&rec.RecordHash, &rec.CanonicalPayload, &rec.OccurredAt, &rec.IngestedAt,
		); err != nil {
			span.RecordError(err)
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

func (a *PostgresAuditTrailRepositoryAdapter) GetLatestCheckpoint(ctx context.Context, tenantID string, shardID string) (*contracts.CheckpointRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.GetLatestCheckpoint")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.GetLatestCheckpointQuery, tenantID, shardID)
	var cp contracts.CheckpointRecord
	err := row.Scan(
		&cp.ID, &cp.PublicID, &cp.TenantID, &cp.SourceService, &cp.ExternalID, &cp.CorrelationID,
		&cp.ShardID, &cp.CheckpointSeq, &cp.StartSequence, &cp.EndSequence, &cp.RecordCount,
		&cp.MerkleRootHash, &cp.HeadRecordHash, &cp.AnchorSignature, &cp.Status, &cp.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &cp, nil
}

func (a *PostgresAuditTrailRepositoryAdapter) AppendCheckpoint(ctx context.Context, cp contracts.CheckpointRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.AppendCheckpoint")
	defer span.End()

	_, err := a.db.ExecContext(
		ctx,
		queries.InsertCheckpointQuery,
		cp.ID, cp.PublicID, cp.TenantID, cp.SourceService, cp.ExternalID, cp.CorrelationID,
		cp.ShardID, cp.CheckpointSeq, cp.StartSequence, cp.EndSequence, cp.RecordCount,
		cp.MerkleRootHash, cp.HeadRecordHash, cp.AnchorSignature, cp.Status, cp.CreatedAt,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (a *PostgresAuditTrailRepositoryAdapter) RecordAccessLog(ctx context.Context, log contracts.AccessLogRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.RecordAccessLog")
	defer span.End()

	_, err := a.db.ExecContext(
		ctx,
		queries.InsertAccessLogQuery,
		log.ID, log.PublicID, log.TenantID, log.SourceService, log.ExternalID, log.CorrelationID,
		log.AuditorID, log.CaseID, log.QueryScope, log.Reason, log.AccessedAt,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (a *PostgresAuditTrailRepositoryAdapter) QueryAuditRecords(ctx context.Context, in contracts.QueryAuditInput) ([]contracts.AuditRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresAuditTrailRepositoryAdapter.QueryAuditRecords")
	defer span.End()

	rows, err := a.db.QueryContext(ctx, queries.QueryAuditRecordsBase, in.TenantID, in.ShardID, in.Limit)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	var records []contracts.AuditRecord
	for rows.Next() {
		var rec contracts.AuditRecord
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.ShardID, &rec.SequenceNumber, &rec.ActorID, &rec.ActorType, &rec.ActionName,
			&rec.TargetType, &rec.TargetID, &rec.Outcome, &rec.ComplianceClass, &rec.PrevRecordHash,
			&rec.RecordHash, &rec.CanonicalPayload, &rec.OccurredAt, &rec.IngestedAt,
		); err != nil {
			span.RecordError(err)
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

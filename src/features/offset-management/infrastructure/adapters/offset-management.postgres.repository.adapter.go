package adapters

import (
	"context"
	"database/sql"
	"errors"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/infrastructure/queries"
	sharedErrors "event-driven-platform/src/shared/errors"
)

type PostgresOffsetManagementAdapter struct {
	db *sql.DB
}

func NewPostgresOffsetManagementAdapter(db *sql.DB) *PostgresOffsetManagementAdapter {
	return &PostgresOffsetManagementAdapter{db: db}
}

func (a *PostgresOffsetManagementAdapter) UpsertStoredOffset(ctx context.Context, record contracts.ConsumerStoredOffsetRecord) (*contracts.ConsumerStoredOffsetRecord, error) {
	metadata := record.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	row := a.db.QueryRowContext(ctx, queries.FlowUpsertStoredOffset,
		record.ID, record.PublicID, record.TenantID, record.SourceService, record.ExternalID, record.CorrelationID,
		record.GroupID, record.Topic, record.Partition, record.StoredOffset, record.LogEndOffset, record.Lag,
		record.CommitEpoch, metadata,
	)

	var updated contracts.ConsumerStoredOffsetRecord
	updated = record
	err := row.Scan(&updated.ID, &updated.PublicID, &updated.CommitEpoch, &updated.CreatedAt, &updated.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

func (a *PostgresOffsetManagementAdapter) GetStoredOffset(ctx context.Context, tenantID, groupID, topic string, partition int32) (*contracts.ConsumerStoredOffsetRecord, error) {
	row := a.db.QueryRowContext(ctx, queries.FlowGetStoredOffset, tenantID, groupID, topic, partition)

	var rec contracts.ConsumerStoredOffsetRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&rec.GroupID, &rec.Topic, &rec.Partition, &rec.StoredOffset, &rec.LogEndOffset, &rec.Lag,
		&rec.CommitEpoch, &rec.Metadata, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sharedErrors.NewNotFoundError("stored offset not found for partition")
		}
		return nil, err
	}
	return &rec, nil
}

func (a *PostgresOffsetManagementAdapter) ListStoredOffsets(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerStoredOffsetRecord, error) {
	rows, err := a.db.QueryContext(ctx, queries.FlowListStoredOffsetsByGroup, tenantID, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []contracts.ConsumerStoredOffsetRecord
	for rows.Next() {
		var rec contracts.ConsumerStoredOffsetRecord
		scanErr := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.GroupID, &rec.Topic, &rec.Partition, &rec.StoredOffset, &rec.LogEndOffset, &rec.Lag,
			&rec.CommitEpoch, &rec.Metadata, &rec.CreatedAt, &rec.UpdatedAt,
		)
		if scanErr != nil {
			return nil, scanErr
		}
		list = append(list, rec)
	}
	return list, rows.Err()
}

func (a *PostgresOffsetManagementAdapter) RecordAuditLog(ctx context.Context, log contracts.ConsumerOffsetAuditLogRecord) error {
	_, err := a.db.ExecContext(ctx, queries.FlowInsertOffsetAuditLog,
		log.ID, log.PublicID, log.TenantID, log.SourceService, log.ExternalID, log.CorrelationID,
		log.GroupID, log.Topic, log.Partition, string(log.OperationType), log.PreviousOffset,
		log.TargetOffset, log.Reason,
	)
	return err
}

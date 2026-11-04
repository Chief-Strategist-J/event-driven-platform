package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/ports"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/queries"
	infraDb "event-driven-platform/src/infra/database"
)

type PostgresDeadLetterRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresDeadLetterRepositoryAdapter(db *sql.DB) ports.DeadLetterRepositoryPort {
	return &PostgresDeadLetterRepositoryAdapter{db: db}
}

func (p *PostgresDeadLetterRepositoryAdapter) Quarantine(ctx context.Context, record contracts.DeadLetterQuarantineRecord) error {
	headersJSON, err := json.Marshal(record.Headers)
	if err != nil {
		headersJSON = []byte("{}")
	}

	_, err = p.db.ExecContext(
		ctx,
		queries.FlowInsertDeadLetterQuarantine,
		record.ID,
		record.PublicID,
		record.TenantID,
		record.SourceService,
		record.ExternalID,
		record.CorrelationID,
		record.Topic,
		record.Partition,
		record.SourceOffset,
		record.ConsumerGroup,
		record.HandlerID,
		record.EventID,
		record.AggregateType,
		record.AggregateID,
		record.AggregateVersion,
		record.EventType,
		record.Payload,
		headersJSON,
		record.FailureReason,
		record.ErrorCode,
		record.ErrorFingerprint,
		record.Status,
		record.RedriveCount,
		record.MaxRedriveAttempts,
	)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) GetQuarantinedByID(ctx context.Context, tenantID, id string) (*contracts.DeadLetterQuarantineRecord, error) {
	var rec contracts.DeadLetterQuarantineRecord
	var headersRaw []byte

	err := p.db.QueryRowContext(ctx, queries.FlowGetQuarantinedByID, tenantID, id).Scan(
		&rec.ID,
		&rec.PublicID,
		&rec.TenantID,
		&rec.SourceService,
		&rec.ExternalID,
		&rec.CorrelationID,
		&rec.Topic,
		&rec.Partition,
		&rec.SourceOffset,
		&rec.ConsumerGroup,
		&rec.HandlerID,
		&rec.EventID,
		&rec.AggregateType,
		&rec.AggregateID,
		&rec.AggregateVersion,
		&rec.EventType,
		&rec.Payload,
		&headersRaw,
		&rec.FailureReason,
		&rec.ErrorCode,
		&rec.ErrorFingerprint,
		&rec.Status,
		&rec.RedriveCount,
		&rec.MaxRedriveAttempts,
		&rec.QuarantinedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, infraDb.MapDatabaseError(err)
	}

	if len(headersRaw) > 0 {
		_ = json.Unmarshal(headersRaw, &rec.Headers)
	}

	return &rec, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) ListQuarantined(ctx context.Context, tenantID, handlerID string, status contracts.QuarantineStatus, limit, offset int) ([]contracts.DeadLetterQuarantineRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowListQuarantined, tenantID, handlerID, string(status), limit, offset)
	if err != nil {
		return nil, infraDb.MapDatabaseError(err)
	}
	defer rows.Close()

	var results []contracts.DeadLetterQuarantineRecord
	for rows.Next() {
		var rec contracts.DeadLetterQuarantineRecord
		var headersRaw []byte

		if err := rows.Scan(
			&rec.ID,
			&rec.PublicID,
			&rec.TenantID,
			&rec.SourceService,
			&rec.ExternalID,
			&rec.CorrelationID,
			&rec.Topic,
			&rec.Partition,
			&rec.SourceOffset,
			&rec.ConsumerGroup,
			&rec.HandlerID,
			&rec.EventID,
			&rec.AggregateType,
			&rec.AggregateID,
			&rec.AggregateVersion,
			&rec.EventType,
			&rec.Payload,
			&headersRaw,
			&rec.FailureReason,
			&rec.ErrorCode,
			&rec.ErrorFingerprint,
			&rec.Status,
			&rec.RedriveCount,
			&rec.MaxRedriveAttempts,
			&rec.QuarantinedAt,
			&rec.UpdatedAt,
		); err != nil {
			return nil, infraDb.MapDatabaseError(err)
		}

		if len(headersRaw) > 0 {
			_ = json.Unmarshal(headersRaw, &rec.Headers)
		}
		results = append(results, rec)
	}

	return results, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) UpdateDisposition(ctx context.Context, tenantID, id string, status contracts.QuarantineStatus, redriveCount int) error {
	_, err := p.db.ExecContext(ctx, queries.FlowUpdateQuarantineDisposition, tenantID, id, string(status), redriveCount)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) RecordAttempt(ctx context.Context, attempt contracts.AttemptRecord) (int, error) {
	query := `
INSERT INTO attempt_record (
    tenant_id, consumer_group, handler_id, event_id, attempt_count, first_attempt_at, last_attempt_at, last_error_fingerprint, last_error_class
) VALUES (
    $1, $2, $3, $4, 1, NOW(), NOW(), $5, $6
) ON CONFLICT (tenant_id, consumer_group, handler_id, event_id) DO UPDATE
SET attempt_count = attempt_record.attempt_count + 1,
    last_attempt_at = NOW(),
    last_error_fingerprint = $5,
    last_error_class = $6
RETURNING attempt_count;`

	var count int
	err := p.db.QueryRowContext(ctx, query, attempt.TenantID, attempt.ConsumerGroup, attempt.HandlerID, attempt.EventID, attempt.LastErrorFingerprint, attempt.LastErrorClass).Scan(&count)
	if err != nil {
		return 0, infraDb.MapDatabaseError(err)
	}
	return count, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) DeleteAttempt(ctx context.Context, tenantID, consumerGroup, handlerID, eventID string) error {
	_, err := p.db.ExecContext(ctx, "DELETE FROM attempt_record WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND event_id = $4", tenantID, consumerGroup, handlerID, eventID)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) ParkKey(ctx context.Context, park contracts.AggregateParkingState) error {
	query := `
INSERT INTO aggregate_parking_store (
    tenant_id, handler_id, aggregate_type, aggregate_id, is_parked, parked_reason, quarantine_id, owner_epoch, parked_event_count, parked_at, updated_at
) VALUES (
    $1, $2, $3, $4, TRUE, $5, $6, $7, $8, NOW(), NOW()
) ON CONFLICT (tenant_id, handler_id, aggregate_type, aggregate_id) DO UPDATE
SET is_parked = TRUE,
    parked_reason = $5,
    quarantine_id = $6,
    owner_epoch = $7,
    updated_at = NOW();`

	_, err := p.db.ExecContext(
		ctx,
		query,
		park.TenantID,
		park.HandlerID,
		park.AggregateType,
		park.AggregateID,
		park.ParkedReason,
		park.QuarantineID,
		park.OwnerEpoch,
		park.ParkedEventCount,
	)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) UnparkKey(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string, ownerEpoch int64) error {
	query := `
UPDATE aggregate_parking_store
SET is_parked = FALSE,
    updated_at = NOW()
WHERE tenant_id = $1 AND handler_id = $2 AND aggregate_type = $3 AND aggregate_id = $4 AND ($5 <= 0 OR owner_epoch <= $5);`

	_, err := p.db.ExecContext(ctx, query, tenantID, handlerID, aggregateType, aggregateID, ownerEpoch)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) IsKeyParked(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error) {
	var isParked bool
	err := p.db.QueryRowContext(ctx, queries.FlowCheckKeyParked, tenantID, handlerID, aggregateType, aggregateID).Scan(&isParked)
	if err != nil {
		return false, infraDb.MapDatabaseError(err)
	}
	return isParked, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) BufferParkedEvent(ctx context.Context, event contracts.ParkedEventRecord) error {
	headersJSON, err := json.Marshal(event.Headers)
	if err != nil {
		headersJSON = []byte("{}")
	}

	query := `
INSERT INTO parked_event (
    id, tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version, event_id, event_type, payload, headers, source_offset, arrived_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW()
);`

	_, err = p.db.ExecContext(ctx, query, event.ID, event.TenantID, event.ConsumerGroup, event.HandlerID, event.AggregateType, event.AggregateID, event.AggregateVersion, event.EventID, event.EventType, event.Payload, headersJSON, event.SourceOffset)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) GetParkedEventsForDrain(ctx context.Context, tenantID, consumerGroup, handlerID, aggregateType, aggregateID string) ([]contracts.ParkedEventRecord, error) {
	query := `
SELECT 
    id, tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version, event_id, event_type, payload, headers, source_offset, arrived_at
FROM parked_event
WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_type = $4 AND aggregate_id = $5
ORDER BY aggregate_version ASC;`

	rows, err := p.db.QueryContext(ctx, query, tenantID, consumerGroup, handlerID, aggregateType, aggregateID)
	if err != nil {
		return nil, infraDb.MapDatabaseError(err)
	}
	defer rows.Close()

	var events []contracts.ParkedEventRecord
	for rows.Next() {
		var e contracts.ParkedEventRecord
		var headersRaw []byte
		if err := rows.Scan(&e.ID, &e.TenantID, &e.ConsumerGroup, &e.HandlerID, &e.AggregateType, &e.AggregateID, &e.AggregateVersion, &e.EventID, &e.EventType, &e.Payload, &headersRaw, &e.SourceOffset, &e.ArrivedAt); err != nil {
			return nil, infraDb.MapDatabaseError(err)
		}
		if len(headersRaw) > 0 {
			_ = json.Unmarshal(headersRaw, &e.Headers)
		}
		events = append(events, e)
	}
	return events, nil
}

func (p *PostgresDeadLetterRepositoryAdapter) DeleteParkedEvent(ctx context.Context, tenantID, consumerGroup, handlerID, aggregateType, aggregateID string, version int64) error {
	query := `
DELETE FROM parked_event
WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_type = $4 AND aggregate_id = $5 AND aggregate_version = $6;`

	_, err := p.db.ExecContext(ctx, query, tenantID, consumerGroup, handlerID, aggregateType, aggregateID, version)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) StoreConsumedOffset(ctx context.Context, offset contracts.ConsumedOffsetRecord) error {
	query := `
INSERT INTO consumed_offset (
    tenant_id, consumer_group, topic, partition, next_offset, owner_epoch, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, NOW()
) ON CONFLICT (tenant_id, consumer_group, topic, partition) DO UPDATE
SET next_offset = $5,
    owner_epoch = $6,
    updated_at = NOW();`

	_, err := p.db.ExecContext(ctx, query, offset.TenantID, offset.ConsumerGroup, offset.Topic, offset.Partition, offset.NextOffset, offset.OwnerEpoch)
	if err != nil {
		return infraDb.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresDeadLetterRepositoryAdapter) GetConsumedOffset(ctx context.Context, tenantID, consumerGroup, topic string, partition int) (*contracts.ConsumedOffsetRecord, error) {
	query := `
SELECT tenant_id, consumer_group, topic, partition, next_offset, owner_epoch, updated_at
FROM consumed_offset
WHERE tenant_id = $1 AND consumer_group = $2 AND topic = $3 AND partition = $4;`

	var off contracts.ConsumedOffsetRecord
	err := p.db.QueryRowContext(ctx, query, tenantID, consumerGroup, topic, partition).Scan(
		&off.TenantID, &off.ConsumerGroup, &off.Topic, &off.Partition, &off.NextOffset, &off.OwnerEpoch, &off.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, infraDb.MapDatabaseError(err)
	}
	return &off, nil
}

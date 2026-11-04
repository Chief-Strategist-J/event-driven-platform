package adapters

import (
	"context"
	"database/sql"
	"time"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/infrastructure/ports"
	"event-driven-platform/src/features/consumer-groups/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresConsumerGroupsAdapter struct {
	db *sql.DB
}

func NewPostgresConsumerGroupsAdapter(db *sql.DB) *PostgresConsumerGroupsAdapter {
	return &PostgresConsumerGroupsAdapter{db: db}
}

var _ ports.ConsumerGroupsRepositoryPort = (*PostgresConsumerGroupsAdapter)(nil)

func (p *PostgresConsumerGroupsAdapter) UpsertMember(ctx context.Context, m contracts.ConsumerGroupMemberRecord) (*contracts.ConsumerGroupMemberRecord, error) {
	row := p.db.QueryRowContext(ctx, queries.FlowUpsertGroupMember,
		m.ID, m.PublicID, m.TenantID, m.SourceService, m.ExternalID, m.CorrelationID,
		m.GroupID, m.MemberID, m.InstanceID, m.ClientHost, m.AssignmentStrategy,
		m.AssignedPartitions, m.GenerationID, m.HeartbeatAt, string(m.Status),
	)

	err := row.Scan(&m.ID, &m.PublicID, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	return &m, nil
}

func (p *PostgresConsumerGroupsAdapter) GetMember(ctx context.Context, tenantID, groupID, memberID string) (*contracts.ConsumerGroupMemberRecord, error) {
	row := p.db.QueryRowContext(ctx, queries.FlowGetGroupMember, tenantID, groupID, memberID)
	var m contracts.ConsumerGroupMemberRecord
	var instID sql.NullString
	var statusStr string

	err := row.Scan(
		&m.ID, &m.PublicID, &m.TenantID, &m.SourceService, &m.ExternalID, &m.CorrelationID,
		&m.GroupID, &m.MemberID, &instID, &m.ClientHost, &m.AssignmentStrategy,
		&m.AssignedPartitions, &m.GenerationID, &m.HeartbeatAt, &statusStr,
		&m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, database.MapDatabaseError(err)
	}
	if instID.Valid {
		val := instID.String
		m.InstanceID = &val
	}
	m.Status = contracts.MemberStatus(statusStr)
	return &m, nil
}

func (p *PostgresConsumerGroupsAdapter) Heartbeat(ctx context.Context, tenantID, groupID, memberID string, now time.Time) (int64, contracts.MemberStatus, error) {
	var genID int64
	var statusStr string
	row := p.db.QueryRowContext(ctx, queries.FlowUpdateMemberHeartbeat, tenantID, groupID, memberID, now)
	err := row.Scan(&genID, &statusStr)
	if err != nil {
		return 0, "", database.MapDatabaseError(err)
	}
	return genID, contracts.MemberStatus(statusStr), nil
}

func (p *PostgresConsumerGroupsAdapter) UpdatePartitions(ctx context.Context, tenantID, groupID, memberID string, partitions []byte, generationID int64) error {
	_, err := p.db.ExecContext(ctx, queries.FlowUpdateMemberPartitions, tenantID, groupID, memberID, partitions, generationID)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresConsumerGroupsAdapter) RecordRebalance(ctx context.Context, l contracts.RebalanceLogRecord) error {
	row := p.db.QueryRowContext(ctx, queries.FlowInsertRebalanceLog,
		l.ID, l.PublicID, l.TenantID, l.SourceService, l.ExternalID, l.CorrelationID,
		l.GroupID, l.GenerationID, l.RebalanceReason, l.MemberCount,
		l.RevokedPartitions, l.AssignedPartitions, l.DurationMs,
	)
	err := row.Scan(&l.CreatedAt)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresConsumerGroupsAdapter) ListMembers(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerGroupMemberRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowListGroupMembers, tenantID, groupID)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var members []contracts.ConsumerGroupMemberRecord
	for rows.Next() {
		var m contracts.ConsumerGroupMemberRecord
		var instID sql.NullString
		var statusStr string

		err := rows.Scan(
			&m.ID, &m.PublicID, &m.TenantID, &m.SourceService, &m.ExternalID, &m.CorrelationID,
			&m.GroupID, &m.MemberID, &instID, &m.ClientHost, &m.AssignmentStrategy,
			&m.AssignedPartitions, &m.GenerationID, &m.HeartbeatAt, &statusStr,
			&m.CreatedAt, &m.UpdatedAt,
		)
		if err != nil {
			return nil, database.MapDatabaseError(err)
		}
		if instID.Valid {
			val := instID.String
			m.InstanceID = &val
		}
		m.Status = contracts.MemberStatus(statusStr)
		members = append(members, m)
	}
	return members, nil
}


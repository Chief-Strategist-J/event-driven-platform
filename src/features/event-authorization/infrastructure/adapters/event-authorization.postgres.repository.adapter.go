package adapters

import (
	"context"
	"database/sql"
	"errors"

	"event-driven-platform/src/features/event-authorization/contracts"
	"event-driven-platform/src/features/event-authorization/infrastructure/ports"
	"event-driven-platform/src/features/event-authorization/infrastructure/queries"
)

type PostgresEventAuthorizationRepository struct {
	db *sql.DB
}

var _ ports.EventAuthorizationRepositoryPort = (*PostgresEventAuthorizationRepository)(nil)

func NewPostgresEventAuthorizationRepository(db *sql.DB) *PostgresEventAuthorizationRepository {
	return &PostgresEventAuthorizationRepository{db: db}
}

func (r *PostgresEventAuthorizationRepository) SavePolicy(ctx context.Context, p *contracts.AuthPolicyRecord) error {
	_, err := r.db.ExecContext(ctx, queries.UpsertPolicyQuery,
		p.ID,
		p.PublicID,
		p.TenantID,
		p.SourceService,
		p.ExternalID,
		p.CorrelationID,
		p.PolicyName,
		p.PrincipalID,
		string(p.PrincipalType),
		string(p.ResourceType),
		p.ResourcePattern,
		string(p.Operation),
		string(p.Effect),
		string(p.State),
		p.CreatedAt,
		p.UpdatedAt,
	)
	return err
}

func (r *PostgresEventAuthorizationRepository) GetPolicyByPublicID(ctx context.Context, publicID string) (*contracts.AuthPolicyRecord, error) {
	row := r.db.QueryRowContext(ctx, queries.SelectPolicyByPublicIDQuery, publicID)
	var p contracts.AuthPolicyRecord
	var pType, rType, op, eff, st string
	err := row.Scan(
		&p.ID,
		&p.PublicID,
		&p.TenantID,
		&p.SourceService,
		&p.ExternalID,
		&p.CorrelationID,
		&p.PolicyName,
		&p.PrincipalID,
		&pType,
		&rType,
		&p.ResourcePattern,
		&op,
		&eff,
		&st,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.PrincipalType = contracts.PrincipalType(pType)
	p.ResourceType = contracts.ResourceType(rType)
	p.Operation = contracts.Operation(op)
	p.Effect = contracts.PolicyEffect(eff)
	p.State = contracts.PolicyState(st)
	return &p, nil
}

func (r *PostgresEventAuthorizationRepository) FindMatchingPolicies(ctx context.Context, tenantID, principalID string, resType contracts.ResourceType, op contracts.Operation) ([]*contracts.AuthPolicyRecord, error) {
	rows, err := r.db.QueryContext(ctx, queries.SelectMatchingPoliciesQuery, tenantID, principalID, string(resType), string(op))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*contracts.AuthPolicyRecord
	for rows.Next() {
		var p contracts.AuthPolicyRecord
		var pType, rType, opStr, eff, st string
		if err := rows.Scan(
			&p.ID,
			&p.PublicID,
			&p.TenantID,
			&p.SourceService,
			&p.ExternalID,
			&p.CorrelationID,
			&p.PolicyName,
			&p.PrincipalID,
			&pType,
			&rType,
			&p.ResourcePattern,
			&opStr,
			&eff,
			&st,
			&p.CreatedAt,
			&p.UpdatedAt,
		); err != nil {
			return nil, err
		}
		p.PrincipalType = contracts.PrincipalType(pType)
		p.ResourceType = contracts.ResourceType(rType)
		p.Operation = contracts.Operation(opStr)
		p.Effect = contracts.PolicyEffect(eff)
		p.State = contracts.PolicyState(st)
		results = append(results, &p)
	}
	return results, rows.Err()
}

func (r *PostgresEventAuthorizationRepository) GetTopicWriterCount(ctx context.Context, tenantID, topicPattern string) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, queries.CountTopicWritersQuery, tenantID, topicPattern).Scan(&count)
	return count, err
}

func (r *PostgresEventAuthorizationRepository) UpdatePolicyState(ctx context.Context, publicID string, state contracts.PolicyState) error {
	_, err := r.db.ExecContext(ctx, queries.UpdatePolicyStateQuery, publicID, string(state))
	return err
}

func (r *PostgresEventAuthorizationRepository) SaveAuditLog(ctx context.Context, a *contracts.AuthAuditRecord) error {
	_, err := r.db.ExecContext(ctx, queries.InsertAuditLogQuery,
		a.ID,
		a.PublicID,
		a.TenantID,
		a.SourceService,
		a.ExternalID,
		a.CorrelationID,
		a.PrincipalID,
		string(a.PrincipalType),
		string(a.ResourceType),
		a.ResourceName,
		string(a.Operation),
		string(a.Decision),
		a.MatchedPolicyID,
		a.Reason,
		a.EvaluatedAt,
	)
	return err
}

func (r *PostgresEventAuthorizationRepository) GetAuditLogByPublicID(ctx context.Context, publicID string) (*contracts.AuthAuditRecord, error) {
	row := r.db.QueryRowContext(ctx, queries.SelectAuditLogByPublicIDQuery, publicID)
	var a contracts.AuthAuditRecord
	var pType, rType, opStr, dec string
	err := row.Scan(
		&a.ID,
		&a.PublicID,
		&a.TenantID,
		&a.SourceService,
		&a.ExternalID,
		&a.CorrelationID,
		&a.PrincipalID,
		&pType,
		&rType,
		&a.ResourceName,
		&opStr,
		&dec,
		&a.MatchedPolicyID,
		&a.Reason,
		&a.EvaluatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a.PrincipalType = contracts.PrincipalType(pType)
	a.ResourceType = contracts.ResourceType(rType)
	a.Operation = contracts.Operation(opStr)
	a.Decision = contracts.Decision(dec)
	return &a, nil
}

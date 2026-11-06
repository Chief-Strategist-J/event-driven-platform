package adapters

import (
	"context"
	"database/sql"
	"errors"

	"event-driven-platform/src/features/event-operations-dashboard/contracts"
	"event-driven-platform/src/features/event-operations-dashboard/infrastructure/ports"
	"event-driven-platform/src/features/event-operations-dashboard/infrastructure/queries"
)

type PostgresEventOperationsDashboardRepository struct {
	db *sql.DB
}

var _ ports.EventOperationsDashboardRepositoryPort = (*PostgresEventOperationsDashboardRepository)(nil)

func NewPostgresEventOperationsDashboardRepository(db *sql.DB) *PostgresEventOperationsDashboardRepository {
	return &PostgresEventOperationsDashboardRepository{db: db}
}

func (r *PostgresEventOperationsDashboardRepository) SaveHopTelemetry(ctx context.Context, h *contracts.PipelineHopRecord) error {
	_, err := r.db.ExecContext(ctx, queries.UpsertPipelineHopQuery,
		h.ID,
		h.PublicID,
		h.TenantID,
		h.SourceService,
		h.ExternalID,
		h.CorrelationID,
		h.FlowName,
		string(h.HopCode),
		string(h.Tier),
		string(h.Status),
		string(h.PauseReason),
		h.LagSeconds,
		h.OldestHeldAgeSeconds,
		h.RunbookURL,
		h.LastReportedAt,
		h.CreatedAt,
		h.UpdatedAt,
	)
	return err
}

func (r *PostgresEventOperationsDashboardRepository) GetHopsByFlow(ctx context.Context, tenantID, flowName string) ([]*contracts.PipelineHopRecord, error) {
	rows, err := r.db.QueryContext(ctx, queries.SelectHopsByFlowQuery, tenantID, flowName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hops []*contracts.PipelineHopRecord
	for rows.Next() {
		var h contracts.PipelineHopRecord
		var hopCode, tier, status, pReason string
		if err := rows.Scan(
			&h.ID,
			&h.PublicID,
			&h.TenantID,
			&h.SourceService,
			&h.ExternalID,
			&h.CorrelationID,
			&h.FlowName,
			&hopCode,
			&tier,
			&status,
			&pReason,
			&h.LagSeconds,
			&h.OldestHeldAgeSeconds,
			&h.RunbookURL,
			&h.LastReportedAt,
			&h.CreatedAt,
			&h.UpdatedAt,
		); err != nil {
			return nil, err
		}
		h.HopCode = contracts.HopCode(hopCode)
		h.Tier = contracts.FlowTier(tier)
		h.Status = contracts.HopStatus(status)
		h.PauseReason = contracts.PauseReason(pReason)
		hops = append(hops, &h)
	}
	return hops, rows.Err()
}

func (r *PostgresEventOperationsDashboardRepository) GetHopByPublicID(ctx context.Context, publicID string) (*contracts.PipelineHopRecord, error) {
	row := r.db.QueryRowContext(ctx, queries.SelectHopByPublicIDQuery, publicID)
	var h contracts.PipelineHopRecord
	var hopCode, tier, status, pReason string
	err := row.Scan(
		&h.ID,
		&h.PublicID,
		&h.TenantID,
		&h.SourceService,
		&h.ExternalID,
		&h.CorrelationID,
		&h.FlowName,
		&hopCode,
		&tier,
		&status,
		&pReason,
		&h.LagSeconds,
		&h.OldestHeldAgeSeconds,
		&h.RunbookURL,
		&h.LastReportedAt,
		&h.CreatedAt,
		&h.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	h.HopCode = contracts.HopCode(hopCode)
	h.Tier = contracts.FlowTier(tier)
	h.Status = contracts.HopStatus(status)
	h.PauseReason = contracts.PauseReason(pReason)
	return &h, nil
}

func (r *PostgresEventOperationsDashboardRepository) SaveTriageLog(ctx context.Context, t *contracts.IncidentTriageRecord) error {
	_, err := r.db.ExecContext(ctx, queries.InsertIncidentTriageQuery,
		t.ID,
		t.PublicID,
		t.TenantID,
		t.SourceService,
		t.ExternalID,
		t.CorrelationID,
		t.IncidentID,
		t.FlowName,
		t.LocalizedHop,
		string(t.Severity),
		t.TriageAction,
		t.CCTLActionLink,
		t.TriagedBy,
		t.TriagedAt,
	)
	return err
}

func (r *PostgresEventOperationsDashboardRepository) GetTriageLogByPublicID(ctx context.Context, publicID string) (*contracts.IncidentTriageRecord, error) {
	row := r.db.QueryRowContext(ctx, queries.SelectTriageByPublicIDQuery, publicID)
	var t contracts.IncidentTriageRecord
	var sev string
	err := row.Scan(
		&t.ID,
		&t.PublicID,
		&t.TenantID,
		&t.SourceService,
		&t.ExternalID,
		&t.CorrelationID,
		&t.IncidentID,
		&t.FlowName,
		&t.LocalizedHop,
		&sev,
		&t.TriageAction,
		&t.CCTLActionLink,
		&t.TriagedBy,
		&t.TriagedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.Severity = contracts.IncidentSeverity(sev)
	return &t, nil
}

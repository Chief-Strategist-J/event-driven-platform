package ports

import (
	"context"

	"github.com/google/uuid"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
)

type ConsistencyRepositoryPort interface {
	UpsertWatermark(ctx context.Context, record contracts.WatermarkRecord) error
	GetWatermarksByView(ctx context.Context, tenantID string, viewName string) ([]contracts.WatermarkRecord, error)
	RecordDriftIncident(ctx context.Context, record contracts.DriftIncidentRecord) error
	GetActiveDriftIncidents(ctx context.Context, tenantID string, viewName string) ([]contracts.DriftIncidentRecord, error)
	UpdateDriftIncidentStatus(ctx context.Context, id uuid.UUID, tenantID string, status contracts.IncidentStatus, healAttempts int) error
	RecordCheckRun(ctx context.Context, record contracts.CheckRunRecord) error
	ListCheckRuns(ctx context.Context, tenantID string, viewName string, limit int) ([]contracts.CheckRunRecord, error)
	RegisterSilence(ctx context.Context, record contracts.SilenceRecord) error
	GetActiveSilence(ctx context.Context, tenantID string, viewName string) (*contracts.SilenceRecord, error)
}


package ports

import (
	"context"
	"event-driven-platform/src/features/event-operations-dashboard/contracts"
)

type EventOperationsDashboardRepositoryPort interface {
	SaveHopTelemetry(ctx context.Context, hop *contracts.PipelineHopRecord) error
	GetHopsByFlow(ctx context.Context, tenantID, flowName string) ([]*contracts.PipelineHopRecord, error)
	GetHopByPublicID(ctx context.Context, publicID string) (*contracts.PipelineHopRecord, error)
	SaveTriageLog(ctx context.Context, triage *contracts.IncidentTriageRecord) error
	GetTriageLogByPublicID(ctx context.Context, publicID string) (*contracts.IncidentTriageRecord, error)
}

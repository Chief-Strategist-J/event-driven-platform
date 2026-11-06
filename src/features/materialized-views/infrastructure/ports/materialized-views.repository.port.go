package ports

import (
	"context"

	"event-driven-platform/src/features/materialized-views/contracts"
)

type ViewRepositoryPort interface {
	RegisterView(ctx context.Context, reg contracts.ViewRegistryRecord) error
	GetViewByName(ctx context.Context, tenantID string, viewName string) (*contracts.ViewRegistryRecord, error)
	GetViewByPublicID(ctx context.Context, publicID string) (*contracts.ViewRegistryRecord, error)
	UpdateViewStatus(ctx context.Context, tenantID string, viewName string, status contracts.ViewStatus) error

	GetProjectionState(ctx context.Context, tenantID string, viewName string, aggType string, aggID string) (*contracts.ProjectionStateRecord, error)
	UpsertProjectionState(ctx context.Context, state contracts.ProjectionStateRecord) error

	QuarantineHeldEvent(ctx context.Context, event contracts.HeldEventRecord) error
	ListHeldEvents(ctx context.Context, tenantID string, viewName string, aggType string, aggID string) ([]contracts.HeldEventRecord, error)
	DeleteHeldEvent(ctx context.Context, tenantID string, eventID string) error

	InitiateRebuild(ctx context.Context, log contracts.RebuildLogRecord) error
	GetRebuildLog(ctx context.Context, tenantID string, rebuildID string) (*contracts.RebuildLogRecord, error)
	UpdateRebuildPhase(ctx context.Context, tenantID string, rebuildID string, phase contracts.RebuildPhase, cursor int64) error

	RecordContribution(ctx context.Context, contrib contracts.AggContributionRecord) error
	GetAggregateContributionTotal(ctx context.Context, tenantID string, viewName string, aggType string, aggID string) (float64, error)
	UpsertPeriodLedger(ctx context.Context, period contracts.PeriodLedgerRecord) error
	GetPeriodLedger(ctx context.Context, tenantID string, viewName string, periodKey string) (*contracts.PeriodLedgerRecord, error)
	ApplyPeriodAdjustment(ctx context.Context, tenantID string, viewName string, periodKey string, adjustment float64) error
}


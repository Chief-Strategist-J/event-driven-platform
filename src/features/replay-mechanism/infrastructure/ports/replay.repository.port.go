package ports

import (
	"context"

	"event-driven-platform/src/features/replay-mechanism/contracts"
)

type ReplayRepositoryPort interface {
	CreateReplayRun(ctx context.Context, run contracts.ReplayRunRecord) (*contracts.ReplayRunRecord, error)
	UpdateReplayRunState(ctx context.Context, tenantID, runID string, state contracts.ReplayState) (*contracts.ReplayRunRecord, error)
	UpdateReplayRunOffset(ctx context.Context, tenantID, runID string, currentOffset int64, state contracts.ReplayState) (*contracts.ReplayRunRecord, error)
	GetReplayRun(ctx context.Context, tenantID, runID string) (*contracts.ReplayRunRecord, error)
	ListReplayRunsByTenant(ctx context.Context, tenantID string) ([]contracts.ReplayRunRecord, error)
	RecordEffectLog(ctx context.Context, effect contracts.ReplayEffectRecord) error
	RecordAuditLog(ctx context.Context, audit contracts.ReplayAuditRecord) error
}


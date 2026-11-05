package ports

import (
	"context"

	"event-driven-platform/src/features/backpressure/contracts"
)

type BackpressureRepositoryPort interface {
	UpsertBackpressureRecord(ctx context.Context, record contracts.BackpressureRecord) (*contracts.BackpressureRecord, error)
	GetBackpressureRecord(ctx context.Context, tenantID, handlerID, dependencyID string) (*contracts.BackpressureRecord, error)
	ListBackpressureByTenant(ctx context.Context, tenantID string) ([]contracts.BackpressureRecord, error)
	RecordAuditLog(ctx context.Context, audit contracts.BackpressureAuditRecord) error
}


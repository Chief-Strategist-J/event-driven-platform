package ports

import (
	"context"
	"time"

	"event-driven-platform/src/features/transactional-outbox/contracts"
)

type OutboxRepositoryPort interface {
	ClaimIdempotency(ctx context.Context, tenantID, idempotencyKey, recordID string) (bool, string, error)
	Save(ctx context.Context, record contracts.OutboxRecord) error
	SaveAndImmediateDelete(ctx context.Context, record contracts.OutboxRecord) error
	GetByID(ctx context.Context, tenantID string, id string) (*contracts.OutboxRecord, error)
	FetchPending(ctx context.Context, limit int) ([]contracts.OutboxRecord, error)
	ClaimInFlight(ctx context.Context, workerID string, leaseDuration time.Duration, limit int) ([]contracts.OutboxRecord, error)
	MarkPublished(ctx context.Context, tenantID string, id string) error
	MarkPublishedByWorker(ctx context.Context, tenantID string, id string, workerID string) error
	MarkFailed(ctx context.Context, tenantID string, id string, errMessage string) error
	PurgeExpired(ctx context.Context) error
}

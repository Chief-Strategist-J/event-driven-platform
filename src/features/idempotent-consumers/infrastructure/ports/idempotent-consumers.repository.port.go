package ports

import (
	"context"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
)

type ConsumerRepositoryPort interface {
	RecordInbox(ctx context.Context, record contracts.ConsumerInboxRecord) (bool, error)
	GetProjection(ctx context.Context, tenantID, aggregateType, aggregateID string) (*contracts.AggregateProjectionState, error)
	ApplyProjectionEffect(ctx context.Context, tenantID, aggregateType, aggregateID string, version int64, eventID, payloadHash string) error
	ParkAggregate(ctx context.Context, tenantID, aggregateType, aggregateID string, reason string) error
	RecordPartitionOffset(ctx context.Context, topic string, partition int, offset int64) error
}

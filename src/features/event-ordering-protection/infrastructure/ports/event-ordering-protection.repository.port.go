package ports

import (
	"context"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
)

type EventOrderingRepositoryPort interface {
	GetSequenceTracker(ctx context.Context, tenantID, group, handler, aggType, aggID string) (*contracts.AggregateSequenceTrackerRecord, error)
	ApplyEventSequence(ctx context.Context, r contracts.AggregateSequenceTrackerRecord) error
	HoldAggregateKey(ctx context.Context, r contracts.AggregateSequenceTrackerRecord) error
	StoreHeldEvent(ctx context.Context, r contracts.SequenceHeldEventRecord) error
	GetHeldEventsForDrain(ctx context.Context, tenantID, group, handler, aggType, aggID string) ([]contracts.SequenceHeldEventRecord, error)
	DeleteHeldEvent(ctx context.Context, tenantID, group, handler, aggType, aggID string, version int64) error
	ListHeldTrackers(ctx context.Context, tenantID, group, handler string, limit, offset int) ([]contracts.AggregateSequenceTrackerRecord, error)
}


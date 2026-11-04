package ports

import (
	"context"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
)

type DeadLetterRepositoryPort interface {
	Quarantine(ctx context.Context, record contracts.DeadLetterQuarantineRecord) error
	GetQuarantinedByID(ctx context.Context, tenantID, id string) (*contracts.DeadLetterQuarantineRecord, error)
	ListQuarantined(ctx context.Context, tenantID, handlerID string, status contracts.QuarantineStatus, limit, offset int) ([]contracts.DeadLetterQuarantineRecord, error)
	UpdateDisposition(ctx context.Context, tenantID, id string, status contracts.QuarantineStatus, redriveCount int) error

	RecordAttempt(ctx context.Context, attempt contracts.AttemptRecord) (int, error)
	DeleteAttempt(ctx context.Context, tenantID, consumerGroup, handlerID, eventID string) error

	ParkKey(ctx context.Context, park contracts.AggregateParkingState) error
	UnparkKey(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string, ownerEpoch int64) error
	IsKeyParked(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error)

	BufferParkedEvent(ctx context.Context, event contracts.ParkedEventRecord) error
	GetParkedEventsForDrain(ctx context.Context, tenantID, consumerGroup, handlerID, aggregateType, aggregateID string) ([]contracts.ParkedEventRecord, error)
	DeleteParkedEvent(ctx context.Context, tenantID, consumerGroup, handlerID, aggregateType, aggregateID string, version int64) error

	StoreConsumedOffset(ctx context.Context, offset contracts.ConsumedOffsetRecord) error
	GetConsumedOffset(ctx context.Context, tenantID, consumerGroup, topic string, partition int) (*contracts.ConsumedOffsetRecord, error)
}

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
	ParkKey(ctx context.Context, park contracts.AggregateParkingState) error
	UnparkKey(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) error
	IsKeyParked(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error)
}


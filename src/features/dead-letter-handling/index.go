package deadletterhandling

import (
	"context"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/service"
)

type Feature struct {
	svc *service.DeadLetterService
}

func NewFeature(svc *service.DeadLetterService) *Feature {
	return &Feature{svc: svc}
}

func (f *Feature) Quarantine(ctx context.Context, input contracts.QuarantineEventInput) (*contracts.DeadLetterQuarantineRecord, error) {
	return f.svc.QuarantineEvent(ctx, input)
}

func (f *Feature) Redrive(ctx context.Context, tenantID, id string, invoker service.ConsumerInvoker) (*contracts.RedriveResult, error) {
	return f.svc.RedriveEvent(ctx, tenantID, id, invoker)
}

func (f *Feature) Discard(ctx context.Context, tenantID, id, reason string) error {
	return f.svc.DiscardEvent(ctx, tenantID, id, reason)
}

func (f *Feature) IsKeyParked(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error) {
	return f.svc.IsKeyParked(ctx, tenantID, handlerID, aggregateType, aggregateID)
}

func (f *Feature) ListQuarantined(ctx context.Context, tenantID, handlerID string, status contracts.QuarantineStatus, limit, offset int) ([]contracts.DeadLetterQuarantineRecord, error) {
	return f.svc.ListQuarantined(ctx, tenantID, handlerID, status, limit, offset)
}


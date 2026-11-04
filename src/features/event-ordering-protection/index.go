package eventorderingprotection

import (
	"context"
	"database/sql"

	contracts "event-driven-platform/src/features/event-ordering-protection/contracts"
	adapters "event-driven-platform/src/features/event-ordering-protection/infrastructure/adapters"
	ports "event-driven-platform/src/features/event-ordering-protection/infrastructure/ports"
	service "event-driven-platform/src/features/event-ordering-protection/service"
)

type EventOrderingProtectionFeature struct {
	Service *service.EventOrderingService
}

func NewEventOrderingProtectionFeatureWithPostgres(db *sql.DB) *EventOrderingProtectionFeature {
	adapter := adapters.NewPostgresEventOrderingAdapter(db)
	svc := service.NewEventOrderingService(adapter)
	return &EventOrderingProtectionFeature{
		Service: svc,
	}
}

func NewEventOrderingProtectionFeatureWithMemory() *EventOrderingProtectionFeature {
	adapter := adapters.NewMemoryEventOrderingAdapter()
	svc := service.NewEventOrderingService(adapter)
	return &EventOrderingProtectionFeature{
		Service: svc,
	}
}

func (f *EventOrderingProtectionFeature) EvaluateAndApply(ctx context.Context, input contracts.IngestEventSequenceInput) (*contracts.SequenceEvaluationResult, error) {
	return f.Service.EvaluateAndApply(ctx, input)
}

func (f *EventOrderingProtectionFeature) DrainHeldEvents(ctx context.Context, input contracts.DrainHeldEventsInput) (*contracts.DrainHeldEventsResult, error) {
	return f.Service.DrainHeldEvents(ctx, input)
}

func (f *EventOrderingProtectionFeature) ListHeldTrackers(ctx context.Context, tenantID, group, handler string, limit, offset int) ([]contracts.AggregateSequenceTrackerRecord, error) {
	return f.Service.ListHeldTrackers(ctx, tenantID, group, handler, limit, offset)
}

func (f *EventOrderingProtectionFeature) GetRepository() ports.EventOrderingRepositoryPort {
	return f.Service.GetRepository()
}

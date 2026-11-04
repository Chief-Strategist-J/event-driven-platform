package retrytopics

import (
	"context"
	"database/sql"

	contracts "event-driven-platform/src/features/retry-topics/contracts"
	adapters "event-driven-platform/src/features/retry-topics/infrastructure/adapters"
	ports "event-driven-platform/src/features/retry-topics/infrastructure/ports"
	service "event-driven-platform/src/features/retry-topics/service"
)

type RetryTopicsFeature struct {
	Service *service.RetryTopicsService
}

func NewRetryTopicsFeatureWithPostgres(db *sql.DB) *RetryTopicsFeature {
	adapter := adapters.NewPostgresRetryTopicsAdapter(db)
	svc := service.NewRetryTopicsService(adapter)
	return &RetryTopicsFeature{
		Service: svc,
	}
}

func NewRetryTopicsFeatureWithMemory() *RetryTopicsFeature {
	adapter := adapters.NewMemoryRetryTopicsAdapter()
	svc := service.NewRetryTopicsService(adapter)
	return &RetryTopicsFeature{
		Service: svc,
	}
}

func (f *RetryTopicsFeature) EvaluateRetry(ctx context.Context, input contracts.EvaluateRetryInput) (*contracts.EvaluateRetryResult, error) {
	return f.Service.EvaluateRetry(ctx, input)
}

func (f *RetryTopicsFeature) ScheduleRetryAttempt(ctx context.Context, input contracts.ScheduleRetryInput) (*contracts.RetryAttemptLedgerRecord, error) {
	return f.Service.ScheduleRetryAttempt(ctx, input)
}

func (f *RetryTopicsFeature) GetDueRetries(ctx context.Context, tenantID string, limit int) ([]contracts.RetryAttemptLedgerRecord, error) {
	return f.Service.GetDueRetries(ctx, tenantID, limit)
}

func (f *RetryTopicsFeature) GetRepository() ports.RetryTopicsRepositoryPort {
	return f.Service.GetRepository()
}

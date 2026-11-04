package offset_management

import (
	"context"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/infrastructure/adapters"
	"event-driven-platform/src/features/offset-management/infrastructure/ports"
	"event-driven-platform/src/features/offset-management/service"
)

type OffsetManagementFeature struct {
	Service *service.OffsetManagementService
}

func NewOffsetManagementFeature(repo ports.OffsetManagementRepositoryPort) *OffsetManagementFeature {
	if repo == nil {
		repo = adapters.NewMemoryOffsetManagementAdapter()
	}
	return &OffsetManagementFeature{
		Service: service.NewOffsetManagementService(repo),
	}
}

func (f *OffsetManagementFeature) CommitOffset(ctx context.Context, input contracts.CommitOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	return f.Service.CommitOffset(ctx, input)
}

func (f *OffsetManagementFeature) RewindOffset(ctx context.Context, input contracts.RewindOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	return f.Service.RewindOffset(ctx, input)
}

func (f *OffsetManagementFeature) FastForwardOffset(ctx context.Context, input contracts.FastForwardOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	return f.Service.FastForwardOffset(ctx, input)
}

func (f *OffsetManagementFeature) EvaluateLag(ctx context.Context, tenantID, groupID, topic string, partition int32, rate float64) (*contracts.LagEvaluationResult, error) {
	return f.Service.EvaluateLag(ctx, tenantID, groupID, topic, partition, rate)
}


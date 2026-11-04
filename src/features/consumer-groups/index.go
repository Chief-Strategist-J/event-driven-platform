package consumergroups

import (
	"context"
	"database/sql"

	contracts "event-driven-platform/src/features/consumer-groups/contracts"
	adapters "event-driven-platform/src/features/consumer-groups/infrastructure/adapters"
	ports "event-driven-platform/src/features/consumer-groups/infrastructure/ports"
	service "event-driven-platform/src/features/consumer-groups/service"
)

type ConsumerGroupsFeature struct {
	Service *service.ConsumerGroupsService
}

func NewConsumerGroupsFeatureWithPostgres(db *sql.DB) *ConsumerGroupsFeature {
	adapter := adapters.NewPostgresConsumerGroupsAdapter(db)
	svc := service.NewConsumerGroupsService(adapter)
	return &ConsumerGroupsFeature{
		Service: svc,
	}
}

func NewConsumerGroupsFeatureWithMemory() *ConsumerGroupsFeature {
	adapter := adapters.NewMemoryConsumerGroupsAdapter()
	svc := service.NewConsumerGroupsService(adapter)
	return &ConsumerGroupsFeature{
		Service: svc,
	}
}

func (f *ConsumerGroupsFeature) RegisterMember(ctx context.Context, input contracts.RegisterMemberInput) (*contracts.ConsumerGroupMemberRecord, error) {
	return f.Service.RegisterMember(ctx, input)
}

func (f *ConsumerGroupsFeature) ProcessHeartbeat(ctx context.Context, input contracts.HeartbeatInput) (*contracts.HeartbeatResult, error) {
	return f.Service.ProcessHeartbeat(ctx, input)
}

func (f *ConsumerGroupsFeature) SyncRebalance(ctx context.Context, input contracts.RebalanceSyncInput) (*contracts.RebalanceLogRecord, error) {
	return f.Service.SyncRebalance(ctx, input)
}

func (f *ConsumerGroupsFeature) ListMembers(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerGroupMemberRecord, error) {
	return f.Service.ListMembers(ctx, tenantID, groupID)
}

func (f *ConsumerGroupsFeature) GetRepository() ports.ConsumerGroupsRepositoryPort {
	return f.Service.GetRepository()
}

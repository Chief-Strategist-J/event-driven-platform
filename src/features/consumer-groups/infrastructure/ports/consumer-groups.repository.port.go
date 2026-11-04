package ports

import (
	"context"
	"time"

	"event-driven-platform/src/features/consumer-groups/contracts"
)

type ConsumerGroupsRepositoryPort interface {
	UpsertMember(ctx context.Context, member contracts.ConsumerGroupMemberRecord) (*contracts.ConsumerGroupMemberRecord, error)
	GetMember(ctx context.Context, tenantID, groupID, memberID string) (*contracts.ConsumerGroupMemberRecord, error)
	Heartbeat(ctx context.Context, tenantID, groupID, memberID string, now time.Time) (generationID int64, status contracts.MemberStatus, err error)
	UpdatePartitions(ctx context.Context, tenantID, groupID, memberID string, partitions []byte, generationID int64) error
	RecordRebalance(ctx context.Context, log contracts.RebalanceLogRecord) error
	ListMembers(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerGroupMemberRecord, error)
}


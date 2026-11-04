package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
)

type MemoryConsumerGroupsAdapter struct {
	mu        sync.RWMutex
	members   map[string]contracts.ConsumerGroupMemberRecord
	rebalances []contracts.RebalanceLogRecord
}

func NewMemoryConsumerGroupsAdapter() *MemoryConsumerGroupsAdapter {
	return &MemoryConsumerGroupsAdapter{
		members: make(map[string]contracts.ConsumerGroupMemberRecord),
	}
}

var _ ports.ConsumerGroupsRepositoryPort = (*MemoryConsumerGroupsAdapter)(nil)

func (m *MemoryConsumerGroupsAdapter) UpsertMember(ctx context.Context, member contracts.ConsumerGroupMemberRecord) (*contracts.ConsumerGroupMemberRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", member.TenantID, member.GroupID, member.MemberID)
	now := time.Now().UTC()
	if existing, exists := m.members[key]; exists {
		existing.ClientHost = member.ClientHost
		existing.InstanceID = member.InstanceID
		existing.AssignmentStrategy = member.AssignmentStrategy
		existing.HeartbeatAt = member.HeartbeatAt
		existing.Status = member.Status
		existing.UpdatedAt = now
		m.members[key] = existing
		return &existing, nil
	}

	member.CreatedAt = now
	member.UpdatedAt = now
	m.members[key] = member
	return &member, nil
}

func (m *MemoryConsumerGroupsAdapter) GetMember(ctx context.Context, tenantID, groupID, memberID string) (*contracts.ConsumerGroupMemberRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, groupID, memberID)
	rec, exists := m.members[key]
	if !exists {
		return nil, nil
	}
	return &rec, nil
}

func (m *MemoryConsumerGroupsAdapter) Heartbeat(ctx context.Context, tenantID, groupID, memberID string, now time.Time) (int64, contracts.MemberStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, groupID, memberID)
	rec, exists := m.members[key]
	if !exists {
		return 0, "", errors.NewNotFoundError("member not found")
	}

	rec.HeartbeatAt = now
	rec.Status = contracts.MemberStatusStable
	rec.UpdatedAt = now
	m.members[key] = rec
	return rec.GenerationID, rec.Status, nil
}

func (m *MemoryConsumerGroupsAdapter) UpdatePartitions(ctx context.Context, tenantID, groupID, memberID string, partitions []byte, generationID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", tenantID, groupID, memberID)
	rec, exists := m.members[key]
	if !exists {
		return errors.NewNotFoundError("member not found")
	}

	rec.AssignedPartitions = partitions
	rec.GenerationID = generationID
	rec.Status = contracts.MemberStatusStable
	rec.UpdatedAt = time.Now().UTC()
	m.members[key] = rec
	return nil
}

func (m *MemoryConsumerGroupsAdapter) RecordRebalance(ctx context.Context, l contracts.RebalanceLogRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	l.CreatedAt = time.Now().UTC()
	m.rebalances = append(m.rebalances, l)
	return nil
}

func (m *MemoryConsumerGroupsAdapter) ListMembers(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerGroupMemberRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []contracts.ConsumerGroupMemberRecord
	for _, rec := range m.members {
		if rec.TenantID == tenantID && rec.GroupID == groupID {
			result = append(result, rec)
		}
	}
	return result, nil
}

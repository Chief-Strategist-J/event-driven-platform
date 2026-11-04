package adapters

import (
	"context"
	"sync"
	"time"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/ports"
)

type MemoryConsumerRepositoryAdapter struct {
	mu          sync.Mutex
	inbox       map[string]contracts.ConsumerInboxRecord
	projections map[string]contracts.AggregateProjectionState
	offsets     map[string]int64
}

func NewMemoryConsumerRepositoryAdapter() ports.ConsumerRepositoryPort {
	return &MemoryConsumerRepositoryAdapter{
		inbox:       make(map[string]contracts.ConsumerInboxRecord),
		projections: make(map[string]contracts.AggregateProjectionState),
		offsets:     make(map[string]int64),
	}
}

func (m *MemoryConsumerRepositoryAdapter) RecordInbox(_ context.Context, record contracts.ConsumerInboxRecord) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := record.TenantID + ":" + record.HandlerID + ":" + record.EventID
	if _, exists := m.inbox[key]; exists {
		return false, nil
	}
	m.inbox[key] = record
	return true, nil
}

func (m *MemoryConsumerRepositoryAdapter) GetProjection(_ context.Context, tenantID, handlerID, aggregateType, aggregateID string) (*contracts.AggregateProjectionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	if proj, exists := m.projections[key]; exists {
		return &proj, nil
	}
	return nil, nil
}

func (m *MemoryConsumerRepositoryAdapter) ApplyProjectionEffect(_ context.Context, tenantID, handlerID, aggregateType, aggregateID string, version int64, eventID, payloadHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	m.projections[key] = contracts.AggregateProjectionState{
		TenantID:        tenantID,
		HandlerID:       handlerID,
		AggregateType:   aggregateType,
		AggregateID:     aggregateID,
		LastVersion:     version,
		LastEventID:     eventID,
		LastPayloadHash: payloadHash,
		IsParked:        false,
		UpdatedAt:       time.Now().UTC(),
	}
	return nil
}

func (m *MemoryConsumerRepositoryAdapter) ParkAggregate(_ context.Context, tenantID, handlerID, aggregateType, aggregateID string, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	existing := m.projections[key]
	existing.TenantID = tenantID
	existing.HandlerID = handlerID
	existing.AggregateType = aggregateType
	existing.AggregateID = aggregateID
	existing.IsParked = true
	existing.UpdatedAt = time.Now().UTC()
	m.projections[key] = existing
	return nil
}

func (m *MemoryConsumerRepositoryAdapter) RecordPartitionOffset(_ context.Context, topic string, partition int, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := topic + ":" + string(rune(partition))
	m.offsets[key] = offset
	return nil
}

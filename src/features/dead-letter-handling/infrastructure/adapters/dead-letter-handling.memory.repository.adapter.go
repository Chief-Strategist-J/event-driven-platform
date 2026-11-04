package adapters

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/ports"
)

type MemoryDeadLetterRepositoryAdapter struct {
	mu         sync.Mutex
	quarantine map[string]contracts.DeadLetterQuarantineRecord
	parking    map[string]contracts.AggregateParkingState
}

func NewMemoryDeadLetterRepositoryAdapter() ports.DeadLetterRepositoryPort {
	return &MemoryDeadLetterRepositoryAdapter{
		quarantine: make(map[string]contracts.DeadLetterQuarantineRecord),
		parking:    make(map[string]contracts.AggregateParkingState),
	}
}

func (m *MemoryDeadLetterRepositoryAdapter) Quarantine(_ context.Context, record contracts.DeadLetterQuarantineRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := record.TenantID + ":" + record.ID
	m.quarantine[key] = record
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) GetQuarantinedByID(_ context.Context, tenantID, id string) (*contracts.DeadLetterQuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + id
	if rec, exists := m.quarantine[key]; exists {
		return &rec, nil
	}
	return nil, nil
}

func (m *MemoryDeadLetterRepositoryAdapter) ListQuarantined(_ context.Context, tenantID, handlerID string, status contracts.QuarantineStatus, limit, offset int) ([]contracts.DeadLetterQuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var results []contracts.DeadLetterQuarantineRecord
	for _, rec := range m.quarantine {
		if rec.TenantID != tenantID {
			continue
		}
		if handlerID != "" && rec.HandlerID != handlerID {
			continue
		}
		if status != "" && rec.Status != status {
			continue
		}
		results = append(results, rec)
	}

	if offset >= len(results) {
		return []contracts.DeadLetterQuarantineRecord{}, nil
	}
	end := offset + limit
	if end > len(results) {
		end = len(results)
	}
	return results[offset:end], nil
}

func (m *MemoryDeadLetterRepositoryAdapter) UpdateDisposition(_ context.Context, tenantID, id string, status contracts.QuarantineStatus, redriveCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + id
	if rec, exists := m.quarantine[key]; exists {
		rec.Status = status
		rec.RedriveCount = redriveCount
		rec.UpdatedAt = time.Now().UTC()
		m.quarantine[key] = rec
	}
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) ParkKey(_ context.Context, park contracts.AggregateParkingState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := park.TenantID + ":" + park.HandlerID + ":" + park.AggregateType + ":" + park.AggregateID
	m.parking[key] = park
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) UnparkKey(_ context.Context, tenantID, handlerID, aggregateType, aggregateID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	if p, exists := m.parking[key]; exists {
		p.IsParked = false
		p.UpdatedAt = time.Now().UTC()
		m.parking[key] = p
	}
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) IsKeyParked(_ context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	if p, exists := m.parking[key]; exists {
		return p.IsParked, nil
	}
	return false, nil
}

func DeepCopyPayload(src []byte) []byte {
	var v any
	if err := json.Unmarshal(src, &v); err != nil {
		return src
	}
	b, _ := json.Marshal(v)
	return b
}


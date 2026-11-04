package adapters

import (
	"context"
	"sort"
	"sync"
	"time"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/ports"
)

type MemoryDeadLetterRepositoryAdapter struct {
	mu           sync.Mutex
	quarantine   map[string]contracts.DeadLetterQuarantineRecord
	parking      map[string]contracts.AggregateParkingState
	attempts     map[string]contracts.AttemptRecord
	parkedEvents map[string]contracts.ParkedEventRecord
	offsets      map[string]contracts.ConsumedOffsetRecord
}

func NewMemoryDeadLetterRepositoryAdapter() ports.DeadLetterRepositoryPort {
	return &MemoryDeadLetterRepositoryAdapter{
		quarantine:   make(map[string]contracts.DeadLetterQuarantineRecord),
		parking:      make(map[string]contracts.AggregateParkingState),
		attempts:     make(map[string]contracts.AttemptRecord),
		parkedEvents: make(map[string]contracts.ParkedEventRecord),
		offsets:      make(map[string]contracts.ConsumedOffsetRecord),
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

func (m *MemoryDeadLetterRepositoryAdapter) RecordAttempt(_ context.Context, attempt contracts.AttemptRecord) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := attempt.TenantID + ":" + attempt.ConsumerGroup + ":" + attempt.HandlerID + ":" + attempt.EventID
	if existing, exists := m.attempts[key]; exists {
		existing.AttemptCount++
		existing.LastAttemptAt = time.Now().UTC()
		existing.LastErrorFingerprint = attempt.LastErrorFingerprint
		existing.LastErrorClass = attempt.LastErrorClass
		m.attempts[key] = existing
		return existing.AttemptCount, nil
	}

	attempt.AttemptCount = 1
	attempt.FirstAttemptAt = time.Now().UTC()
	attempt.LastAttemptAt = time.Now().UTC()
	m.attempts[key] = attempt
	return 1, nil
}

func (m *MemoryDeadLetterRepositoryAdapter) DeleteAttempt(_ context.Context, tenantID, consumerGroup, handlerID, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + consumerGroup + ":" + handlerID + ":" + eventID
	delete(m.attempts, key)
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) ParkKey(_ context.Context, park contracts.AggregateParkingState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := park.TenantID + ":" + park.HandlerID + ":" + park.AggregateType + ":" + park.AggregateID
	m.parking[key] = park
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) UnparkKey(_ context.Context, tenantID, handlerID, aggregateType, aggregateID string, ownerEpoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	if p, exists := m.parking[key]; exists {
		if ownerEpoch > 0 && p.OwnerEpoch > ownerEpoch {
			return nil
		}
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

func (m *MemoryDeadLetterRepositoryAdapter) BufferParkedEvent(_ context.Context, event contracts.ParkedEventRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := event.TenantID + ":" + event.ConsumerGroup + ":" + event.HandlerID + ":" + event.AggregateType + ":" + event.AggregateID + ":" + string(rune(event.AggregateVersion))
	m.parkedEvents[key] = event

	parkKey := event.TenantID + ":" + event.HandlerID + ":" + event.AggregateType + ":" + event.AggregateID
	if p, exists := m.parking[parkKey]; exists {
		p.ParkedEventCount++
		p.UpdatedAt = time.Now().UTC()
		m.parking[parkKey] = p
	}
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) GetParkedEventsForDrain(_ context.Context, tenantID, consumerGroup, handlerID, aggregateType, aggregateID string) ([]contracts.ParkedEventRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var events []contracts.ParkedEventRecord
	prefix := tenantID + ":" + consumerGroup + ":" + handlerID + ":" + aggregateType + ":" + aggregateID + ":"
	for k, v := range m.parkedEvents {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			events = append(events, v)
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].AggregateVersion < events[j].AggregateVersion
	})

	return events, nil
}

func (m *MemoryDeadLetterRepositoryAdapter) DeleteParkedEvent(_ context.Context, tenantID, consumerGroup, handlerID, aggregateType, aggregateID string, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + consumerGroup + ":" + handlerID + ":" + aggregateType + ":" + aggregateID + ":" + string(rune(version))
	delete(m.parkedEvents, key)

	parkKey := tenantID + ":" + handlerID + ":" + aggregateType + ":" + aggregateID
	if p, exists := m.parking[parkKey]; exists {
		if p.ParkedEventCount > 0 {
			p.ParkedEventCount--
		}
		p.UpdatedAt = time.Now().UTC()
		m.parking[parkKey] = p
	}
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) StoreConsumedOffset(_ context.Context, offset contracts.ConsumedOffsetRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := offset.TenantID + ":" + offset.ConsumerGroup + ":" + offset.Topic + ":" + string(rune(offset.Partition))
	m.offsets[key] = offset
	return nil
}

func (m *MemoryDeadLetterRepositoryAdapter) GetConsumedOffset(_ context.Context, tenantID, consumerGroup, topic string, partition int) (*contracts.ConsumedOffsetRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + consumerGroup + ":" + topic + ":" + string(rune(partition))
	if off, exists := m.offsets[key]; exists {
		return &off, nil
	}
	return nil, nil
}

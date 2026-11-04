package adapters

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
	"event-driven-platform/src/features/event-ordering-protection/infrastructure/ports"
	"event-driven-platform/src/features/event-ordering-protection/domain/rules"
)

type MemoryEventOrderingAdapter struct {
	mu         sync.RWMutex
	trackers   map[string]contracts.AggregateSequenceTrackerRecord
	heldEvents map[string][]contracts.SequenceHeldEventRecord
}

func NewMemoryEventOrderingAdapter() *MemoryEventOrderingAdapter {
	return &MemoryEventOrderingAdapter{
		trackers:   make(map[string]contracts.AggregateSequenceTrackerRecord),
		heldEvents: make(map[string][]contracts.SequenceHeldEventRecord),
	}
}

var _ ports.EventOrderingRepositoryPort = (*MemoryEventOrderingAdapter)(nil)

func (m *MemoryEventOrderingAdapter) makeKey(tenantID, group, handler, aggType, aggID string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", tenantID, group, handler, aggType, aggID)
}

func (m *MemoryEventOrderingAdapter) GetSequenceTracker(_ context.Context, tenantID, group, handler, aggType, aggID string) (*contracts.AggregateSequenceTrackerRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := m.makeKey(tenantID, group, handler, aggType, aggID)
	rec, exists := m.trackers[key]
	if !exists {
		return nil, nil
	}
	return &rec, nil
}

func (m *MemoryEventOrderingAdapter) ApplyEventSequence(_ context.Context, r contracts.AggregateSequenceTrackerRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.makeKey(r.TenantID, r.ConsumerGroup, r.HandlerID, r.AggregateType, r.AggregateID)
	curr, exists := m.trackers[key]
	if exists && curr.OwnerEpoch > r.OwnerEpoch {
		return rules.ErrFencedEpoch
	}
	r.UpdatedAt = time.Now()
	m.trackers[key] = r
	return nil
}

func (m *MemoryEventOrderingAdapter) HoldAggregateKey(_ context.Context, r contracts.AggregateSequenceTrackerRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.makeKey(r.TenantID, r.ConsumerGroup, r.HandlerID, r.AggregateType, r.AggregateID)
	curr, exists := m.trackers[key]
	if exists && curr.OwnerEpoch > r.OwnerEpoch {
		return rules.ErrFencedEpoch
	}
	r.UpdatedAt = time.Now()
	m.trackers[key] = r
	return nil
}

func (m *MemoryEventOrderingAdapter) StoreHeldEvent(_ context.Context, r contracts.SequenceHeldEventRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.makeKey(r.TenantID, r.ConsumerGroup, r.HandlerID, r.AggregateType, r.AggregateID)
	list := m.heldEvents[key]
	for _, item := range list {
		if item.AggregateVersion == r.AggregateVersion {
			return nil
		}
	}
	r.ArrivedAt = time.Now()
	list = append(list, r)
	sort.Slice(list, func(i, j int) bool {
		return list[i].AggregateVersion < list[j].AggregateVersion
	})
	m.heldEvents[key] = list
	return nil
}

func (m *MemoryEventOrderingAdapter) GetHeldEventsForDrain(_ context.Context, tenantID, group, handler, aggType, aggID string) ([]contracts.SequenceHeldEventRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := m.makeKey(tenantID, group, handler, aggType, aggID)
	list := m.heldEvents[key]
	copied := make([]contracts.SequenceHeldEventRecord, len(list))
	copy(copied, list)
	return copied, nil
}

func (m *MemoryEventOrderingAdapter) DeleteHeldEvent(_ context.Context, tenantID, group, handler, aggType, aggID string, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.makeKey(tenantID, group, handler, aggType, aggID)
	list := m.heldEvents[key]
	filtered := make([]contracts.SequenceHeldEventRecord, 0, len(list))
	for _, item := range list {
		if item.AggregateVersion != version {
			filtered = append(filtered, item)
		}
	}
	m.heldEvents[key] = filtered
	return nil
}

func (m *MemoryEventOrderingAdapter) ListHeldTrackers(_ context.Context, tenantID, group, handler string, limit, offset int) ([]contracts.AggregateSequenceTrackerRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []contracts.AggregateSequenceTrackerRecord
	for _, rec := range m.trackers {
		if rec.TenantID == tenantID && rec.IsHeld {
			if group != "" && rec.ConsumerGroup != group {
				continue
			}
			if handler != "" && rec.HandlerID != handler {
				continue
			}
			res = append(res, rec)
		}
	}
	if offset >= len(res) {
		return []contracts.AggregateSequenceTrackerRecord{}, nil
	}
	end := offset + limit
	if end > len(res) {
		end = len(res)
	}
	return res[offset:end], nil
}


package adapters

import (
	"context"
	"fmt"
	"sync"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/infrastructure/ports"
)

type MemoryEventVersioningAdapter struct {
	mu        sync.RWMutex
	schemas   map[string]contracts.SchemaVersionRecord
	upcasters map[string]contracts.UpcasterRecord
}

func NewMemoryEventVersioningAdapter() *MemoryEventVersioningAdapter {
	return &MemoryEventVersioningAdapter{
		schemas:   make(map[string]contracts.SchemaVersionRecord),
		upcasters: make(map[string]contracts.UpcasterRecord),
	}
}

var _ ports.EventVersioningRepositoryPort = (*MemoryEventVersioningAdapter)(nil)

func (m *MemoryEventVersioningAdapter) SaveSchemaVersion(_ context.Context, record contracts.SchemaVersionRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%s:%s:%d", record.TenantID, record.StreamName, record.EventType, record.SchemaVersion)
	m.schemas[key] = record
	return nil
}

func (m *MemoryEventVersioningAdapter) GetLatestSchemaVersion(_ context.Context, tenantID, streamName, eventType string) (*contracts.SchemaVersionRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var latest *contracts.SchemaVersionRecord
	prefix := fmt.Sprintf("%s:%s:%s:", tenantID, streamName, eventType)
	for k, v := range m.schemas {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			record := v
			if latest == nil || record.SchemaVersion > latest.SchemaVersion {
				latest = &record
			}
		}
	}
	return latest, nil
}

func (m *MemoryEventVersioningAdapter) GetSchemaVersion(_ context.Context, tenantID, streamName, eventType string, version int) (*contracts.SchemaVersionRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := fmt.Sprintf("%s:%s:%s:%d", tenantID, streamName, eventType, version)
	record, exists := m.schemas[key]
	if !exists {
		return nil, nil
	}
	return &record, nil
}

func (m *MemoryEventVersioningAdapter) ListSchemaVersions(_ context.Context, tenantID, streamName, eventType string) ([]contracts.SchemaVersionRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var list []contracts.SchemaVersionRecord
	prefix := fmt.Sprintf("%s:%s:%s:", tenantID, streamName, eventType)
	for k, v := range m.schemas {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			list = append(list, v)
		}
	}
	return list, nil
}

func (m *MemoryEventVersioningAdapter) SaveUpcaster(_ context.Context, record contracts.UpcasterRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%s:%d:%d", record.TenantID, record.EventType, record.FromVersion, record.ToVersion)
	m.upcasters[key] = record
	return nil
}

func (m *MemoryEventVersioningAdapter) GetUpcasterChain(_ context.Context, tenantID, eventType string, fromVersion, toVersion int) ([]contracts.UpcasterRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var chain []contracts.UpcasterRecord
	curr := fromVersion
	for curr < toVersion {
		key := fmt.Sprintf("%s:%s:%d:%d", tenantID, eventType, curr, curr+1)
		rec, exists := m.upcasters[key]
		if !exists || !rec.IsActive {
			break
		}
		chain = append(chain, rec)
		curr++
	}
	return chain, nil
}

package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"event-driven-platform/src/features/backpressure/contracts"
	"event-driven-platform/src/features/backpressure/infrastructure/ports"
)

type BackpressureMemoryRepositoryAdapter struct {
	mu      sync.RWMutex
	records map[string]*contracts.BackpressureRecord
	audits  []contracts.BackpressureAuditRecord
}

func NewBackpressureMemoryRepositoryAdapter() ports.BackpressureRepositoryPort {
	return &BackpressureMemoryRepositoryAdapter{
		records: make(map[string]*contracts.BackpressureRecord),
		audits:  make([]contracts.BackpressureAuditRecord, 0),
	}
}

func memKey(tenantID, handlerID, dependencyID string) string {
	return fmt.Sprintf("%s::%s::%s", tenantID, handlerID, dependencyID)
}

func (a *BackpressureMemoryRepositoryAdapter) UpsertBackpressureRecord(ctx context.Context, record contracts.BackpressureRecord) (*contracts.BackpressureRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now().UTC()
	key := memKey(record.TenantID, record.HandlerID, record.DependencyID)
	if existing, ok := a.records[key]; ok {
		existing.State = record.State
		existing.PauseReason = record.PauseReason
		existing.ConcurrencyLimit = record.ConcurrencyLimit
		existing.PausedAt = record.PausedAt
		existing.ResumedAt = record.ResumedAt
		existing.UpdatedAt = now
		copy := *existing
		return &copy, nil
	}
	record.CreatedAt = now
	record.UpdatedAt = now
	clone := record
	a.records[key] = &clone
	return &clone, nil
}

func (a *BackpressureMemoryRepositoryAdapter) GetBackpressureRecord(ctx context.Context, tenantID, handlerID, dependencyID string) (*contracts.BackpressureRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := memKey(tenantID, handlerID, dependencyID)
	rec, ok := a.records[key]
	if !ok {
		return nil, fmt.Errorf("backpressure record not found: %s", key)
	}
	clone := *rec
	return &clone, nil
}

func (a *BackpressureMemoryRepositoryAdapter) ListBackpressureByTenant(ctx context.Context, tenantID string) ([]contracts.BackpressureRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]contracts.BackpressureRecord, 0)
	for _, rec := range a.records {
		if rec.TenantID == tenantID {
			result = append(result, *rec)
		}
	}
	return result, nil
}

func (a *BackpressureMemoryRepositoryAdapter) RecordAuditLog(ctx context.Context, audit contracts.BackpressureAuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	audit.CreatedAt = time.Now().UTC()
	a.audits = append(a.audits, audit)
	return nil
}


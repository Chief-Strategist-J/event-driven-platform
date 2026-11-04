package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
)

type MemoryRetryTopicsAdapter struct {
	mu      sync.RWMutex
	records map[string]contracts.RetryAttemptLedgerRecord
}

func NewMemoryRetryTopicsAdapter() *MemoryRetryTopicsAdapter {
	return &MemoryRetryTopicsAdapter{
		records: make(map[string]contracts.RetryAttemptLedgerRecord),
	}
}

var _ ports.RetryTopicsRepositoryPort = (*MemoryRetryTopicsAdapter)(nil)

func (m *MemoryRetryTopicsAdapter) RecordRetryAttempt(ctx context.Context, rec contracts.RetryAttemptLedgerRecord) (*contracts.RetryAttemptLedgerRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	uniqueKey := fmt.Sprintf("%s:%s:%s:%s:%d", rec.TenantID, rec.ConsumerGroup, rec.HandlerID, rec.EventID, rec.AttemptNumber)
	for _, existing := range m.records {
		checkKey := fmt.Sprintf("%s:%s:%s:%s:%d", existing.TenantID, existing.ConsumerGroup, existing.HandlerID, existing.EventID, existing.AttemptNumber)
		if checkKey == uniqueKey {
			return nil, errors.NewConflictError("retry attempt already exists for event")
		}
	}

	now := time.Now().UTC()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	key := fmt.Sprintf("%s:%s", rec.TenantID, rec.ID)
	m.records[key] = rec
	return &rec, nil
}

func (m *MemoryRetryTopicsAdapter) GetDueRetryAttempts(ctx context.Context, tenantID string, asOf time.Time, limit int) ([]contracts.RetryAttemptLedgerRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []contracts.RetryAttemptLedgerRecord
	for _, rec := range m.records {
		if rec.TenantID == tenantID && rec.Status == contracts.StatusScheduled {
			if !rec.DueAt.After(asOf) {
				result = append(result, rec)
				if limit > 0 && len(result) >= limit {
					break
				}
			}
		}
	}
	return result, nil
}

func (m *MemoryRetryTopicsAdapter) UpdateRetryStatus(ctx context.Context, tenantID, id string, status contracts.RetryAttemptStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", tenantID, id)
	rec, exists := m.records[key]
	if !exists {
		return errors.NewNotFoundError("retry attempt not found")
	}

	rec.Status = status
	rec.UpdatedAt = time.Now().UTC()
	m.records[key] = rec
	return nil
}

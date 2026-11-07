package adapters

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/ports"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
)

var ErrRecordNotFound = errors.New("outbox record not found")
var ErrWorkerOwnershipLost = errors.New("outbox record worker ownership lost or lease expired")

type MemoryOutboxRepositoryAdapter struct {
	mu            sync.Mutex
	records       map[string]contracts.OutboxRecord
	idempotencies map[string]string
}

func NewMemoryOutboxRepositoryAdapter() ports.OutboxRepositoryPort {
	return &MemoryOutboxRepositoryAdapter{
		records:       make(map[string]contracts.OutboxRecord),
		idempotencies: make(map[string]string),
	}
}

func (m *MemoryOutboxRepositoryAdapter) ClaimIdempotency(_ context.Context, tenantID, idempotencyKey, recordID string) (bool, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + idempotencyKey
	if existingID, exists := m.idempotencies[key]; exists {
		return false, existingID, nil
	}
	m.idempotencies[key] = recordID
	return true, recordID, nil
}

func (m *MemoryOutboxRepositoryAdapter) Save(_ context.Context, record contracts.OutboxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.records {
		if existing.TenantID == record.TenantID && existing.SourceService == record.SourceService && existing.ExternalID == record.ExternalID {
			return appErrors.NewConflictError("an outbox event with the same externalId already exists for this tenant and source service")
		}
	}
	key := record.TenantID + ":" + record.ID
	m.records[key] = record
	return nil
}

func (m *MemoryOutboxRepositoryAdapter) SaveAndImmediateDelete(_ context.Context, record contracts.OutboxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

func (m *MemoryOutboxRepositoryAdapter) GetByID(_ context.Context, tenantID string, id string) (*contracts.OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + ":" + id
	rec, ok := m.records[key]
	if !ok {
		return nil, ErrRecordNotFound
	}
	return &rec, nil
}

func (m *MemoryOutboxRepositoryAdapter) FetchPending(_ context.Context, limit int) ([]contracts.OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	parkedAggregates := make(map[string]bool)
	for _, rec := range m.records {
		if rec.Status == contracts.StatusParked {
			aggKey := rec.TenantID + ":" + rec.AggregateType + ":" + rec.AggregateID
			parkedAggregates[aggKey] = true
		}
	}

	var pending []contracts.OutboxRecord
	for _, rec := range m.records {
		if rec.Status == contracts.StatusPending {
			aggKey := rec.TenantID + ":" + rec.AggregateType + ":" + rec.AggregateID
			if parkedAggregates[aggKey] {
				continue
			}
			pending = append(pending, rec)
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		if pending[i].AggregateVersion != pending[j].AggregateVersion {
			return pending[i].AggregateVersion < pending[j].AggregateVersion
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})

	if len(pending) > limit {
		pending = pending[:limit]
	}
	return pending, nil
}

func (m *MemoryOutboxRepositoryAdapter) ClaimInFlight(_ context.Context, workerID string, leaseDuration time.Duration, limit int) ([]contracts.OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	parkedAggregates := make(map[string]bool)
	for _, rec := range m.records {
		if rec.Status == contracts.StatusParked {
			aggKey := rec.TenantID + ":" + rec.AggregateType + ":" + rec.AggregateID
			parkedAggregates[aggKey] = true
		}
	}

	var eligible []contracts.OutboxRecord
	for _, rec := range m.records {
		aggKey := rec.TenantID + ":" + rec.AggregateType + ":" + rec.AggregateID
		if parkedAggregates[aggKey] {
			continue
		}

		canClaim := false
		if rec.Status == contracts.StatusPending {
			canClaim = true
		} else if rec.Status == contracts.StatusInFlight && rec.LeaseExpiresAt != nil && rec.LeaseExpiresAt.Before(now) {
			canClaim = true
		}

		if canClaim {
			eligible = append(eligible, rec)
		}
	}

	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].AggregateVersion != eligible[j].AggregateVersion {
			return eligible[i].AggregateVersion < eligible[j].AggregateVersion
		}
		return eligible[i].CreatedAt.Before(eligible[j].CreatedAt)
	})

	if len(eligible) > limit {
		eligible = eligible[:limit]
	}

	leaseExpiry := now.Add(leaseDuration)
	for _, item := range eligible {
		key := item.TenantID + ":" + item.ID
		rec := m.records[key]
		rec.Status = contracts.StatusInFlight
		rec.WorkerID = workerID
		rec.LeaseExpiresAt = &leaseExpiry
		rec.UpdatedAt = now
		m.records[key] = rec
	}

	return eligible, nil
}

func (m *MemoryOutboxRepositoryAdapter) MarkPublished(_ context.Context, tenantID string, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + id
	rec, ok := m.records[key]
	if !ok {
		return ErrRecordNotFound
	}
	now := time.Now().UTC()
	rec.Status = contracts.StatusPublished
	rec.ProcessedAt = &now
	rec.UpdatedAt = now
	m.records[key] = rec
	return nil
}

func (m *MemoryOutboxRepositoryAdapter) MarkPublishedByWorker(_ context.Context, tenantID string, id string, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + id
	rec, ok := m.records[key]
	if !ok {
		return ErrRecordNotFound
	}

	now := time.Now().UTC()
	if rec.WorkerID != workerID || (rec.LeaseExpiresAt != nil && rec.LeaseExpiresAt.Before(now)) {
		return ErrWorkerOwnershipLost
	}

	rec.Status = contracts.StatusPublished
	rec.ProcessedAt = &now
	rec.UpdatedAt = now
	m.records[key] = rec
	return nil
}

func (m *MemoryOutboxRepositoryAdapter) MarkFailed(_ context.Context, tenantID string, id string, errMessage string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantID + ":" + id
	rec, ok := m.records[key]
	if !ok {
		return ErrRecordNotFound
	}
	rec.RetryCount++
	rec.LastError = errMessage
	rec.UpdatedAt = time.Now().UTC()
	if rec.RetryCount >= constants.MaxWorkerRetries {
		rec.Status = contracts.StatusParked
	} else {
		rec.Status = contracts.StatusPending
	}
	m.records[key] = rec
	return nil
}

func (m *MemoryOutboxRepositoryAdapter) PurgeExpired(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().UTC().Add(-constants.PurgeIntervalHours * time.Hour)
	for key, rec := range m.records {
		if rec.Status == contracts.StatusPublished && rec.CreatedAt.Before(cutoff) {
			delete(m.records, key)
		}
	}
	return nil
}

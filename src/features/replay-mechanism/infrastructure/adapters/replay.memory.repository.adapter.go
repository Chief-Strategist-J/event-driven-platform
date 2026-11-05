package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"event-driven-platform/src/features/replay-mechanism/contracts"
	"event-driven-platform/src/features/replay-mechanism/infrastructure/ports"
)

type ReplayMemoryRepositoryAdapter struct {
	mu      sync.RWMutex
	runs    map[string]*contracts.ReplayRunRecord
	effects []contracts.ReplayEffectRecord
	audits  []contracts.ReplayAuditRecord
}

func NewReplayMemoryRepositoryAdapter() ports.ReplayRepositoryPort {
	return &ReplayMemoryRepositoryAdapter{
		runs:    make(map[string]*contracts.ReplayRunRecord),
		effects: make([]contracts.ReplayEffectRecord, 0),
		audits:  make([]contracts.ReplayAuditRecord, 0),
	}
}

func replayMemKey(tenantID, runID string) string {
	return fmt.Sprintf("%s::%s", tenantID, runID)
}

func (a *ReplayMemoryRepositoryAdapter) CreateReplayRun(ctx context.Context, run contracts.ReplayRunRecord) (*contracts.ReplayRunRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now().UTC()
	run.CreatedAt = now
	run.UpdatedAt = now
	if run.Version == 0 {
		run.Version = 1
	}
	clone := run
	a.runs[replayMemKey(run.TenantID, run.ID)] = &clone
	return &clone, nil
}

func (a *ReplayMemoryRepositoryAdapter) UpdateReplayRunState(ctx context.Context, tenantID, runID string, state contracts.ReplayState) (*contracts.ReplayRunRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := replayMemKey(tenantID, runID)
	rec, ok := a.runs[key]
	if !ok {
		return nil, fmt.Errorf("replay run not found: %s", key)
	}
	rec.State = state
	rec.Version++
	rec.UpdatedAt = time.Now().UTC()
	clone := *rec
	return &clone, nil
}

func (a *ReplayMemoryRepositoryAdapter) UpdateReplayRunOffset(ctx context.Context, tenantID, runID string, currentOffset int64, state contracts.ReplayState) (*contracts.ReplayRunRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := replayMemKey(tenantID, runID)
	rec, ok := a.runs[key]
	if !ok {
		return nil, fmt.Errorf("replay run not found: %s", key)
	}
	rec.CurrentOffset = currentOffset
	rec.State = state
	rec.Version++
	rec.UpdatedAt = time.Now().UTC()
	clone := *rec
	return &clone, nil
}

func (a *ReplayMemoryRepositoryAdapter) GetReplayRun(ctx context.Context, tenantID, runID string) (*contracts.ReplayRunRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	key := replayMemKey(tenantID, runID)
	rec, ok := a.runs[key]
	if !ok {
		return nil, fmt.Errorf("replay run not found: %s", key)
	}
	clone := *rec
	return &clone, nil
}

func (a *ReplayMemoryRepositoryAdapter) ListReplayRunsByTenant(ctx context.Context, tenantID string) ([]contracts.ReplayRunRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	res := make([]contracts.ReplayRunRecord, 0)
	for _, rec := range a.runs {
		if rec.TenantID == tenantID {
			res = append(res, *rec)
		}
	}
	return res, nil
}

func (a *ReplayMemoryRepositoryAdapter) RecordEffectLog(ctx context.Context, effect contracts.ReplayEffectRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	effect.CreatedAt = time.Now().UTC()
	a.effects = append(a.effects, effect)
	return nil
}

func (a *ReplayMemoryRepositoryAdapter) RecordAuditLog(ctx context.Context, audit contracts.ReplayAuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	audit.CreatedAt = time.Now().UTC()
	a.audits = append(a.audits, audit)
	return nil
}


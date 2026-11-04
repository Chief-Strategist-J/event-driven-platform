package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/shared/errors"
)

type MemoryOffsetManagementAdapter struct {
	mu      sync.RWMutex
	offsets map[string]contracts.ConsumerStoredOffsetRecord
	logs    []contracts.ConsumerOffsetAuditLogRecord
}

func NewMemoryOffsetManagementAdapter() *MemoryOffsetManagementAdapter {
	return &MemoryOffsetManagementAdapter{
		offsets: make(map[string]contracts.ConsumerStoredOffsetRecord),
		logs:    make([]contracts.ConsumerOffsetAuditLogRecord, 0),
	}
}

func (a *MemoryOffsetManagementAdapter) UpsertStoredOffset(ctx context.Context, record contracts.ConsumerStoredOffsetRecord) (*contracts.ConsumerStoredOffsetRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s:%d", record.TenantID, record.GroupID, record.Topic, record.Partition)
	existing, exists := a.offsets[key]
	if exists {
		record.CommitEpoch = existing.CommitEpoch + 1
		record.CreatedAt = existing.CreatedAt
	} else {
		record.CommitEpoch = 1
		record.CreatedAt = time.Now().UTC()
	}
	record.UpdatedAt = time.Now().UTC()
	a.offsets[key] = record

	return &record, nil
}

func (a *MemoryOffsetManagementAdapter) GetStoredOffset(ctx context.Context, tenantID, groupID, topic string, partition int32) (*contracts.ConsumerStoredOffsetRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := fmt.Sprintf("%s:%s:%s:%d", tenantID, groupID, topic, partition)
	rec, exists := a.offsets[key]
	if !exists {
		return nil, errors.NewNotFoundError("stored offset not found for partition")
	}
	return &rec, nil
}

func (a *MemoryOffsetManagementAdapter) ListStoredOffsets(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerStoredOffsetRecord, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var result []contracts.ConsumerStoredOffsetRecord
	for _, rec := range a.offsets {
		if rec.TenantID == tenantID && rec.GroupID == groupID {
			result = append(result, rec)
		}
	}
	return result, nil
}

func (a *MemoryOffsetManagementAdapter) RecordAuditLog(ctx context.Context, log contracts.ConsumerOffsetAuditLogRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	log.CreatedAt = time.Now().UTC()
	a.logs = append(a.logs, log)
	return nil
}

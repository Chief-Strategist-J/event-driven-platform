package ports

import (
	"context"

	"event-driven-platform/src/features/offset-management/contracts"
)

type OffsetManagementRepositoryPort interface {
	UpsertStoredOffset(ctx context.Context, record contracts.ConsumerStoredOffsetRecord) (*contracts.ConsumerStoredOffsetRecord, error)
	GetStoredOffset(ctx context.Context, tenantID, groupID, topic string, partition int32) (*contracts.ConsumerStoredOffsetRecord, error)
	ListStoredOffsets(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerStoredOffsetRecord, error)
	RecordAuditLog(ctx context.Context, log contracts.ConsumerOffsetAuditLogRecord) error
}


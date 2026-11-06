package ports

import (
	"context"

	"event-driven-platform/src/features/audit-trail/contracts"
)

type AuditTrailRepositoryPort interface {
	GetLatestRecord(ctx context.Context, tenantID string, shardID string) (*contracts.AuditRecord, error)
	AppendAuditRecord(ctx context.Context, record contracts.AuditRecord) error
	GetShardRecordsRange(ctx context.Context, in contracts.VerifyShardIntegrityInput, startSeq int64, endSeq int64) ([]contracts.AuditRecord, error)
	GetLatestCheckpoint(ctx context.Context, tenantID string, shardID string) (*contracts.CheckpointRecord, error)
	AppendCheckpoint(ctx context.Context, checkpoint contracts.CheckpointRecord) error
	RecordAccessLog(ctx context.Context, log contracts.AccessLogRecord) error
	QueryAuditRecords(ctx context.Context, in contracts.QueryAuditInput) ([]contracts.AuditRecord, error)
}

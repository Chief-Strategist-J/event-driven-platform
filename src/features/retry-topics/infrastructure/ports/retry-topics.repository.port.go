package ports

import (
	"context"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
)

type RetryTopicsRepositoryPort interface {
	RecordRetryAttempt(ctx context.Context, rec contracts.RetryAttemptLedgerRecord) (*contracts.RetryAttemptLedgerRecord, error)
	GetDueRetryAttempts(ctx context.Context, tenantID string, asOf time.Time, limit int) ([]contracts.RetryAttemptLedgerRecord, error)
	UpdateRetryStatus(ctx context.Context, tenantID, id string, status contracts.RetryAttemptStatus) error
}


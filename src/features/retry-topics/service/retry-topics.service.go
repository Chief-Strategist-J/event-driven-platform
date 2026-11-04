package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/domain/machines"
	"event-driven-platform/src/features/retry-topics/domain/rules"
	"event-driven-platform/src/features/retry-topics/infrastructure/ports"
	"event-driven-platform/src/shared/utils"
)

type RetryTopicsService struct {
	repo ports.RetryTopicsRepositoryPort
}

func NewRetryTopicsService(repo ports.RetryTopicsRepositoryPort) *RetryTopicsService {
	return &RetryTopicsService{repo: repo}
}

func (s *RetryTopicsService) GetRepository() ports.RetryTopicsRepositoryPort {
	return s.repo
}

func (s *RetryTopicsService) EvaluateRetry(ctx context.Context, input contracts.EvaluateRetryInput) (*contracts.EvaluateRetryResult, error) {
	if err := contracts.ValidateEvaluateRetry(input); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	result := rules.EvaluateRetryDecision(input, now)
	return &result, nil
}

func (s *RetryTopicsService) ScheduleRetryAttempt(ctx context.Context, input contracts.ScheduleRetryInput) (*contracts.RetryAttemptLedgerRecord, error) {
	if err := contracts.ValidateScheduleRetry(input); err != nil {
		return nil, err
	}

	id, _ := utils.GenerateUUIDv7()
	publicID, _ := utils.GeneratePublicID("try")

	corrID := input.CorrelationID
	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	} else {
		if _, err := uuid.Parse(corrID); err != nil {
			corrID, _ = utils.GenerateUUIDv7()
		}
	}

	idempotencyKey := rules.DeriveDeterministicIdempotencyKey(input.HandlerID, input.EventID, "process")

	rec := contracts.RetryAttemptLedgerRecord{
		ID:                    id,
		PublicID:              publicID,
		TenantID:              input.TenantID,
		SourceService:         input.SourceService,
		ExternalID:            input.ExternalID,
		CorrelationID:         corrID,
		ConsumerGroup:         input.ConsumerGroup,
		HandlerID:             input.HandlerID,
		EventID:               input.EventID,
		AggregateType:         input.AggregateType,
		AggregateID:           input.AggregateID,
		AggregateVersion:      input.AggregateVersion,
		AttemptNumber:         input.AttemptNumber,
		MaxAttempts:           input.MaxAttempts,
		BackoffMs:             input.BackoffMs,
		DueAt:                 input.DueAt,
		Status:                contracts.StatusScheduled,
		FailureClassification: input.Classification,
		IdempotencyKey:        idempotencyKey,
		ErrorMessage:          input.ErrorMessage,
		ErrorCode:             input.ErrorCode,
		ValidUntil:            input.ValidUntil,
	}

	created, err := s.repo.RecordRetryAttempt(ctx, rec)
	if err != nil {
		return nil, fmt.Errorf("failed recording retry attempt: %w", err)
	}
	return created, nil
}

func (s *RetryTopicsService) GetDueRetries(ctx context.Context, tenantID string, limit int) ([]contracts.RetryAttemptLedgerRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	now := time.Now().UTC()
	return s.repo.GetDueRetryAttempts(ctx, tenantID, now, limit)
}

func (s *RetryTopicsService) MarkAttemptStatus(ctx context.Context, tenantID, id string, currentStatus, newStatus contracts.RetryAttemptStatus) error {
	next, err := machines.TransitionRetryStatus(currentStatus, newStatus)
	if err != nil {
		return err
	}
	return s.repo.UpdateRetryStatus(ctx, tenantID, id, next)
}


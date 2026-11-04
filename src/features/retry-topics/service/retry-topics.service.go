package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/domain/machines"
	"event-driven-platform/src/features/retry-topics/domain/rules"
	"event-driven-platform/src/features/retry-topics/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
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
	ctx, span := observability.StartSpan(ctx, "retry-topics.evaluate_retry")
	defer span.End()

	span.SetAttributes(
		attribute.String("stream_ordering", string(input.StreamOrdering)),
		attribute.String("classification", string(input.Classification)),
		attribute.Int("attempt_number", input.AttemptNumber),
		attribute.Int("max_attempts", input.MaxAttempts),
	)

	if err := contracts.ValidateEvaluateRetry(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "retry-topics", "validation rejected retry evaluation", "", map[string]any{"error": err.Error()})
		return nil, err
	}
	now := time.Now().UTC()
	result := rules.EvaluateRetryDecision(input, now)

	span.SetAttributes(
		attribute.String("retry.disposition", string(result.Disposition)),
		attribute.Int64("retry.backoff_ms", result.BackoffMs),
	)

	return &result, nil
}

func (s *RetryTopicsService) ScheduleRetryAttempt(ctx context.Context, input contracts.ScheduleRetryInput) (*contracts.RetryAttemptLedgerRecord, error) {
	ctx, span := observability.StartSpan(ctx, "retry-topics.schedule_retry_attempt")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.HandlerID),
		attribute.String("event_id", input.EventID),
		attribute.String("aggregate_id", input.AggregateID),
		attribute.Int("attempt_number", input.AttemptNumber),
	)

	if err := contracts.ValidateScheduleRetry(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "retry-topics", "validation rejected retry scheduling", input.TenantID, map[string]any{"error": err.Error()})
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
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "retry-topics", "failed recording retry attempt in repository", input.TenantID, map[string]any{"error": err.Error()})
		return nil, fmt.Errorf("failed recording retry attempt: %w", err)
	}

	span.SetAttributes(
		attribute.String("retry.public_id", created.PublicID),
		attribute.String("retry.status", string(created.Status)),
	)

	return created, nil
}

func (s *RetryTopicsService) GetDueRetries(ctx context.Context, tenantID string, limit int) ([]contracts.RetryAttemptLedgerRecord, error) {
	ctx, span := observability.StartSpan(ctx, "retry-topics.get_due_retries")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.Int("limit", limit),
	)

	if limit <= 0 {
		limit = 50
	}
	now := time.Now().UTC()
	records, err := s.repo.GetDueRetryAttempts(ctx, tenantID, now, limit)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "retry-topics", "failed getting due retries from repository", tenantID, map[string]any{"error": err.Error()})
		return nil, err
	}

	span.SetAttributes(attribute.Int("records_count", len(records)))
	return records, nil
}

func (s *RetryTopicsService) MarkAttemptStatus(ctx context.Context, tenantID, id string, currentStatus, newStatus contracts.RetryAttemptStatus) error {
	ctx, span := observability.StartSpan(ctx, "retry-topics.mark_attempt_status")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("attempt_id", id),
		attribute.String("current_status", string(currentStatus)),
		attribute.String("new_status", string(newStatus)),
	)

	next, err := machines.TransitionRetryStatus(currentStatus, newStatus)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "INVALID_TRANSITION"))
		observability.LogWarn(ctx, "retry-topics", "invalid retry status transition attempted", tenantID, map[string]any{
			"error":          err.Error(),
			"current_status": currentStatus,
			"new_status":     newStatus,
		})
		return err
	}

	if err := s.repo.UpdateRetryStatus(ctx, tenantID, id, next); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "retry-topics", "failed updating retry status in repository", tenantID, map[string]any{"error": err.Error()})
		return err
	}
	return nil
}



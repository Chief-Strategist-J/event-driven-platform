package service

import (
	"context"
	"fmt"

	"event-driven-platform/messaging/engine/producers"
	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/domain/machines"
	"event-driven-platform/src/features/transactional-outbox/domain/rules"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/ports"
	"event-driven-platform/src/shared/utils"
)

type OutboxService struct {
	repo         ports.OutboxRepositoryPort
	producer     producers.ProducerPort
	stateMachine *machines.OutboxStateMachine
}

func NewOutboxService(repo ports.OutboxRepositoryPort, producer producers.ProducerPort) *OutboxService {
	return &OutboxService{
		repo:         repo,
		producer:     producer,
		stateMachine: machines.NewOutboxStateMachine(),
	}
}

func (s *OutboxService) StageEvent(ctx context.Context, input contracts.CreateOutboxRecordInput) (*contracts.OutboxRecord, error) {
	if validationErr := rules.ValidateOutboxInput(input); validationErr != nil {
		return nil, validationErr
	}

	newUUID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}

	publicID, err := utils.GeneratePublicID("otb")
	if err != nil {
		return nil, err
	}

	headers := input.Headers
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["event_type"] = input.EventType
	headers["schema_version"] = input.SchemaVersion
	headers["source_service"] = input.SourceService

	now := utils.NowUTC()
	record := contracts.OutboxRecord{
		ID:               newUUID,
		PublicID:         publicID,
		TenantID:         input.TenantID,
		SourceService:    input.SourceService,
		ExternalID:       input.ExternalID,
		CorrelationID:    input.CorrelationID,
		CausationID:      input.CausationID,
		AggregateType:    input.AggregateType,
		AggregateID:      input.AggregateID,
		AggregateVersion: input.AggregateVersion,
		EventType:        input.EventType,
		SchemaVersion:    input.SchemaVersion,
		Payload:          input.Payload,
		Headers:          headers,
		Status:           contracts.StatusPending,
		RetryCount:       0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if err := s.repo.Save(ctx, record); err != nil {
		return nil, fmt.Errorf("failed staging outbox record: %w", err)
	}

	return &record, nil
}

func (s *OutboxService) RelayBatch(ctx context.Context, batchSize int) (int, error) {
	records, err := s.repo.FetchPending(ctx, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed fetching pending outbox records: %w", err)
	}

	publishedCount := 0
	for _, rec := range records {
		topic := rec.AggregateType
		keyBytes := []byte(rec.AggregateID)

		if err := s.producer.Publish(ctx, topic, keyBytes, rec.Payload); err != nil {
			_ = s.repo.MarkFailed(ctx, rec.TenantID, rec.ID, err.Error())
			continue
		}

		if _, err := s.stateMachine.TransitionTo(rec.Status, contracts.StatusPublished); err != nil {
			return publishedCount, fmt.Errorf("invalid transition to published: %w", err)
		}

		if err := s.repo.MarkPublished(ctx, rec.TenantID, rec.ID); err != nil {
			return publishedCount, fmt.Errorf("failed marking outbox published: %w", err)
		}
		publishedCount++
	}

	return publishedCount, nil
}

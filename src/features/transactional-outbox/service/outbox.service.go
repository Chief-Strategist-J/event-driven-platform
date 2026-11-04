package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/messaging/engine/producers"
	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/domain/machines"
	"event-driven-platform/src/features/transactional-outbox/domain/rules"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/ports"
	"event-driven-platform/src/shared/constants"
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

func (s *OutboxService) StageEvent(ctx context.Context, input contracts.CreateOutboxRecordInput, idempotencyKey string) (*contracts.OutboxRecord, error) {
	if validationErr := rules.ValidateOutboxInput(input); validationErr != nil {
		return nil, validationErr
	}

	newUUID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}

	if idempotencyKey != "" {
		claimed, existingRecordID, err := s.repo.ClaimIdempotency(ctx, input.TenantID, idempotencyKey, newUUID)
		if err != nil {
			return nil, fmt.Errorf("failed claiming idempotency key: %w", err)
		}
		if !claimed {
			existing, err := s.repo.GetByID(ctx, input.TenantID, existingRecordID)
			if err == nil && existing != nil {
				return existing, nil
			}
		}
	}

	publicID, err := utils.GeneratePublicID(constants.PrefixOutbox)
	if err != nil {
		return nil, err
	}

	headers := input.Headers
	if headers == nil {
		headers = make(map[string]string)
	}
	headers[constants.HeaderEventType] = input.EventType
	headers[constants.HeaderSchemaVersion] = input.SchemaVersion
	headers[constants.HeaderSourceService] = input.SourceService

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

	if input.CDCMode {
		if err := s.repo.SaveAndImmediateDelete(ctx, record); err != nil {
			return nil, fmt.Errorf("failed staging cdc outbox record: %w", err)
		}
		return &record, nil
	}

	if err := s.repo.Save(ctx, record); err != nil {
		return nil, fmt.Errorf("failed staging outbox record: %w", err)
	}

	return &record, nil
}

func (s *OutboxService) EmitCanaryHeartbeat(ctx context.Context, tenantID, sourceService string) (*contracts.OutboxRecord, error) {
	canaryID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}

	input := contracts.CreateOutboxRecordInput{
		TenantID:         tenantID,
		SourceService:    sourceService,
		ExternalID:       "canary_" + canaryID,
		CorrelationID:    canaryID,
		CausationID:      "canary_scheduler",
		AggregateType:    constants.PrefixCanary,
		AggregateID:      sourceService,
		AggregateVersion: 1,
		EventType:        "heartbeat.v1",
		SchemaVersion:    constants.APIVersionV1,
		Payload:          []byte(fmt.Sprintf(`{"timestamp":%d,"source":"%s"}`, time.Now().Unix(), sourceService)),
	}

	return s.StageEvent(ctx, input, "")
}

func (s *OutboxService) RelayBatch(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = constants.DefaultRelayBatchSize
	}

	workerID, err := utils.GenerateUUIDv7()
	if err != nil {
		return 0, err
	}

	leaseDuration := time.Duration(constants.DefaultLeaseSeconds) * time.Second
	records, err := s.repo.ClaimInFlight(ctx, workerID, leaseDuration, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed claiming in-flight outbox records: %w", err)
	}

	publishedCount := 0
	for _, rec := range records {
		topic := rec.AggregateType
		keyBytes := []byte(rec.AggregateID)

		if err := s.producer.Publish(ctx, topic, keyBytes, rec.Payload); err != nil {
			_ = s.repo.MarkFailed(ctx, rec.TenantID, rec.ID, err.Error())
			continue
		}

		if _, err := s.stateMachine.TransitionTo(contracts.StatusInFlight, contracts.StatusPublished); err != nil {
			return publishedCount, fmt.Errorf("invalid transition to published: %w", err)
		}

		if err := s.repo.MarkPublishedByWorker(ctx, rec.TenantID, rec.ID, workerID); err != nil {
			return publishedCount, fmt.Errorf("failed marking outbox published by worker: %w", err)
		}
		publishedCount++
	}

	return publishedCount, nil
}

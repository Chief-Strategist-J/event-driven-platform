package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/src/features/dead-letter-handling/contracts"
	"event-driven-platform/src/features/dead-letter-handling/domain/machines"
	"event-driven-platform/src/features/dead-letter-handling/domain/rules"
	"event-driven-platform/src/features/dead-letter-handling/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/utils"
)

type ConsumerInvoker func(ctx context.Context, record contracts.DeadLetterQuarantineRecord) (bool, error)

type DeadLetterService struct {
	repo         ports.DeadLetterRepositoryPort
	stateMachine *machines.DeadLetterStateMachine
}

func NewDeadLetterService(repo ports.DeadLetterRepositoryPort) *DeadLetterService {
	return &DeadLetterService{
		repo:         repo,
		stateMachine: machines.NewDeadLetterStateMachine(),
	}
}

func (s *DeadLetterService) QuarantineEvent(ctx context.Context, input contracts.QuarantineEventInput) (*contracts.DeadLetterQuarantineRecord, error) {
	if validationErr := contracts.ValidateQuarantineInput(input); validationErr != nil {
		return nil, validationErr
	}

	if !rules.IsMessageDefect(input.ErrorCode) {
		return nil, errors.NewPreconditionFailedError("infrastructure and system errors must not be sent to DLQ (DL-08)")
	}

	id, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}
	publicID, err := utils.GeneratePublicID("dlq")
	if err != nil {
		return nil, err
	}

	fingerprint := contracts.ComputeErrorFingerprint(input.ErrorCode, input.FailureReason)

	record := contracts.DeadLetterQuarantineRecord{
		ID:                 id,
		PublicID:           publicID,
		TenantID:           input.TenantID,
		SourceService:      input.SourceService,
		ExternalID:         input.ExternalID,
		CorrelationID:      input.CorrelationID,
		Topic:              input.Topic,
		Partition:          input.Partition,
		SourceOffset:       input.SourceOffset,
		ConsumerGroup:      input.ConsumerGroup,
		HandlerID:          input.HandlerID,
		EventID:            input.EventID,
		AggregateType:      input.AggregateType,
		AggregateID:        input.AggregateID,
		AggregateVersion:   input.AggregateVersion,
		EventType:          input.EventType,
		Payload:            input.Payload,
		Headers:            input.Headers,
		FailureReason:      input.FailureReason,
		ErrorCode:          input.ErrorCode,
		ErrorFingerprint:   fingerprint,
		Status:             contracts.StatusQuarantined,
		RedriveCount:       0,
		MaxRedriveAttempts: 3,
		QuarantinedAt:      time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}

	if err := s.repo.Quarantine(ctx, record); err != nil {
		return nil, fmt.Errorf("failed persisting dead letter record: %w", err)
	}

	park := contracts.AggregateParkingState{
		TenantID:      input.TenantID,
		HandlerID:     input.HandlerID,
		AggregateType: input.AggregateType,
		AggregateID:   input.AggregateID,
		IsParked:      true,
		ParkedReason:  input.FailureReason,
		QuarantineID:  id,
		ParkedAt:      time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
	_ = s.repo.ParkKey(ctx, park)

	return &record, nil
}

func (s *DeadLetterService) RedriveEvent(ctx context.Context, tenantID, id string, invoker ConsumerInvoker) (*contracts.RedriveResult, error) {
	record, err := s.repo.GetQuarantinedByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, errors.NewNotFoundError("quarantined event not found")
	}

	if !rules.CanRedrive(*record) {
		return &contracts.RedriveResult{
			QuarantineID: id,
			Status:       record.Status,
			RedriveCount: record.RedriveCount,
			Success:      false,
			Reason:       "event has reached maximum redrive attempts or is in terminal state",
		}, nil
	}

	nextCount := record.RedriveCount + 1
	_ = s.repo.UpdateDisposition(ctx, tenantID, id, contracts.StatusRedriving, nextCount)

	success, err := invoker(ctx, *record)
	if err != nil || !success {
		_ = s.repo.UpdateDisposition(ctx, tenantID, id, contracts.StatusQuarantined, nextCount)
		return &contracts.RedriveResult{
			QuarantineID: id,
			Status:       contracts.StatusQuarantined,
			RedriveCount: nextCount,
			Success:      false,
			Reason:       "redrive execution failed",
		}, nil
	}

	_ = s.repo.UpdateDisposition(ctx, tenantID, id, contracts.StatusRedrivenSuccess, nextCount)
	_ = s.repo.UnparkKey(ctx, tenantID, record.HandlerID, record.AggregateType, record.AggregateID)

	return &contracts.RedriveResult{
		QuarantineID: id,
		Status:       contracts.StatusRedrivenSuccess,
		RedriveCount: nextCount,
		Success:      true,
		Reason:       "event redriven successfully and aggregate unparked",
	}, nil
}

func (s *DeadLetterService) DiscardEvent(ctx context.Context, tenantID, id, reason string) error {
	record, err := s.repo.GetQuarantinedByID(ctx, tenantID, id)
	if err != nil {
		return err
	}
	if record == nil {
		return errors.NewNotFoundError("quarantined event not found")
	}

	if _, err := s.stateMachine.Transition(record.Status, contracts.StatusDiscarded); err != nil {
		return errors.NewConflictError("cannot discard event in current state")
	}

	if err := s.repo.UpdateDisposition(ctx, tenantID, id, contracts.StatusDiscarded, record.RedriveCount); err != nil {
		return err
	}

	_ = s.repo.UnparkKey(ctx, tenantID, record.HandlerID, record.AggregateType, record.AggregateID)
	return nil
}

func (s *DeadLetterService) IsKeyParked(ctx context.Context, tenantID, handlerID, aggregateType, aggregateID string) (bool, error) {
	return s.repo.IsKeyParked(ctx, tenantID, handlerID, aggregateType, aggregateID)
}

func (s *DeadLetterService) ListQuarantined(ctx context.Context, tenantID, handlerID string, status contracts.QuarantineStatus, limit, offset int) ([]contracts.DeadLetterQuarantineRecord, error) {
	return s.repo.ListQuarantined(ctx, tenantID, handlerID, status, limit, offset)
}


package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/domain/machines"
	"event-driven-platform/src/features/idempotent-consumers/domain/rules"
	"event-driven-platform/src/features/idempotent-consumers/infrastructure/ports"
	"event-driven-platform/src/shared/utils"
)

type ConsumerService struct {
	repo         ports.ConsumerRepositoryPort
	stateMachine *machines.ConsumerStateMachine
}

func NewConsumerService(repo ports.ConsumerRepositoryPort) *ConsumerService {
	return &ConsumerService{
		repo:         repo,
		stateMachine: machines.NewConsumerStateMachine(),
	}
}

func (s *ConsumerService) ConsumeEvent(ctx context.Context, event contracts.InboundConsumerEvent) (*contracts.ConsumeResult, error) {
	if validationErr := contracts.ValidateInboundConsumerEvent(event); validationErr != nil {
		return nil, validationErr
	}

	payloadHash := contracts.ComputePayloadHash(event.Payload)

	inboxID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}

	inboxRecord := contracts.ConsumerInboxRecord{
		ID:               inboxID,
		TenantID:         event.TenantID,
		HandlerID:        event.HandlerID,
		EventID:          event.EventID,
		AggregateType:    event.AggregateType,
		AggregateID:      event.AggregateID,
		AggregateVersion: event.AggregateVersion,
		EventType:        event.EventType,
		ProcessedAt:      time.Now().UTC(),
		PayloadHash:      payloadHash,
	}

	inserted, err := s.repo.RecordInbox(ctx, inboxRecord)
	if err != nil {
		return nil, fmt.Errorf("inbox recording failed: %w", err)
	}

	if !inserted {
		return &contracts.ConsumeResult{
			EventID:        event.EventID,
			AggregateID:    event.AggregateID,
			Action:         contracts.ActionSkip,
			AppliedVersion: event.AggregateVersion,
			Reason:         "transport duplicate detected via inbox (handler_id + event_id)",
		}, nil
	}

	proj, err := s.repo.GetProjection(ctx, event.TenantID, event.AggregateType, event.AggregateID)
	if err != nil {
		return nil, fmt.Errorf("projection read failed: %w", err)
	}

	var currentVersion int64 = 0
	if proj != nil {
		currentVersion = proj.LastVersion
		if proj.IsParked {
			return &contracts.ConsumeResult{
				EventID:        event.EventID,
				AggregateID:    event.AggregateID,
				Action:         contracts.ActionParkPoison,
				CurrentVersion: currentVersion,
				AppliedVersion: currentVersion,
				Reason:         "aggregate projection is parked; subsequent versions suppressed",
			}, nil
		}

		if event.AggregateVersion == proj.LastVersion && !rules.IsContentHashConsistent(proj.LastPayloadHash, payloadHash) {
			_ = s.repo.ParkAggregate(ctx, event.TenantID, event.AggregateType, event.AggregateID, "split-brain content collision")
			return &contracts.ConsumeResult{
				EventID:        event.EventID,
				AggregateID:    event.AggregateID,
				Action:         contracts.ActionParkPoison,
				CurrentVersion: currentVersion,
				AppliedVersion: currentVersion,
				Reason:         "divergent content detected for same version; aggregate parked",
			}, nil
		}
	}

	action := rules.EvaluateVersionRule(currentVersion, event.AggregateVersion, event.SemanticClass)
	switch action {
	case contracts.ActionSkip:
		return &contracts.ConsumeResult{
			EventID:        event.EventID,
			AggregateID:    event.AggregateID,
			Action:         contracts.ActionSkip,
			CurrentVersion: currentVersion,
			AppliedVersion: currentVersion,
			Reason:         "incoming version <= last applied version; duplicate skipped",
		}, nil

	case contracts.ActionHoldGap:
		return &contracts.ConsumeResult{
			EventID:        event.EventID,
			AggregateID:    event.AggregateID,
			Action:         contracts.ActionHoldGap,
			CurrentVersion: currentVersion,
			AppliedVersion: currentVersion,
			Reason:         "version gap detected (upstream event loss); holding key",
		}, nil

	case contracts.ActionApply:
		if err := s.repo.ApplyProjectionEffect(ctx, event.TenantID, event.AggregateType, event.AggregateID, event.AggregateVersion, event.EventID, payloadHash); err != nil {
			return nil, fmt.Errorf("failed applying projection effect: %w", err)
		}

		return &contracts.ConsumeResult{
			EventID:        event.EventID,
			AggregateID:    event.AggregateID,
			Action:         contracts.ActionApply,
			CurrentVersion: currentVersion,
			AppliedVersion: event.AggregateVersion,
			Reason:         "effect applied and version advanced atomically",
		}, nil

	default:
		return nil, fmt.Errorf("unhandled consumer action: %s", action)
	}
}

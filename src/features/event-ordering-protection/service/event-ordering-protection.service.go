package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
	"event-driven-platform/src/features/event-ordering-protection/domain/machines"
	"event-driven-platform/src/features/event-ordering-protection/domain/rules"
	"event-driven-platform/src/features/event-ordering-protection/infrastructure/ports"
	"event-driven-platform/src/shared/utils"
)

type EventOrderingService struct {
	repo ports.EventOrderingRepositoryPort
}

func NewEventOrderingService(repo ports.EventOrderingRepositoryPort) *EventOrderingService {
	return &EventOrderingService{repo: repo}
}

func (s *EventOrderingService) GetRepository() ports.EventOrderingRepositoryPort {
	return s.repo
}

func (s *EventOrderingService) EvaluateAndApply(ctx context.Context, input contracts.IngestEventSequenceInput) (*contracts.SequenceEvaluationResult, error) {
	if err := contracts.ValidateIngestEventSequence(input); err != nil {
		return nil, err
	}

	tracker, err := s.repo.GetSequenceTracker(ctx, input.TenantID, input.ConsumerGroup, input.HandlerID, input.AggregateType, input.AggregateID)
	if err != nil {
		return nil, fmt.Errorf("failed fetching sequence tracker: %w", err)
	}

	var currentVersion int64 = 0
	var currentHash string = ""
	var isHeld bool = false
	var holdReason contracts.HoldReason = ""

	if tracker != nil {
		if tracker.OwnerEpoch > input.OwnerEpoch {
			return &contracts.SequenceEvaluationResult{
				Action:          contracts.ActionFencedEpochReject,
				CurrentVersion:  tracker.LastAppliedVersion,
				IncomingVersion: input.AggregateVersion,
				IsHeld:          tracker.IsHeld,
			}, nil
		}
		currentVersion = tracker.LastAppliedVersion
		currentHash = tracker.LastPayloadHash
		isHeld = tracker.IsHeld
		holdReason = tracker.HoldReason
	}

	payloadHash := rules.ComputePayloadHash(input.Payload)
	decision := rules.EvaluateSequenceProgression(currentVersion, currentHash, input.AggregateVersion, payloadHash, isHeld, holdReason)

	corrID := input.CorrelationID
	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	switch decision.Action {
	case contracts.ActionApply:
		id, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("seq")
		record := contracts.AggregateSequenceTrackerRecord{
			ID:                 id,
			PublicID:           publicID,
			TenantID:           input.TenantID,
			SourceService:      input.SourceService,
			ExternalID:         input.ExternalID,
			CorrelationID:      corrID,
			ConsumerGroup:      input.ConsumerGroup,
			HandlerID:          input.HandlerID,
			AggregateType:      input.AggregateType,
			AggregateID:        input.AggregateID,
			LastAppliedVersion: input.AggregateVersion,
			LastPayloadHash:    payloadHash,
			OwnerEpoch:         input.OwnerEpoch,
			IsHeld:             isHeld,
			HoldReason:         holdReason,
		}
		if err := s.repo.ApplyEventSequence(ctx, record); err != nil {
			return nil, fmt.Errorf("failed applying event sequence: %w", err)
		}
		decision.CurrentVersion = input.AggregateVersion

	case contracts.ActionGapHeld, contracts.ActionIntegrityMismatch:
		id, _ := utils.GenerateUUIDv7()
		publicID, _ := utils.GeneratePublicID("seq")
		blockingVer := input.AggregateVersion
		holdRecord := contracts.AggregateSequenceTrackerRecord{
			ID:                 id,
			PublicID:           publicID,
			TenantID:           input.TenantID,
			SourceService:      input.SourceService,
			ExternalID:         input.ExternalID,
			CorrelationID:      corrID,
			ConsumerGroup:      input.ConsumerGroup,
			HandlerID:          input.HandlerID,
			AggregateType:      input.AggregateType,
			AggregateID:        input.AggregateID,
			LastAppliedVersion: currentVersion,
			LastPayloadHash:    payloadHash,
			OwnerEpoch:         input.OwnerEpoch,
			IsHeld:             true,
			HoldReason:         decision.HoldReason,
			BlockingVersion:    &blockingVer,
		}
		if err := s.repo.HoldAggregateKey(ctx, holdRecord); err != nil {
			return nil, fmt.Errorf("failed recording held aggregate: %w", err)
		}

		heldEventUUID := input.EventID
		if _, err := uuid.Parse(heldEventUUID); err != nil {
			heldEventUUID, _ = utils.GenerateUUIDv7()
		}

		heldID, _ := utils.GenerateUUIDv7()
		heldPublicID, _ := utils.GeneratePublicID("hld")
		heldEvent := contracts.SequenceHeldEventRecord{
			ID:               heldID,
			PublicID:         heldPublicID,
			TenantID:         input.TenantID,
			SourceService:    input.SourceService,
			ExternalID:       input.ExternalID,
			CorrelationID:    corrID,
			ConsumerGroup:    input.ConsumerGroup,
			HandlerID:        input.HandlerID,
			AggregateType:    input.AggregateType,
			AggregateID:      input.AggregateID,
			AggregateVersion: input.AggregateVersion,
			EventID:          heldEventUUID,
			EventType:        input.EventType,
			Payload:          input.Payload,
			PayloadHash:      payloadHash,
			Headers:          input.Headers,
			SourceOffset:     input.SourceOffset,
		}
		if err := s.repo.StoreHeldEvent(ctx, heldEvent); err != nil {
			return nil, fmt.Errorf("failed storing held event: %w", err)
		}
	}

	return &decision, nil
}

func (s *EventOrderingService) DrainHeldEvents(ctx context.Context, input contracts.DrainHeldEventsInput) (*contracts.DrainHeldEventsResult, error) {
	tracker, err := s.repo.GetSequenceTracker(ctx, input.TenantID, input.ConsumerGroup, input.HandlerID, input.AggregateType, input.AggregateID)
	if err != nil {
		return nil, fmt.Errorf("failed fetching tracker for drain: %w", err)
	}
	if tracker == nil || !tracker.IsHeld {
		return &contracts.DrainHeldEventsResult{
			EventsDrained: 0,
			NewVersion:    0,
			RemainingHeld: 0,
			IsUnheld:      true,
		}, nil
	}

	if tracker.OwnerEpoch > input.OwnerEpoch {
		return nil, rules.ErrFencedEpoch
	}

	heldEvents, err := s.repo.GetHeldEventsForDrain(ctx, input.TenantID, input.ConsumerGroup, input.HandlerID, input.AggregateType, input.AggregateID)
	if err != nil {
		return nil, fmt.Errorf("failed fetching held events for drain: %w", err)
	}

	currentVersion := tracker.LastAppliedVersion
	currentHash := tracker.LastPayloadHash
	var eventsDrained int64 = 0

	for _, held := range heldEvents {
		if !machines.CanDrainSequence(currentVersion, held.AggregateVersion) {
			break
		}
		currentVersion = held.AggregateVersion
		currentHash = held.PayloadHash
		eventsDrained++

		if err := s.repo.DeleteHeldEvent(ctx, input.TenantID, input.ConsumerGroup, input.HandlerID, input.AggregateType, input.AggregateID, held.AggregateVersion); err != nil {
			return nil, fmt.Errorf("failed deleting drained held event: %w", err)
		}
	}

	remaining := int64(len(heldEvents)) - eventsDrained
	isUnheld := remaining == 0

	tracker.LastAppliedVersion = currentVersion
	tracker.LastPayloadHash = currentHash
	tracker.OwnerEpoch = input.OwnerEpoch
	tracker.IsHeld = !isUnheld
	if isUnheld {
		tracker.HoldReason = ""
		tracker.BlockingVersion = nil
		if err := s.repo.ApplyEventSequence(ctx, *tracker); err != nil {
			return nil, fmt.Errorf("failed updating tracker after drain: %w", err)
		}
	} else {
		if err := s.repo.HoldAggregateKey(ctx, *tracker); err != nil {
			return nil, fmt.Errorf("failed updating held tracker after partial drain: %w", err)
		}
	}

	return &contracts.DrainHeldEventsResult{
		EventsDrained: eventsDrained,
		NewVersion:    currentVersion,
		RemainingHeld: remaining,
		IsUnheld:      isUnheld,
	}, nil
}

func (s *EventOrderingService) ListHeldTrackers(ctx context.Context, tenantID, group, handler string, limit, offset int) ([]contracts.AggregateSequenceTrackerRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListHeldTrackers(ctx, tenantID, group, handler, limit, offset)
}


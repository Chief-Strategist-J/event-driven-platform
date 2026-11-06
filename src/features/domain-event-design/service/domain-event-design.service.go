package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/domain-event-design/contracts"
	"event-driven-platform/src/features/domain-event-design/domain/rules"
	"event-driven-platform/src/features/domain-event-design/infrastructure/ports"
)

var (
	ErrCatalogInactive = errors.New("event catalog entry is inactive")
)

type DomainEventService struct {
	repo ports.DomainEventRepositoryPort
}

func NewDomainEventService(repo ports.DomainEventRepositoryPort) *DomainEventService {
	return &DomainEventService{repo: repo}
}

func (s *DomainEventService) BuildCanonicalEnvelope(
	ctx context.Context,
	cmd contracts.BuildEnvelopeCommand,
) (contracts.BuildEnvelopeResult, error) {
	if err := rules.ValidateEventType(cmd.EventType); err != nil {
		return contracts.BuildEnvelopeResult{}, err
	}

	if err := rules.ValidateCausalDepth(cmd.CausationDepth); err != nil {
		return contracts.BuildEnvelopeResult{}, err
	}

	if err := rules.ValidateTransactionGroup(cmd.TransactionGroupID, cmd.GroupSize); err != nil {
		return contracts.BuildEnvelopeResult{}, err
	}

	catalogEntry, err := s.repo.GetCatalogEntry(ctx, cmd.TenantID, cmd.EventType)
	if err != nil {
		catalogEntry = contracts.EventCatalogEntry{
			EventType:         cmd.EventType,
			OwningAggregate:   cmd.AggregateType,
			SemanticsClass:    contracts.SemanticsClassState,
			Visibility:        contracts.EventVisibilityPrivate,
			PIIClassification: contracts.PIIClassNone,
			IsActive:          true,
		}
	}

	if !catalogEntry.IsActive {
		return contracts.BuildEnvelopeResult{}, ErrCatalogInactive
	}

	reserveCmd := contracts.ReserveAggregateVersionCommand{
		TenantID:         cmd.TenantID,
		SourceService:    cmd.SourceService,
		ExternalID:       cmd.AggregateID,
		CorrelationID:    cmd.CorrelationID,
		AggregateType:    cmd.AggregateType,
		AggregateID:      cmd.AggregateID,
		ExpectedVersion:  cmd.ExpectedVersion,
		VersionIncrement: cmd.VersionIncrement,
	}

	reserved, err := s.repo.ReserveAggregateVersion(ctx, reserveCmd)
	if err != nil {
		return contracts.BuildEnvelopeResult{}, err
	}

	effectiveAt := cmd.EffectiveAt
	if effectiveAt.IsZero() {
		effectiveAt = time.Now().UTC()
	}
	recordedAt := time.Now().UTC()

	eventID := uuid.New().String()

	groupSize := cmd.GroupSize
	if groupSize <= 0 {
		groupSize = 1
	}

	envelope := contracts.CanonicalEnvelope{
		EventID:            eventID,
		EventType:          cmd.EventType,
		SchemaVersion:      "1.0.0",
		AggregateType:      cmd.AggregateType,
		AggregateID:        cmd.AggregateID,
		AggregateVersion:   reserved.ReservedVersion,
		TenantID:           cmd.TenantID,
		EffectiveAt:        effectiveAt,
		RecordedAt:         recordedAt,
		CausationID:        cmd.CausationID,
		CorrelationID:      cmd.CorrelationID,
		CausationDepth:     cmd.CausationDepth,
		TransactionGroupID: cmd.TransactionGroupID,
		GroupSize:          groupSize,
		SemanticsClass:     catalogEntry.SemanticsClass,
		Visibility:         catalogEntry.Visibility,
		PIIClassification:  catalogEntry.PIIClassification,
		SourceService:      cmd.SourceService,
		TraceContext:       cmd.TraceContext,
		Payload:            cmd.Payload,
	}

	pathHash := rules.ComputeCausalPathHash(eventID, cmd.CausationID, cmd.CausationDepth)
	var corrUUID uuid.UUID
	if parsed, pErr := uuid.Parse(cmd.CorrelationID); pErr == nil {
		corrUUID = parsed
	} else {
		corrUUID = uuid.New()
	}

	_ = s.repo.RecordCausalEntry(ctx, contracts.CausalChainEntry{
		ID:                uuid.New(),
		PublicID:          "csl_" + uuid.New().String()[:18],
		TenantID:          cmd.TenantID,
		SourceService:     cmd.SourceService,
		ExternalID:        eventID,
		CorrelationID:     corrUUID,
		EventID:           eventID,
		CausationID:       cmd.CausationID,
		CausationDepth:    cmd.CausationDepth,
		CausationPathHash: pathHash,
		CreatedAt:         recordedAt,
	})

	if cmd.TransactionGroupID != "" {
		_, _ = s.repo.UpsertTransactionGroup(ctx, contracts.RegisterTransactionGroupPartCommand{
			TenantID:           cmd.TenantID,
			SourceService:      cmd.SourceService,
			ExternalID:         eventID,
			CorrelationID:      cmd.CorrelationID,
			TransactionGroupID: cmd.TransactionGroupID,
			ExpectedGroupSize:  groupSize,
		})
	}

	return contracts.BuildEnvelopeResult{
		Envelope: envelope,
		Status:   "BUILT_AND_COMMITTED",
	}, nil
}

func (s *DomainEventService) RegisterCatalogEvent(
	ctx context.Context,
	cmd contracts.RegisterCatalogEventCommand,
) (contracts.EventCatalogEntry, error) {
	if err := rules.ValidateEventType(cmd.EventType); err != nil {
		return contracts.EventCatalogEntry{}, err
	}

	if strings.TrimSpace(cmd.OwningAggregate) == "" {
		return contracts.EventCatalogEntry{}, errors.New("owning_aggregate cannot be empty")
	}

	fingerprint := strings.TrimSpace(cmd.SchemaFingerprint)
	if len(fingerprint) != 64 {
		fingerprint = strings.Repeat("0", 64)
	}

	var corrUUID uuid.UUID
	if parsed, err := uuid.Parse(cmd.CorrelationID); err == nil {
		corrUUID = parsed
	} else {
		corrUUID = uuid.New()
	}

	entry := contracts.EventCatalogEntry{
		ID:                uuid.New(),
		PublicID:          "dec_" + uuid.New().String()[:18],
		TenantID:          cmd.TenantID,
		SourceService:     cmd.SourceService,
		ExternalID:        cmd.ExternalID,
		CorrelationID:     corrUUID,
		EventType:         cmd.EventType,
		OwningAggregate:   cmd.OwningAggregate,
		SemanticsClass:    cmd.SemanticsClass,
		Visibility:        cmd.Visibility,
		PIIClassification: cmd.PIIClassification,
		SchemaFingerprint: fingerprint,
		IsActive:          true,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}

	if err := s.repo.SaveCatalogEntry(ctx, entry); err != nil {
		return contracts.EventCatalogEntry{}, err
	}
	return entry, nil
}

func (s *DomainEventService) ReserveAggregateVersion(
	ctx context.Context,
	cmd contracts.ReserveAggregateVersionCommand,
) (contracts.ReserveAggregateVersionResult, error) {
	if strings.TrimSpace(cmd.AggregateType) == "" || strings.TrimSpace(cmd.AggregateID) == "" {
		return contracts.ReserveAggregateVersionResult{}, errors.New("aggregate_type and aggregate_id cannot be empty")
	}
	return s.repo.ReserveAggregateVersion(ctx, cmd)
}

func (s *DomainEventService) VerifyCausalChain(
	ctx context.Context,
	cmd contracts.VerifyCausalChainCommand,
) (contracts.VerifyCausalChainResult, error) {
	if err := rules.ValidateCausalDepth(cmd.CausationDepth); err != nil {
		return contracts.VerifyCausalChainResult{
			EventID:        cmd.EventID,
			IsValid:        false,
			CausationDepth: cmd.CausationDepth,
			LoopDetected:   true,
		}, err
	}

	pathHash := rules.ComputeCausalPathHash(cmd.EventID, cmd.CausationID, cmd.CausationDepth)

	if strings.TrimSpace(cmd.EventID) != "" && strings.TrimSpace(cmd.CausationID) != "" {
		if strings.TrimSpace(cmd.EventID) == strings.TrimSpace(cmd.CausationID) {
			return contracts.VerifyCausalChainResult{
				EventID:        cmd.EventID,
				IsValid:        false,
				CausationDepth: cmd.CausationDepth,
				PathHash:       pathHash,
				LoopDetected:   true,
			}, errors.New("immediate causal loop detected: event_id equals causation_id")
		}
	}

	return contracts.VerifyCausalChainResult{
		EventID:        cmd.EventID,
		IsValid:        true,
		CausationDepth: cmd.CausationDepth,
		PathHash:       pathHash,
		LoopDetected:   false,
	}, nil
}

func (s *DomainEventService) RegisterTransactionGroupPart(
	ctx context.Context,
	cmd contracts.RegisterTransactionGroupPartCommand,
) (contracts.RegisterTransactionGroupPartResult, error) {
	if strings.TrimSpace(cmd.TransactionGroupID) == "" {
		return contracts.RegisterTransactionGroupPartResult{}, errors.New("transaction_group_id cannot be empty")
	}
	if cmd.ExpectedGroupSize <= 0 {
		return contracts.RegisterTransactionGroupPartResult{}, rules.ErrInvalidTransactionGroup
	}
	return s.repo.UpsertTransactionGroup(ctx, cmd)
}

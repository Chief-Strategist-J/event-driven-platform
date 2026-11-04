package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/domain/machines"
	"event-driven-platform/src/features/event-versioning/domain/rules"
	"event-driven-platform/src/features/event-versioning/infrastructure/ports"
	"event-driven-platform/src/shared/utils"
)

type EventVersioningService struct {
	repo   ports.EventVersioningRepositoryPort
	engine *machines.UpcasterEngine
}

func NewEventVersioningService(repo ports.EventVersioningRepositoryPort, engine *machines.UpcasterEngine) *EventVersioningService {
	if engine == nil {
		engine = machines.NewUpcasterEngine()
	}
	return &EventVersioningService{
		repo:   repo,
		engine: engine,
	}
}

func (s *EventVersioningService) RegisterTransformer(name string, fn machines.UpcastTransformerFunc) {
	s.engine.RegisterTransformer(name, fn)
}

func (s *EventVersioningService) RegisterSchemaVersion(ctx context.Context, input contracts.RegisterSchemaVersionInput) (*contracts.SchemaVersionRecord, error) {
	if err := rules.ValidateRegisterSchemaInput(input); err != nil {
		return nil, err
	}

	if input.SchemaVersion > 1 {
		prevVersion, err := s.repo.GetSchemaVersion(ctx, input.TenantID, input.StreamName, input.EventType, input.SchemaVersion-1)
		if err != nil {
			return nil, fmt.Errorf("failed fetching previous schema version: %w", err)
		}
		if prevVersion != nil && input.CompatibilityMode != contracts.CompatNone {
			checkResult := rules.EvaluateJSONSchemaCompatibility(prevVersion.SchemaDefinition, input.SchemaDefinition, input.CompatibilityMode)
			if !checkResult.IsCompatible {
				return nil, fmt.Errorf("%w: %v", rules.ErrIncompatibleSchema, checkResult.Violations)
			}
		}
	}

	hash := sha256.Sum256(input.SchemaDefinition)
	payloadHash := hex.EncodeToString(hash[:])

	id, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, fmt.Errorf("failed generating internal id: %w", err)
	}

	publicID, err := utils.GeneratePublicID("sch")
	if err != nil {
		return nil, fmt.Errorf("failed generating public id: %w", err)
	}

	corrID := input.CorrelationID
	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	record := contracts.SchemaVersionRecord{
		ID:                id,
		PublicID:          publicID,
		TenantID:          input.TenantID,
		SourceService:     input.SourceService,
		ExternalID:        input.ExternalID,
		CorrelationID:     corrID,
		StreamName:        input.StreamName,
		EventType:         input.EventType,
		SchemaVersion:     input.SchemaVersion,
		SchemaID:          input.SchemaID,
		Format:            input.Format,
		CompatibilityMode: input.CompatibilityMode,
		SchemaDefinition:  input.SchemaDefinition,
		PayloadHash:       payloadHash,
		CreatedAt:         time.Now(),
	}

	if err := s.repo.SaveSchemaVersion(ctx, record); err != nil {
		return nil, fmt.Errorf("failed saving schema version: %w", err)
	}

	return &record, nil
}

func (s *EventVersioningService) CheckCompatibility(ctx context.Context, input contracts.CheckCompatibilityInput) (*contracts.CompatibilityCheckResult, error) {
	prevVersion, err := s.repo.GetSchemaVersion(ctx, input.TenantID, input.StreamName, input.EventType, input.TargetVersion-1)
	if err != nil {
		return nil, fmt.Errorf("failed checking compatibility baseline: %w", err)
	}
	if prevVersion == nil {
		return &contracts.CompatibilityCheckResult{IsCompatible: true}, nil
	}

	result := rules.EvaluateJSONSchemaCompatibility(prevVersion.SchemaDefinition, input.NewSchema, input.CompatibilityMode)
	return &result, nil
}

func (s *EventVersioningService) RegisterUpcaster(ctx context.Context, input contracts.RegisterUpcasterInput) (*contracts.UpcasterRecord, error) {
	if err := rules.ValidateUpcasterDefinition(input); err != nil {
		return nil, err
	}

	id, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, fmt.Errorf("failed generating internal id: %w", err)
	}

	publicID, err := utils.GeneratePublicID("upc")
	if err != nil {
		return nil, fmt.Errorf("failed generating public id: %w", err)
	}

	corrID := input.CorrelationID
	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	record := contracts.UpcasterRecord{
		ID:              id,
		PublicID:        publicID,
		TenantID:        input.TenantID,
		SourceService:   input.SourceService,
		ExternalID:      input.ExternalID,
		CorrelationID:   corrID,
		EventType:       input.EventType,
		FromVersion:     input.FromVersion,
		ToVersion:       input.ToVersion,
		TransformerName: input.TransformerName,
		IsActive:        true,
		CreatedAt:       time.Now(),
	}

	if err := s.repo.SaveUpcaster(ctx, record); err != nil {
		return nil, fmt.Errorf("failed saving upcaster definition: %w", err)
	}

	return &record, nil
}

func (s *EventVersioningService) UpcastEvent(ctx context.Context, input contracts.UpcastStepInput) (*contracts.UpcastResult, error) {
	if input.TargetVersion <= input.SourceVersion {
		return &contracts.UpcastResult{
			EventType:      input.EventType,
			InitialVersion: input.SourceVersion,
			FinalVersion:   input.SourceVersion,
			Payload:        input.Payload,
			StepsApplied:   0,
		}, nil
	}

	chain, err := s.repo.GetUpcasterChain(ctx, input.TenantID, input.EventType, input.SourceVersion, input.TargetVersion)
	if err != nil {
		return nil, fmt.Errorf("failed fetching upcaster chain: %w", err)
	}

	return s.engine.ExecuteChain(input, chain)
}


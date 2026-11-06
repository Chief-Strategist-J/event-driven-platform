package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/domain/machines"
	"event-driven-platform/src/features/partitioning-strategy/domain/rules"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/ports"
	"github.com/google/uuid"
)

type PartitioningService struct {
	repo         ports.PartitionRepositoryPort
	hasher       ports.HasherPort
	stateMachine *machines.MigrationStateMachine
}

func NewPartitioningService(repo ports.PartitionRepositoryPort, hasher ports.HasherPort) *PartitioningService {
	return &PartitioningService{
		repo:         repo,
		hasher:       hasher,
		stateMachine: machines.NewMigrationStateMachine(),
	}
}

func (s *PartitioningService) RegisterTopicTopology(ctx context.Context, in contracts.RegisterTopicTopologyInput) (*contracts.TopicTopology, error) {
	norm, err := contracts.NormalizeRegisterTopicTopologyInput(in)
	if err != nil {
		return nil, err
	}

	corrID, err := uuid.Parse(norm.CorrelationID)
	if err != nil {
		corrID = uuid.New()
	}

	topology := contracts.TopicTopology{
		ID:                    uuid.New(),
		PublicID:              generatePublicID("prt"),
		TenantID:              norm.TenantID,
		SourceService:         norm.SourceService,
		ExternalID:            norm.ExternalID,
		CorrelationID:         corrID,
		TopicName:             norm.TopicName,
		TopicClass:            norm.TopicClass,
		PartitionCount:        norm.PartitionCount,
		ReplicationFactor:     norm.ReplicationFactor,
		MinInSyncReplicas:     norm.MinInSyncReplicas,
		AllowInPlaceExpansion: norm.AllowInPlaceExpansion,
		Version:               1,
		CreatedAt:             time.Now().UTC(),
		UpdatedAt:             time.Now().UTC(),
	}

	if err := rules.ValidateTopicTopology(topology); err != nil {
		return nil, err
	}

	if err := s.repo.SaveTopicTopology(ctx, topology); err != nil {
		return nil, err
	}

	return &topology, nil
}

func (s *PartitioningService) CalculatePartition(ctx context.Context, in contracts.CalculatePartitionInput) (*contracts.CalculatePartitionOutput, error) {
	norm, err := contracts.NormalizeCalculatePartitionInput(in)
	if err != nil {
		return nil, err
	}

	var keyBytes []byte
	var normalizedKey string

	if len(norm.RawKeyBytes) > 0 {
		keyBytes = norm.RawKeyBytes
		normalizedKey = string(norm.RawKeyBytes)
	} else if norm.TopicClass != contracts.TopicClassUnordered {
		formatted, err := rules.FormatAggregateKey(norm.TenantID, norm.AggregateType, norm.AggregateID)
		if err != nil {
			return nil, err
		}
		normalizedKey = formatted
		keyBytes = []byte(formatted)
	} else {
		normalizedKey = norm.AggregateID
		keyBytes = []byte(norm.AggregateID)
	}

	partition, rawHash, maskedHash, err := s.hasher.CalculatePartition(keyBytes, norm.PartitionCount)
	if err != nil {
		return nil, err
	}

	return &contracts.CalculatePartitionOutput{
		TopicName:          norm.TopicName,
		Partition:          partition,
		PartitionCount:     norm.PartitionCount,
		NormalizedKey:      normalizedKey,
		Murmur2Hash:        rawHash,
		MaskedPositiveHash: maskedHash,
	}, nil
}

func (s *PartitioningService) EvaluateSkew(ctx context.Context, in contracts.EvaluateSkewInput) (*contracts.SkewEvaluationOutput, error) {
	norm, err := contracts.NormalizeEvaluateSkewInput(in)
	if err != nil {
		return nil, err
	}

	out, err := rules.CalculateSkewMetrics(norm.TopicName, norm.Partitions)
	if err != nil {
		return nil, err
	}

	corrID, err := uuid.Parse(norm.CorrelationID)
	if err != nil {
		corrID = uuid.New()
	}

	metrics := contracts.PartitionSkewMetrics{
		ID:                  uuid.New(),
		PublicID:            generatePublicID("psk"),
		TenantID:            norm.TenantID,
		SourceService:       norm.SourceService,
		ExternalID:          norm.ExternalID,
		CorrelationID:       corrID,
		TopicName:           norm.TopicName,
		MaxThroughputEPS:    out.MaxThroughputEPS,
		MedianThroughputEPS: out.MedianThroughputEPS,
		SkewRatio:           out.SkewRatio,
		HotPartitionID:      out.HotPartitionID,
		IsAlertBreached:     out.IsAlertBreached,
		MeasuredAt:          time.Now().UTC(),
	}

	if err := s.repo.RecordSkewMetrics(ctx, metrics); err != nil {
		return nil, err
	}

	return &out, nil
}

func (s *PartitioningService) InitiateMigration(ctx context.Context, in contracts.InitiateMigrationInput) (*contracts.MigrationRunbookRecord, error) {
	norm, err := contracts.NormalizeInitiateMigrationInput(in)
	if err != nil {
		return nil, err
	}

	corrID, err := uuid.Parse(norm.CorrelationID)
	if err != nil {
		corrID = uuid.New()
	}

	record := contracts.MigrationRunbookRecord{
		ID:               uuid.New(),
		PublicID:         generatePublicID("pmg"),
		TenantID:         norm.TenantID,
		SourceService:    norm.SourceService,
		ExternalID:       norm.ExternalID,
		CorrelationID:    corrID,
		MigrationID:      norm.MigrationID,
		SourceTopic:      norm.SourceTopic,
		TargetTopic:      norm.TargetTopic,
		SourcePartitions: norm.SourcePartitions,
		TargetPartitions: norm.TargetPartitions,
		Phase:            contracts.MigrationPhasePlanned,
		Version:          1,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	if err := s.repo.CreateMigrationRunbook(ctx, record); err != nil {
		return nil, err
	}

	return &record, nil
}

func (s *PartitioningService) AdvanceMigration(ctx context.Context, in contracts.AdvanceMigrationInput) (*contracts.MigrationRunbookRecord, error) {
	norm, err := contracts.NormalizeAdvanceMigrationInput(in)
	if err != nil {
		return nil, err
	}

	existing, err := s.repo.GetMigrationRunbook(ctx, norm.TenantID, norm.MigrationID)
	if err != nil {
		return nil, err
	}

	nextPhase, err := s.stateMachine.Transition(existing.Phase, norm.TargetPhase)
	if err != nil {
		return nil, err
	}

	if err := s.repo.UpdateMigrationPhase(ctx, norm.TenantID, norm.MigrationID, nextPhase, existing.Version); err != nil {
		return nil, err
	}

	existing.Phase = nextPhase
	existing.Version++
	existing.UpdatedAt = time.Now().UTC()

	return existing, nil
}

func (s *PartitioningService) GetTopicTopology(ctx context.Context, tenantID, topicName string) (*contracts.TopicTopology, error) {
	return s.repo.GetTopicTopology(ctx, tenantID, topicName)
}

func (s *PartitioningService) GetMigrationRunbook(ctx context.Context, tenantID, migrationID string) (*contracts.MigrationRunbookRecord, error) {
	return s.repo.GetMigrationRunbook(ctx, tenantID, migrationID)
}

func generatePublicID(prefix string) string {
	bytes := make([]byte, 10)
	_, _ = rand.Read(bytes)
	return prefix + "_" + hex.EncodeToString(bytes)
}

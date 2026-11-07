package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/features/partitioning-strategy/domain/machines"
	"event-driven-platform/src/features/partitioning-strategy/domain/rules"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
	platformErrors "event-driven-platform/src/shared/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := observability.StartSpan(ctx, "partitioning.register_topic_topology", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", in.TenantID),
		attribute.String("topic_name", in.TopicName),
		attribute.String("topic_class", string(in.TopicClass)),
		attribute.Int("partition_count", in.PartitionCount),
	)

	norm, err := contracts.NormalizeRegisterTopicTopologyInput(in)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "partitioning", "register topic topology validation failed", in.TenantID, map[string]any{
			"error": err.Error(),
		})
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
		observability.RecordError(span, err, attribute.String("error.type", "DOMAIN_RULE_VIOLATION"))
		observability.LogWarn(ctx, "partitioning", "topic topology rule validation failed", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	if err := s.repo.SaveTopicTopology(ctx, topology); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "PERSISTENCE_FAILED"))
		observability.LogError(ctx, "partitioning", "failed persisting topic topology", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	observability.LogInfo(ctx, "partitioning", "topic topology registered successfully", norm.TenantID, map[string]any{
		"topic_name":      norm.TopicName,
		"partition_count": norm.PartitionCount,
		"public_id":       topology.PublicID,
	})

	return &topology, nil
}

func (s *PartitioningService) CalculatePartition(ctx context.Context, in contracts.CalculatePartitionInput) (*contracts.CalculatePartitionOutput, error) {
	ctx, span := observability.StartSpan(ctx, "partitioning.calculate_partition", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", in.TenantID),
		attribute.String("topic_name", in.TopicName),
		attribute.String("aggregate_type", in.AggregateType),
		attribute.String("aggregate_id", in.AggregateID),
		attribute.Int("partition_count", in.PartitionCount),
	)

	norm, err := contracts.NormalizeCalculatePartitionInput(in)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "partitioning", "calculate partition validation failed", in.TenantID, map[string]any{
			"error": err.Error(),
		})
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
			observability.RecordError(span, err, attribute.String("error.type", "KEY_FORMATTING_FAILED"))
			observability.LogWarn(ctx, "partitioning", "failed formatting partition key", norm.TenantID, map[string]any{
				"error": err.Error(),
			})
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
		observability.RecordError(span, err, attribute.String("error.type", "HASH_CALCULATION_FAILED"))
		observability.LogError(ctx, "partitioning", "partition hashing calculation failed", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	span.SetAttributes(
		attribute.Int("resolved_partition", partition),
		attribute.Int("masked_positive_hash", int(maskedHash)),
	)

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
	ctx, span := observability.StartSpan(ctx, "partitioning.evaluate_skew", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", in.TenantID),
		attribute.String("topic_name", in.TopicName),
		attribute.Int("partition_metrics_count", len(in.Partitions)),
	)

	norm, err := contracts.NormalizeEvaluateSkewInput(in)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "partitioning", "evaluate skew validation failed", in.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	out, err := rules.CalculateSkewMetrics(norm.TopicName, norm.Partitions)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "SKEW_CALCULATION_FAILED"))
		observability.LogWarn(ctx, "partitioning", "failed calculating skew metrics", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
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
		observability.RecordError(span, err, attribute.String("error.type", "PERSISTENCE_FAILED"))
		observability.LogError(ctx, "partitioning", "failed recording skew metrics in repository", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	span.SetAttributes(
		attribute.Float64("skew_ratio", out.SkewRatio),
		attribute.Bool("is_alert_breached", out.IsAlertBreached),
		attribute.Int("hot_partition_id", out.HotPartitionID),
	)

	return &out, nil
}

func (s *PartitioningService) InitiateMigration(ctx context.Context, in contracts.InitiateMigrationInput) (*contracts.MigrationRunbookRecord, error) {
	ctx, span := observability.StartSpan(ctx, "partitioning.initiate_migration", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", in.TenantID),
		attribute.String("migration_id", in.MigrationID),
		attribute.String("source_topic", in.SourceTopic),
		attribute.String("target_topic", in.TargetTopic),
		attribute.Int("source_partitions", in.SourcePartitions),
		attribute.Int("target_partitions", in.TargetPartitions),
	)

	norm, err := contracts.NormalizeInitiateMigrationInput(in)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "partitioning", "initiate migration validation failed", in.TenantID, map[string]any{
			"error": err.Error(),
		})
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
		observability.RecordError(span, err, attribute.String("error.type", "PERSISTENCE_FAILED"))
		observability.LogError(ctx, "partitioning", "failed creating migration runbook in repository", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	observability.LogInfo(ctx, "partitioning", "partition migration planned successfully", norm.TenantID, map[string]any{
		"migration_id": norm.MigrationID,
		"source_topic": norm.SourceTopic,
		"target_topic": norm.TargetTopic,
	})

	return &record, nil
}

func (s *PartitioningService) AdvanceMigration(ctx context.Context, in contracts.AdvanceMigrationInput) (*contracts.MigrationRunbookRecord, error) {
	ctx, span := observability.StartSpan(ctx, "partitioning.advance_migration", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", in.TenantID),
		attribute.String("migration_id", in.MigrationID),
		attribute.String("target_phase", string(in.TargetPhase)),
	)

	norm, err := contracts.NormalizeAdvanceMigrationInput(in)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "partitioning", "advance migration validation failed", in.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	existing, err := s.repo.GetMigrationRunbook(ctx, norm.TenantID, norm.MigrationID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		observability.LogWarn(ctx, "partitioning", "migration runbook not found", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	if in.ExpectedPhase != "" && existing.Phase != in.ExpectedPhase {
		err := platformErrors.NewConflictError(fmt.Sprintf("phase conflict: expected %s, current %s", in.ExpectedPhase, existing.Phase))
		observability.RecordError(span, err, attribute.String("error.type", "PHASE_CONFLICT"))
		observability.LogWarn(ctx, "partitioning", "migration expected phase mismatch", norm.TenantID, map[string]any{
			"expected_phase": string(in.ExpectedPhase),
			"current_phase":  string(existing.Phase),
		})
		return nil, err
	}

	nextPhase, err := s.stateMachine.Transition(existing.Phase, norm.TargetPhase)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "INVALID_TRANSITION"))
		observability.LogWarn(ctx, "partitioning", "migration state machine rejected transition", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	if err := s.repo.UpdateMigrationPhase(ctx, norm.TenantID, norm.MigrationID, nextPhase, existing.Version); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "PERSISTENCE_FAILED"))
		observability.LogError(ctx, "partitioning", "failed updating migration phase in repository", norm.TenantID, map[string]any{
			"error": err.Error(),
		})
		return nil, err
	}

	existing.Phase = nextPhase
	existing.Version++
	existing.UpdatedAt = time.Now().UTC()

	observability.LogInfo(ctx, "partitioning", "partition migration phase advanced", norm.TenantID, map[string]any{
		"migration_id": norm.MigrationID,
		"new_phase":    string(nextPhase),
		"version":      existing.Version,
	})

	return existing, nil
}

func (s *PartitioningService) GetTopicTopology(ctx context.Context, tenantID, topicName string) (*contracts.TopicTopology, error) {
	ctx, span := observability.StartSpan(ctx, "partitioning.get_topic_topology", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("topic_name", topicName),
	)

	return s.repo.GetTopicTopology(ctx, tenantID, topicName)
}

func (s *PartitioningService) GetMigrationRunbook(ctx context.Context, tenantID, migrationID string) (*contracts.MigrationRunbookRecord, error) {
	ctx, span := observability.StartSpan(ctx, "partitioning.get_migration_runbook", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("migration_id", migrationID),
	)

	return s.repo.GetMigrationRunbook(ctx, tenantID, migrationID)
}

func generatePublicID(prefix string) string {
	bytes := make([]byte, 10)
	_, _ = rand.Read(bytes)
	return prefix + "_" + hex.EncodeToString(bytes)
}

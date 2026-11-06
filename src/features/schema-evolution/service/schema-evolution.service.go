package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/schema-evolution/contracts"
	"event-driven-platform/src/features/schema-evolution/domain/rules"
	"event-driven-platform/src/features/schema-evolution/infrastructure/ports"
)

var (
	ErrDuplicateSchemaFingerprint = errors.New("identical schema fingerprint already registered in lineage")
	ErrNonMonotonicVersion        = errors.New("new schema version must be strictly greater than latest registered version")
)

type SchemaEvolutionService struct {
	repo   ports.SchemaEvolutionRepositoryPort
	tracer trace.Tracer
}

func NewSchemaEvolutionService(repo ports.SchemaEvolutionRepositoryPort) *SchemaEvolutionService {
	return &SchemaEvolutionService{
		repo:   repo,
		tracer: otel.Tracer("SchemaEvolutionService"),
	}
}

func (s *SchemaEvolutionService) RegisterSchema(ctx context.Context, in contracts.RegisterSchemaInput) (*contracts.SchemaVersionRecord, error) {
	ctx, span := s.tracer.Start(ctx, "SchemaEvolutionService.RegisterSchema")
	defer span.End()

	norm, err := contracts.NormalizeRegisterSchemaInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	fingerprint := rules.ComputeSchemaFingerprint(norm.SchemaDefinition)
	lineage, err := s.repo.GetSchemaLineage(ctx, norm.TenantID, norm.SubjectName)
	if err != nil {
		return nil, err
	}

	if !rules.EvaluateTransitiveCompatibility(fingerprint, lineage) {
		return nil, ErrDuplicateSchemaFingerprint
	}

	if len(lineage) > 0 {
		latest := lineage[len(lineage)-1]
		if norm.SchemaVersion <= latest.SchemaVersion {
			return nil, ErrNonMonotonicVersion
		}
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("sev_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	record := contracts.SchemaVersionRecord{
		ID:                uuid.New(),
		PublicID:          pubID,
		TenantID:          norm.TenantID,
		SourceService:     norm.SourceService,
		ExternalID:        norm.ExternalID,
		CorrelationID:     corrUUID,
		SubjectName:       norm.SubjectName,
		SchemaVersion:     norm.SchemaVersion,
		CompatibilityMode: norm.CompatibilityMode,
		SchemaFingerprint: fingerprint,
		SchemaDefinition:  norm.SchemaDefinition,
		Status:            contracts.EvolutionStatusActive,
		Version:           1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err := s.repo.RegisterSchemaVersion(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *SchemaEvolutionService) CheckConsumerReadiness(ctx context.Context, in contracts.CheckConsumerReadinessInput) (*contracts.ConsumerReadinessOutput, error) {
	ctx, span := s.tracer.Start(ctx, "SchemaEvolutionService.CheckConsumerReadiness")
	defer span.End()

	norm, err := contracts.NormalizeCheckConsumerReadinessInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	consumers, err := s.repo.GetActiveConsumersForSubject(ctx, norm.TenantID, norm.SubjectName)
	if err != nil {
		return nil, err
	}

	out := rules.EvaluateConsumerReadiness(consumers, norm.TargetVersion)
	out.SubjectName = norm.SubjectName
	return &out, nil
}

func (s *SchemaEvolutionService) RegisterConsumerSupport(
	ctx context.Context,
	tenantID string,
	consumerGroup string,
	subjectName string,
	minV int,
	maxV int,
) (*contracts.ConsumerSupportRecord, error) {
	now := time.Now().UTC()
	pubID := fmt.Sprintf("sec_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	record := contracts.ConsumerSupportRecord{
		ID:                  uuid.New(),
		PublicID:            pubID,
		TenantID:            tenantID,
		SourceService:       "consumer-runtime",
		ExternalID:          uuid.New().String(),
		CorrelationID:       uuid.New(),
		ConsumerGroup:       consumerGroup,
		SubjectName:         subjectName,
		MinSupportedVersion: minV,
		MaxSupportedVersion: maxV,
		RunCadence:          "CONTINUOUS",
		Status:              "ACTIVE",
		Version:             1,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if err := s.repo.RegisterConsumerSupport(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *SchemaEvolutionService) InitiateDualPublish(ctx context.Context, in contracts.InitiateDualPublishInput) (*contracts.DualPublishRecord, error) {
	norm, err := contracts.NormalizeInitiateDualPublishInput(in)
	if err != nil {
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("sed_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	record := contracts.DualPublishRecord{
		ID:                   uuid.New(),
		PublicID:             pubID,
		TenantID:             norm.TenantID,
		SourceService:        norm.SourceService,
		ExternalID:           uuid.New().String(),
		CorrelationID:        corrUUID,
		SubjectName:          norm.SubjectName,
		LegacyTopic:          norm.LegacyTopic,
		ModernTopic:          norm.ModernTopic,
		SunsetDeadline:       norm.SunsetDeadline,
		Status:               "ACTIVE",
		DecommissionAttested: false,
		ZeroLagDays:          0,
		Version:              1,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	if err := s.repo.CreateDualPublishEntry(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *SchemaEvolutionService) EvaluateDecommission(ctx context.Context, in contracts.EvaluateDecommissionInput) (*contracts.DecommissionCheckOutput, error) {
	norm, err := contracts.NormalizeEvaluateDecommissionInput(in)
	if err != nil {
		return nil, err
	}

	out := rules.EvaluateDecommissionCheck(norm)
	return &out, nil
}

func (s *SchemaEvolutionService) RecordEvolutionIncident(ctx context.Context, in contracts.RecordIncidentInput) (*contracts.EvolutionIncidentRecord, error) {
	ctx, span := s.tracer.Start(ctx, "SchemaEvolutionService.RecordEvolutionIncident")
	defer span.End()

	norm, err := contracts.NormalizeRecordIncidentInput(in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	corrUUID, _ := uuid.Parse(norm.CorrelationID)
	now := time.Now().UTC()
	pubID := fmt.Sprintf("sei_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:20])

	record := contracts.EvolutionIncidentRecord{
		ID:                 uuid.New(),
		PublicID:           pubID,
		TenantID:           norm.TenantID,
		SourceService:      norm.SourceService,
		ExternalID:         uuid.New().String(),
		CorrelationID:      corrUUID,
		SubjectName:        norm.SubjectName,
		ConsumerGroup:      norm.ConsumerGroup,
		IncidentClass:      norm.IncidentClass,
		PayloadFingerprint: norm.PayloadFingerprint,
		ErrorMessage:       norm.ErrorMessage,
		Status:             "OPEN",
		OccurredAt:         now,
		CreatedAt:          now,
	}

	if err := s.repo.RecordIncident(ctx, record); err != nil {
		return nil, err
	}
	return &record, nil
}


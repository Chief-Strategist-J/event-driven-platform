package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/domain/rules"
	"event-driven-platform/src/features/consumer-groups/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/utils"
)

type ConsumerGroupsService struct {
	repo ports.ConsumerGroupsRepositoryPort
}

func NewConsumerGroupsService(repo ports.ConsumerGroupsRepositoryPort) *ConsumerGroupsService {
	return &ConsumerGroupsService{repo: repo}
}

func (s *ConsumerGroupsService) GetRepository() ports.ConsumerGroupsRepositoryPort {
	return s.repo
}

func (s *ConsumerGroupsService) RegisterMember(ctx context.Context, input contracts.RegisterMemberInput) (*contracts.ConsumerGroupMemberRecord, error) {
	ctx, span := observability.StartSpan(ctx, "consumer-groups.register_member")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("member_id", input.MemberID),
		attribute.String("assignment_strategy", input.AssignmentStrategy),
	)

	if err := contracts.ValidateRegisterMember(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "consumer-groups", "validation rejected member registration", input.TenantID, map[string]any{"error": err.Error()})
		return nil, err
	}
	if err := rules.VerifyAssignorStrategy(input.AssignmentStrategy); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "INVALID_STRATEGY"))
		observability.LogWarn(ctx, "consumer-groups", "eager or invalid assignor strategy rejected", input.TenantID, map[string]any{"error": err.Error(), "strategy": input.AssignmentStrategy})
		return nil, err
	}

	id, _ := utils.GenerateUUIDv7()
	publicID, _ := utils.GeneratePublicID("cgm")

	corrID := input.CorrelationID
	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	} else {
		if _, err := uuid.Parse(corrID); err != nil {
			corrID, _ = utils.GenerateUUIDv7()
		}
	}

	strategy := input.AssignmentStrategy
	if strategy == "" {
		strategy = contracts.CanonicalCooperativeStickyAssignor
	}

	member := contracts.ConsumerGroupMemberRecord{
		ID:                 id,
		PublicID:           publicID,
		TenantID:           input.TenantID,
		SourceService:      input.SourceService,
		ExternalID:         input.ExternalID,
		CorrelationID:      corrID,
		GroupID:            input.GroupID,
		MemberID:           input.MemberID,
		InstanceID:         input.InstanceID,
		ClientHost:         input.ClientHost,
		AssignmentStrategy: strategy,
		AssignedPartitions: json.RawMessage("[]"),
		GenerationID:       0,
		HeartbeatAt:        time.Now().UTC(),
		Status:             contracts.MemberStatusJoining,
	}

	created, err := s.repo.UpsertMember(ctx, member)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "consumer-groups", "failed upserting member in repository", input.TenantID, map[string]any{"error": err.Error()})
		return nil, err
	}

	span.SetAttributes(
		attribute.String("member.public_id", created.PublicID),
		attribute.String("member.status", string(created.Status)),
	)
	return created, nil
}

func (s *ConsumerGroupsService) ProcessHeartbeat(ctx context.Context, input contracts.HeartbeatInput) (*contracts.HeartbeatResult, error) {
	ctx, span := observability.StartSpan(ctx, "consumer-groups.process_heartbeat")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("member_id", input.MemberID),
	)

	if err := contracts.ValidateHeartbeat(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "consumer-groups", "validation rejected heartbeat", input.TenantID, map[string]any{"error": err.Error()})
		return nil, err
	}

	now := time.Now().UTC()
	genID, status, err := s.repo.Heartbeat(ctx, input.TenantID, input.GroupID, input.MemberID, now)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "consumer-groups", "failed processing heartbeat in repository", input.TenantID, map[string]any{"error": err.Error()})
		return nil, fmt.Errorf("failed processing heartbeat: %w", err)
	}

	mustRejoin := status == contracts.MemberStatusRebalancing || status == contracts.MemberStatusDead
	span.SetAttributes(
		attribute.String("member.status", string(status)),
		attribute.Int64("generation_id", genID),
		attribute.Bool("must_rejoin", mustRejoin),
	)

	return &contracts.HeartbeatResult{
		Status:       status,
		GenerationID: genID,
		MustRejoin:   mustRejoin,
	}, nil
}

func (s *ConsumerGroupsService) SyncRebalance(ctx context.Context, input contracts.RebalanceSyncInput) (*contracts.RebalanceLogRecord, error) {
	ctx, span := observability.StartSpan(ctx, "consumer-groups.sync_rebalance")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("member_id", input.MemberID),
		attribute.Int64("generation_id", input.GenerationID),
		attribute.String("rebalance_reason", input.RebalanceReason),
	)

	if err := contracts.ValidateRebalanceSync(input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "consumer-groups", "validation rejected rebalance sync", input.TenantID, map[string]any{"error": err.Error()})
		return nil, err
	}

	id, _ := utils.GenerateUUIDv7()
	publicID, _ := utils.GeneratePublicID("cgr")

	corrID := input.CorrelationID
	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	revokedJSON, _ := json.Marshal(input.RevokedPartitions)
	assignedJSON, _ := json.Marshal(input.AssignedPartitions)

	logRec := contracts.RebalanceLogRecord{
		ID:                 id,
		PublicID:           publicID,
		TenantID:           input.TenantID,
		SourceService:      input.SourceService,
		ExternalID:         input.ExternalID,
		CorrelationID:      corrID,
		GroupID:            input.GroupID,
		GenerationID:       input.GenerationID,
		RebalanceReason:    input.RebalanceReason,
		MemberCount:        input.MemberCount,
		RevokedPartitions:  revokedJSON,
		AssignedPartitions: assignedJSON,
		DurationMs:         input.DurationMs,
		CreatedAt:          time.Now().UTC(),
	}

	if err := s.repo.RecordRebalance(ctx, logRec); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "consumer-groups", "failed recording rebalance", input.TenantID, map[string]any{"error": err.Error()})
		return nil, fmt.Errorf("failed recording rebalance: %w", err)
	}

	if err := s.repo.UpdatePartitions(ctx, input.TenantID, input.GroupID, input.MemberID, assignedJSON, input.GenerationID); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "consumer-groups", "failed updating member partition assignment", input.TenantID, map[string]any{"error": err.Error()})
		return nil, fmt.Errorf("failed updating member partition assignment: %w", err)
	}

	span.SetAttributes(
		attribute.String("rebalance.public_id", logRec.PublicID),
	)

	return &logRec, nil
}

func (s *ConsumerGroupsService) ListMembers(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerGroupMemberRecord, error) {
	ctx, span := observability.StartSpan(ctx, "consumer-groups.list_members")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("group_id", groupID),
	)

	records, err := s.repo.ListMembers(ctx, tenantID, groupID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "consumer-groups", "failed listing members from repository", tenantID, map[string]any{"error": err.Error()})
		return nil, err
	}

	span.SetAttributes(attribute.Int("members_count", len(records)))
	return records, nil
}



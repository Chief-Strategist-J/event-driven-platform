package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/domain/rules"
	"event-driven-platform/src/features/consumer-groups/infrastructure/ports"
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
	if err := contracts.ValidateRegisterMember(input); err != nil {
		return nil, err
	}
	if err := rules.VerifyAssignorStrategy(input.AssignmentStrategy); err != nil {
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

	return s.repo.UpsertMember(ctx, member)
}

func (s *ConsumerGroupsService) ProcessHeartbeat(ctx context.Context, input contracts.HeartbeatInput) (*contracts.HeartbeatResult, error) {
	if err := contracts.ValidateHeartbeat(input); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	genID, status, err := s.repo.Heartbeat(ctx, input.TenantID, input.GroupID, input.MemberID, now)
	if err != nil {
		return nil, fmt.Errorf("failed processing heartbeat: %w", err)
	}

	mustRejoin := status == contracts.MemberStatusRebalancing || status == contracts.MemberStatusDead
	return &contracts.HeartbeatResult{
		Status:       status,
		GenerationID: genID,
		MustRejoin:   mustRejoin,
	}, nil
}

func (s *ConsumerGroupsService) SyncRebalance(ctx context.Context, input contracts.RebalanceSyncInput) (*contracts.RebalanceLogRecord, error) {
	if err := contracts.ValidateRebalanceSync(input); err != nil {
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
		return nil, fmt.Errorf("failed recording rebalance: %w", err)
	}

	if err := s.repo.UpdatePartitions(ctx, input.TenantID, input.GroupID, input.MemberID, assignedJSON, input.GenerationID); err != nil {
		return nil, fmt.Errorf("failed updating member partition assignment: %w", err)
	}

	return &logRec, nil
}

func (s *ConsumerGroupsService) ListMembers(ctx context.Context, tenantID, groupID string) ([]contracts.ConsumerGroupMemberRecord, error) {
	return s.repo.ListMembers(ctx, tenantID, groupID)
}

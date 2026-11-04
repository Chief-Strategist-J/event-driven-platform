package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/domain/rules"
	"event-driven-platform/src/features/offset-management/infrastructure/ports"
	"event-driven-platform/src/shared/utils"
)

type OffsetManagementService struct {
	repo ports.OffsetManagementRepositoryPort
}

func NewOffsetManagementService(repo ports.OffsetManagementRepositoryPort) *OffsetManagementService {
	return &OffsetManagementService{repo: repo}
}

func (s *OffsetManagementService) CommitOffset(ctx context.Context, input contracts.CommitOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	existing, err := s.repo.GetStoredOffset(ctx, input.TenantID, input.GroupID, input.Topic, input.Partition)
	var currentOffset int64 = 0
	if err == nil && existing != nil {
		currentOffset = existing.StoredOffset
	}

	if err := rules.ValidateOffsetCommit(currentOffset, input.Offset); err != nil {
		return nil, err
	}

	lag := input.LogEndOffset - input.Offset
	if lag < 0 {
		lag = 0
	}

	recordID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}
	publicID, err := utils.GeneratePublicID("cso")
	if err != nil {
		return nil, err
	}

	rec := contracts.ConsumerStoredOffsetRecord{
		ID:            recordID,
		PublicID:      publicID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    input.ExternalID,
		CorrelationID: input.CorrelationID,
		GroupID:       input.GroupID,
		Topic:         input.Topic,
		Partition:     input.Partition,
		StoredOffset:  input.Offset,
		LogEndOffset:  input.LogEndOffset,
		Lag:           lag,
		Metadata:      input.Metadata,
	}

	res, err := s.repo.UpsertStoredOffset(ctx, rec)
	if err != nil {
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("coal")
	auditLog := contracts.ConsumerOffsetAuditLogRecord{
		ID:             auditID,
		PublicID:       auditPublicID,
		TenantID:       input.TenantID,
		SourceService:  input.SourceService,
		ExternalID:     fmt.Sprintf("%s-audit-%d", input.ExternalID, time.Now().UnixNano()),
		CorrelationID:  input.CorrelationID,
		GroupID:        input.GroupID,
		Topic:          input.Topic,
		Partition:      input.Partition,
		OperationType:  contracts.OperationCommit,
		PreviousOffset: currentOffset,
		TargetOffset:   input.Offset,
		Reason:         "Routine commit-after-process",
	}
	_ = s.repo.RecordAuditLog(ctx, auditLog)

	return res, nil
}

func (s *OffsetManagementService) RewindOffset(ctx context.Context, input contracts.RewindOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	if input.TargetOffset < 0 {
		return nil, rules.ErrNegativeOffsetDisallowed
	}

	existing, err := s.repo.GetStoredOffset(ctx, input.TenantID, input.GroupID, input.Topic, input.Partition)
	var prevOffset int64 = 0
	var logEndOffset int64 = 0
	if err == nil && existing != nil {
		prevOffset = existing.StoredOffset
		logEndOffset = existing.LogEndOffset
	}

	lag := logEndOffset - input.TargetOffset
	if lag < 0 {
		lag = 0
	}

	recID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}
	pubID, err := utils.GeneratePublicID("cso")
	if err != nil {
		return nil, err
	}

	rec := contracts.ConsumerStoredOffsetRecord{
		ID:            recID,
		PublicID:      pubID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    input.ExternalID,
		CorrelationID: input.CorrelationID,
		GroupID:       input.GroupID,
		Topic:         input.Topic,
		Partition:     input.Partition,
		StoredOffset:  input.TargetOffset,
		LogEndOffset:  logEndOffset,
		Lag:           lag,
	}

	res, err := s.repo.UpsertStoredOffset(ctx, rec)
	if err != nil {
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("coal")
	auditLog := contracts.ConsumerOffsetAuditLogRecord{
		ID:             auditID,
		PublicID:       auditPublicID,
		TenantID:       input.TenantID,
		SourceService:  input.SourceService,
		ExternalID:     fmt.Sprintf("%s-rewind-%d", input.ExternalID, time.Now().UnixNano()),
		CorrelationID:  input.CorrelationID,
		GroupID:        input.GroupID,
		Topic:          input.Topic,
		Partition:      input.Partition,
		OperationType:  contracts.OperationRewind,
		PreviousOffset: prevOffset,
		TargetOffset:   input.TargetOffset,
		Reason:         input.Reason,
	}
	_ = s.repo.RecordAuditLog(ctx, auditLog)

	return res, nil
}

func (s *OffsetManagementService) FastForwardOffset(ctx context.Context, input contracts.FastForwardOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	existing, err := s.repo.GetStoredOffset(ctx, input.TenantID, input.GroupID, input.Topic, input.Partition)
	if err != nil {
		return nil, err
	}

	targetOffset, err := rules.ComputeFastForwardOffset(existing.StoredOffset, input.ShiftBy, existing.LogEndOffset)
	if err != nil {
		return nil, err
	}

	lag := existing.LogEndOffset - targetOffset
	if lag < 0 {
		lag = 0
	}

	recID, err := utils.GenerateUUIDv7()
	if err != nil {
		return nil, err
	}
	pubID, err := utils.GeneratePublicID("cso")
	if err != nil {
		return nil, err
	}

	rec := contracts.ConsumerStoredOffsetRecord{
		ID:            recID,
		PublicID:      pubID,
		TenantID:      input.TenantID,
		SourceService: input.SourceService,
		ExternalID:    input.ExternalID,
		CorrelationID: input.CorrelationID,
		GroupID:       input.GroupID,
		Topic:         input.Topic,
		Partition:     input.Partition,
		StoredOffset:  targetOffset,
		LogEndOffset:  existing.LogEndOffset,
		Lag:           lag,
	}

	res, err := s.repo.UpsertStoredOffset(ctx, rec)
	if err != nil {
		return nil, err
	}

	auditID, _ := utils.GenerateUUIDv7()
	auditPublicID, _ := utils.GeneratePublicID("coal")
	auditLog := contracts.ConsumerOffsetAuditLogRecord{
		ID:             auditID,
		PublicID:       auditPublicID,
		TenantID:       input.TenantID,
		SourceService:  input.SourceService,
		ExternalID:     fmt.Sprintf("%s-fastforward-%d", input.ExternalID, time.Now().UnixNano()),
		CorrelationID:  input.CorrelationID,
		GroupID:        input.GroupID,
		Topic:          input.Topic,
		Partition:      input.Partition,
		OperationType:  contracts.OperationFastForward,
		PreviousOffset: existing.StoredOffset,
		TargetOffset:   targetOffset,
		Reason:         input.Reason,
	}
	_ = s.repo.RecordAuditLog(ctx, auditLog)

	return res, nil
}

func (s *OffsetManagementService) EvaluateLag(ctx context.Context, tenantID, groupID, topic string, partition int32, rate float64) (*contracts.LagEvaluationResult, error) {
	rec, err := s.repo.GetStoredOffset(ctx, tenantID, groupID, topic, partition)
	if err != nil {
		return nil, err
	}

	severity := rules.EvaluateLagSeverity(rec.StoredOffset, rec.LogEndOffset, time.Minute, rate)
	action := "STEADY_STATE"
	if severity == contracts.LagSeveritySurge {
		action = "SCALE_OUT_CONSUMERS"
	} else if severity == contracts.LagSeverityStalled {
		action = "FIRE_CRITICAL_STALL_ALERT"
	}

	return &contracts.LagEvaluationResult{
		GroupID:      rec.GroupID,
		Topic:        rec.Topic,
		Partition:    rec.Partition,
		StoredOffset: rec.StoredOffset,
		LogEndOffset: rec.LogEndOffset,
		Lag:          rec.Lag,
		Severity:     severity,
		Action:       action,
	}, nil
}

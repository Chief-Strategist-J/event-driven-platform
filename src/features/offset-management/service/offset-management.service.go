package service

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"event-driven-platform/src/features/offset-management/contracts"
	"event-driven-platform/src/features/offset-management/domain/rules"
	"event-driven-platform/src/features/offset-management/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/utils"
)

type OffsetManagementService struct {
	repo ports.OffsetManagementRepositoryPort
}

func NewOffsetManagementService(repo ports.OffsetManagementRepositoryPort) *OffsetManagementService {
	return &OffsetManagementService{repo: repo}
}

func (s *OffsetManagementService) CommitOffset(ctx context.Context, input contracts.CommitOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	ctx, span := observability.StartSpan(ctx, "offset-management.commit_offset")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("topic", input.Topic),
		attribute.Int("partition", int(input.Partition)),
		attribute.Int64("offset", input.Offset),
		attribute.Int64("log_end_offset", input.LogEndOffset),
	)

	existing, err := s.repo.GetStoredOffset(ctx, input.TenantID, input.GroupID, input.Topic, input.Partition)
	var currentOffset int64 = 0
	if err == nil && existing != nil {
		currentOffset = existing.StoredOffset
	}

	if err := rules.ValidateOffsetCommit(currentOffset, input.Offset); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "offset-management", "offset commit validation rejected", input.TenantID, map[string]any{"error": err.Error()})
		return nil, err
	}

	lag := input.LogEndOffset - input.Offset
	if lag < 0 {
		lag = 0
	}

	recordID, err := utils.GenerateUUIDv7()
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	publicID, err := utils.GeneratePublicID("cso")
	if err != nil {
		observability.RecordError(span, err)
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
		CommitEpoch:   1,
		Metadata:      input.Metadata,
	}

	res, err := s.repo.UpsertStoredOffset(ctx, rec)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "offset-management", "failed persisting stored offset in repository", input.TenantID, map[string]any{"error": err.Error()})
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

	span.SetAttributes(
		attribute.String("offset.public_id", res.PublicID),
		attribute.Int64("offset.commit_epoch", res.CommitEpoch),
		attribute.Int64("offset.lag", res.Lag),
	)

	return res, nil
}

func (s *OffsetManagementService) RewindOffset(ctx context.Context, input contracts.RewindOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	ctx, span := observability.StartSpan(ctx, "offset-management.rewind_offset")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("topic", input.Topic),
		attribute.Int("partition", int(input.Partition)),
		attribute.Int64("target_offset", input.TargetOffset),
		attribute.String("reason", input.Reason),
	)

	if input.TargetOffset < 0 {
		err := rules.ErrNegativeOffsetDisallowed
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
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
		observability.RecordError(span, err)
		return nil, err
	}
	pubID, err := utils.GeneratePublicID("cso")
	if err != nil {
		observability.RecordError(span, err)
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
		CommitEpoch:   1,
	}

	res, err := s.repo.UpsertStoredOffset(ctx, rec)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "offset-management", "failed persisting rewound offset", input.TenantID, map[string]any{"error": err.Error()})
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

	span.SetAttributes(
		attribute.String("offset.public_id", res.PublicID),
		attribute.Int64("offset.commit_epoch", res.CommitEpoch),
	)

	return res, nil
}

func (s *OffsetManagementService) FastForwardOffset(ctx context.Context, input contracts.FastForwardOffsetInput) (*contracts.ConsumerStoredOffsetRecord, error) {
	ctx, span := observability.StartSpan(ctx, "offset-management.fast_forward_offset")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("topic", input.Topic),
		attribute.Int("partition", int(input.Partition)),
		attribute.Int64("shift_by", input.ShiftBy),
		attribute.String("reason", input.Reason),
	)

	existing, err := s.repo.GetStoredOffset(ctx, input.TenantID, input.GroupID, input.Topic, input.Partition)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	targetOffset, err := rules.ComputeFastForwardOffset(existing.StoredOffset, input.ShiftBy, existing.LogEndOffset)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		return nil, err
	}

	lag := existing.LogEndOffset - targetOffset
	if lag < 0 {
		lag = 0
	}

	recID, err := utils.GenerateUUIDv7()
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	pubID, err := utils.GeneratePublicID("cso")
	if err != nil {
		observability.RecordError(span, err)
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
		CommitEpoch:   1,
	}

	res, err := s.repo.UpsertStoredOffset(ctx, rec)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "STORAGE_ERROR"))
		observability.LogError(ctx, "offset-management", "failed persisting fast-forward offset", input.TenantID, map[string]any{"error": err.Error()})
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

	span.SetAttributes(
		attribute.String("offset.public_id", res.PublicID),
		attribute.Int64("offset.commit_epoch", res.CommitEpoch),
	)

	return res, nil
}

func (s *OffsetManagementService) EvaluateLag(ctx context.Context, tenantID, groupID, topic string, partition int32, rate float64) (*contracts.LagEvaluationResult, error) {
	ctx, span := observability.StartSpan(ctx, "offset-management.evaluate_lag")
	defer span.End()

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("group_id", groupID),
		attribute.String("topic", topic),
		attribute.Int("partition", int(partition)),
		attribute.Float64("rate", rate),
	)

	rec, err := s.repo.GetStoredOffset(ctx, tenantID, groupID, topic, partition)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "NOT_FOUND"))
		return nil, err
	}

	severity := rules.EvaluateLagSeverity(rec.StoredOffset, rec.LogEndOffset, time.Minute, rate)
	action := "STEADY_STATE"
	if severity == contracts.LagSeveritySurge {
		action = "SCALE_OUT_CONSUMERS"
	} else if severity == contracts.LagSeverityStalled {
		action = "FIRE_CRITICAL_STALL_ALERT"
	}

	span.SetAttributes(
		attribute.String("lag.severity", string(severity)),
		attribute.String("lag.action", action),
		attribute.Int64("lag.current", rec.Lag),
	)

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

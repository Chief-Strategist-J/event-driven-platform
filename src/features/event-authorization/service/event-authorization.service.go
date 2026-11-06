package service

import (
	"context"
	"fmt"
	"time"

	"event-driven-platform/src/features/event-authorization/contracts"
	"event-driven-platform/src/features/event-authorization/domain/machines"
	"event-driven-platform/src/features/event-authorization/domain/rules"
	"event-driven-platform/src/features/event-authorization/infrastructure/ports"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type EventAuthorizationService struct {
	repo   ports.EventAuthorizationRepositoryPort
	tracer trace.Tracer
}

func NewEventAuthorizationService(repo ports.EventAuthorizationRepositoryPort) *EventAuthorizationService {
	return &EventAuthorizationService{
		repo:   repo,
		tracer: otel.Tracer("EventAuthorizationService"),
	}
}

func (s *EventAuthorizationService) ProvisionPolicy(ctx context.Context, input *contracts.CreatePolicyInput) (*contracts.AuthPolicyRecord, error) {
	ctx, span := s.tracer.Start(ctx, "EventAuthorizationService.ProvisionPolicy",
		trace.WithAttributes(
			attribute.String("auth.tenant_id", input.TenantID),
			attribute.String("auth.policy_name", input.PolicyName),
			attribute.String("auth.principal_id", input.PrincipalID),
			attribute.String("auth.resource_type", string(input.ResourceType)),
		),
	)
	defer span.End()

	record, err := contracts.NormalizeCreatePolicyInput(input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if record.ResourceType == contracts.ResourceTopic && record.Operation == contracts.OpPublish && record.Effect == contracts.EffectAllow {
		writerCount, err := s.repo.GetTopicWriterCount(ctx, record.TenantID, record.ResourcePattern)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		if writerCount > 0 {
			err := fmt.Errorf("%w: topic %s already has an active writer", contracts.ErrSingleWriterViolation, record.ResourcePattern)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	}

	if err := s.repo.SavePolicy(ctx, record); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to save policy: %w", err)
	}

	span.SetAttributes(attribute.String("auth.policy_public_id", record.PublicID))
	span.SetStatus(codes.Ok, "policy provisioned")
	return record, nil
}

func (s *EventAuthorizationService) EvaluateAccess(ctx context.Context, input *contracts.EvaluateAccessInput) (*contracts.EvaluateAccessOutput, error) {
	ctx, span := s.tracer.Start(ctx, "EventAuthorizationService.EvaluateAccess",
		trace.WithAttributes(
			attribute.String("auth.tenant_id", input.TenantID),
			attribute.String("auth.principal_id", input.PrincipalID),
			attribute.String("auth.resource_name", input.ResourceName),
			attribute.String("auth.operation", string(input.Operation)),
		),
	)
	defer span.End()

	normalized, err := contracts.NormalizeEvaluateAccessInput(input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	policies, err := s.repo.FindMatchingPolicies(ctx, normalized.TenantID, normalized.PrincipalID, normalized.ResourceType, normalized.Operation)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	decision, matchedPolicyID, reason := rules.EvaluatePolicies(policies, normalized.ResourceName, normalized.Operation)

	evalTime := time.Now().UTC()
	auditPubID, _ := contracts.GeneratePublicID("aal")
	auditRecord := &contracts.AuthAuditRecord{
		ID:              uuid.New(),
		PublicID:        auditPubID,
		TenantID:        normalized.TenantID,
		SourceService:   normalized.SourceService,
		ExternalID:      normalized.ExternalID,
		CorrelationID:   normalized.CorrelationID,
		PrincipalID:     normalized.PrincipalID,
		PrincipalType:   normalized.PrincipalType,
		ResourceType:    normalized.ResourceType,
		ResourceName:    normalized.ResourceName,
		Operation:       normalized.Operation,
		Decision:        decision,
		MatchedPolicyID: matchedPolicyID,
		Reason:          reason,
		EvaluatedAt:     evalTime,
	}

	if err := s.repo.SaveAuditLog(ctx, auditRecord); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to save audit log: %w", err)
	}

	span.SetAttributes(
		attribute.String("auth.decision", string(decision)),
		attribute.String("auth.matched_policy_id", matchedPolicyID),
	)
	span.SetStatus(codes.Ok, "evaluation complete")

	return &contracts.EvaluateAccessOutput{
		Decision:        decision,
		MatchedPolicyID: matchedPolicyID,
		Reason:          reason,
		EvaluatedAt:     evalTime.Format(time.RFC3339),
	}, nil
}

func (s *EventAuthorizationService) UpdatePolicyState(ctx context.Context, publicID string, newState contracts.PolicyState) error {
	ctx, span := s.tracer.Start(ctx, "EventAuthorizationService.UpdatePolicyState",
		trace.WithAttributes(
			attribute.String("auth.policy_public_id", publicID),
			attribute.String("auth.new_state", string(newState)),
		),
	)
	defer span.End()

	pol, err := s.repo.GetPolicyByPublicID(ctx, publicID)
	if err != nil {
		return err
	}
	if pol == nil {
		return fmt.Errorf("policy %s not found", publicID)
	}

	if !machines.CanTransitionPolicyState(pol.State, newState) {
		err := fmt.Errorf("%w: cannot transition from %s to %s", machines.ErrIllegalPolicyTransition, pol.State, newState)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := s.repo.UpdatePolicyState(ctx, publicID, newState); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetStatus(codes.Ok, "policy state updated")
	return nil
}

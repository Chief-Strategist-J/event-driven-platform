package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/event-driven-communication/contracts"
	"event-driven-platform/src/features/event-driven-communication/domain/machines"
	"event-driven-platform/src/features/event-driven-communication/domain/rules"
	"event-driven-platform/src/features/event-driven-communication/infrastructure/ports"
)

var (
	ErrSagaInstanceClosed = errors.New("saga instance is closed or already compensated")
)

type EventCommunicationService struct {
	repo ports.EventCommunicationRepositoryPort
	sm   *machines.SagaStateMachine
}

func NewEventCommunicationService(repo ports.EventCommunicationRepositoryPort) *EventCommunicationService {
	return &EventCommunicationService{
		repo: repo,
		sm:   machines.NewSagaStateMachine(),
	}
}

func (s *EventCommunicationService) RegisterSagaDefinition(
	ctx context.Context,
	def contracts.SagaDefinition,
) (contracts.SagaDefinition, error) {
	if err := rules.ValidateSagaStepOrdering(def.Steps); err != nil {
		return contracts.SagaDefinition{}, err
	}

	def.ID = uuid.New()
	def.PublicID = "sgd_" + uuid.New().String()[:18]
	def.TotalSteps = len(def.Steps)
	def.IsActive = true
	def.CreatedAt = time.Now().UTC()
	def.UpdatedAt = time.Now().UTC()

	if err := s.repo.SaveSagaDefinition(ctx, def); err != nil {
		return contracts.SagaDefinition{}, err
	}
	return def, nil
}

func (s *EventCommunicationService) StartSaga(
	ctx context.Context,
	cmd contracts.StartSagaCommand,
) (contracts.StartSagaResult, error) {
	if err := rules.ValidateTimeoutSeconds(cmd.TimeoutSeconds); err != nil {
		return contracts.StartSagaResult{}, err
	}

	def, err := s.repo.GetSagaDefinition(ctx, cmd.TenantID, cmd.SagaDefinitionID)
	if err != nil {
		return contracts.StartSagaResult{}, err
	}

	instID := uuid.New()
	pubID := "sgi_" + uuid.New().String()[:18]
	now := time.Now().UTC()
	deadline := now.Add(time.Duration(cmd.TimeoutSeconds) * time.Second)

	var corrUUID uuid.UUID
	if parsed, err := uuid.Parse(cmd.CorrelationID); err == nil {
		corrUUID = parsed
	} else {
		corrUUID = uuid.New()
	}

	inst := contracts.SagaInstance{
		ID:               instID,
		PublicID:         pubID,
		TenantID:         cmd.TenantID,
		SourceService:    cmd.SourceService,
		ExternalID:       cmd.ExternalID,
		CorrelationID:    corrUUID,
		SagaDefinitionID: cmd.SagaDefinitionID,
		CurrentStepIndex: 0,
		State:            contracts.SagaStateInProgress,
		DeadlineAt:       deadline,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	steps := make([]contracts.SagaStepRecord, len(def.Steps))
	for i, stepDef := range def.Steps {
		payload := "{}"
		if i == 0 && cmd.InitialPayload != "" {
			payload = cmd.InitialPayload
		}
		steps[i] = contracts.SagaStepRecord{
			ID:                  uuid.New(),
			PublicID:            "sgs_" + uuid.New().String()[:18],
			TenantID:            cmd.TenantID,
			SourceService:       cmd.SourceService,
			ExternalID:          cmd.ExternalID + "_step_" + stepDef.StepName,
			CorrelationID:       corrUUID,
			SagaInstanceID:      pubID,
			StepIndex:           stepDef.StepIndex,
			StepName:            stepDef.StepName,
			StepType:            stepDef.StepType,
			State:               contracts.StepStatePending,
			CommandPayload:      payload,
			ReplyPayload:        "{}",
			CompensationPayload: "{}",
			DeadlineAt:          now.Add(time.Duration(stepDef.TimeoutSeconds) * time.Second),
			CreatedAt:           now,
			UpdatedAt:           now,
		}
	}

	if err := s.repo.CreateSagaInstance(ctx, inst, steps); err != nil {
		return contracts.StartSagaResult{}, err
	}

	firstStepName := "none"
	if len(def.Steps) > 0 {
		firstStepName = def.Steps[0].StepName
	}

	return contracts.StartSagaResult{
		SagaInstanceID: pubID,
		CurrentStep:    firstStepName,
		State:          contracts.SagaStateInProgress,
	}, nil
}

func (s *EventCommunicationService) ProcessStepReply(
	ctx context.Context,
	cmd contracts.ProcessStepReplyCommand,
) (contracts.ProcessStepReplyResult, error) {
	inst, err := s.repo.GetSagaInstance(ctx, cmd.TenantID, cmd.SagaInstanceID)
	if err != nil {
		return contracts.ProcessStepReplyResult{}, err
	}

	steps, err := s.repo.GetSagaSteps(ctx, cmd.TenantID, cmd.SagaInstanceID)
	if err != nil {
		return contracts.ProcessStepReplyResult{}, err
	}

	if cmd.StepIndex < 0 || cmd.StepIndex >= len(steps) {
		return contracts.ProcessStepReplyResult{}, rules.ErrStepIndexOutOfBounds
	}

	targetStep := steps[cmd.StepIndex]
	isLate := rules.ClassifyLateReply(inst.State, targetStep.State)

	replyID := uuid.New()
	pubReplyID := "acr_" + uuid.New().String()[:18]
	var corrUUID uuid.UUID
	if parsed, err := uuid.Parse(inst.CorrelationID.String()); err == nil {
		corrUUID = parsed
	} else {
		corrUUID = uuid.New()
	}

	_ = s.repo.SaveCommandReply(ctx, contracts.AsyncCommandReply{
		ID:            replyID,
		PublicID:      pubReplyID,
		TenantID:      cmd.TenantID,
		SourceService: cmd.SourceService,
		ExternalID:    cmd.CommandID,
		CorrelationID: corrUUID,
		CommandID:     cmd.CommandID,
		Outcome:       cmd.Outcome,
		IsLateReply:   isLate,
		ReplyPayload:  cmd.ReplyPayload,
		CreatedAt:     time.Now().UTC(),
	})

	if isLate {
		return contracts.ProcessStepReplyResult{
			SagaInstanceID: cmd.SagaInstanceID,
			NextStepIndex:  inst.CurrentStepIndex,
			SagaState:      inst.State,
			Compensation:   false,
		}, nil
	}

	stepState := contracts.StepStateSucceeded
	if cmd.Outcome != contracts.OutcomeSucceeded {
		stepState = contracts.StepStateFailed
	}

	_ = s.repo.UpdateStepState(ctx, ports.UpdateStepParams{
		TenantID:     cmd.TenantID,
		InstanceID:   cmd.SagaInstanceID,
		StepIndex:    cmd.StepIndex,
		State:        stepState,
		ReplyPayload: cmd.ReplyPayload,
	})

	updatedInst, triggerComp, err := s.sm.TransitionStep(inst, cmd.Outcome, len(steps))
	if err != nil {
		return contracts.ProcessStepReplyResult{}, err
	}

	_ = s.repo.UpdateSagaInstanceState(ctx, ports.UpdateSagaInstanceParams{
		TenantID:      cmd.TenantID,
		InstanceID:    cmd.SagaInstanceID,
		State:         updatedInst.State,
		NextStepIndex: updatedInst.CurrentStepIndex,
	})

	return contracts.ProcessStepReplyResult{
		SagaInstanceID: cmd.SagaInstanceID,
		NextStepIndex:  updatedInst.CurrentStepIndex,
		SagaState:      updatedInst.State,
		Compensation:   triggerComp,
	}, nil
}

func (s *EventCommunicationService) TriggerCompensation(
	ctx context.Context,
	cmd contracts.TriggerCompensationCommand,
) (contracts.TriggerCompensationResult, error) {
	inst, err := s.repo.GetSagaInstance(ctx, cmd.TenantID, cmd.SagaInstanceID)
	if err != nil {
		return contracts.TriggerCompensationResult{}, err
	}

	if inst.State == contracts.SagaStateCompensated || inst.State == contracts.SagaStateSucceeded {
		return contracts.TriggerCompensationResult{}, ErrSagaInstanceClosed
	}

	steps, err := s.repo.GetSagaSteps(ctx, cmd.TenantID, cmd.SagaInstanceID)
	if err != nil {
		return contracts.TriggerCompensationResult{}, err
	}

	compensatedCount := 0
	for i := inst.CurrentStepIndex; i >= 0; i-- {
		if i < len(steps) {
			step := steps[i]
			if step.StepType == contracts.StepTypeCompensable && (step.State == contracts.StepStateSucceeded || step.State == contracts.StepStateSent) {
				_ = s.repo.UpdateStepState(ctx, ports.UpdateStepParams{
					TenantID:     cmd.TenantID,
					InstanceID:   cmd.SagaInstanceID,
					StepIndex:    step.StepIndex,
					State:        contracts.StepStateCompensated,
					ReplyPayload: `{"compensated":true}`,
				})
				compensatedCount++
			}
		}
	}

	_ = s.repo.UpdateSagaInstanceState(ctx, ports.UpdateSagaInstanceParams{
		TenantID:      cmd.TenantID,
		InstanceID:    cmd.SagaInstanceID,
		State:         contracts.SagaStateCompensated,
		NextStepIndex: inst.CurrentStepIndex,
	})

	return contracts.TriggerCompensationResult{
		SagaInstanceID:       cmd.SagaInstanceID,
		CompensatedStepCount: compensatedCount,
		SagaState:            contracts.SagaStateCompensated,
	}, nil
}

func (s *EventCommunicationService) DispatchAsyncCommand(
	ctx context.Context,
	req contracts.DispatchAsyncCommandRequest,
) (contracts.DispatchAsyncCommandResult, error) {
	if strings.TrimSpace(req.TargetTopic) == "" {
		return contracts.DispatchAsyncCommandResult{}, errors.New("target_topic cannot be empty")
	}
	if strings.TrimSpace(req.ReplyToTopic) == "" {
		return contracts.DispatchAsyncCommandResult{}, errors.New("reply_to_topic cannot be empty")
	}

	cmdID := "cmd_" + uuid.New().String()[:18]
	return contracts.DispatchAsyncCommandResult{
		CommandID:    cmdID,
		Status:       "DISPATCHED_TO_OUTBOX",
		ReplyToTopic: req.ReplyToTopic,
	}, nil
}

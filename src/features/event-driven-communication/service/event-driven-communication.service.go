package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"event-driven-platform/src/features/event-driven-communication/contracts"
	"event-driven-platform/src/features/event-driven-communication/domain/machines"
	"event-driven-platform/src/features/event-driven-communication/domain/rules"
	"event-driven-platform/src/features/event-driven-communication/infrastructure/ports"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/utils"
)

var (
	ErrInvalidCommand     = errors.New("invalid command")
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
	def.PublicID = contracts.GeneratePublicID("sgd")
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
	pubID := contracts.GeneratePublicID("sgi")
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
			PublicID:            contracts.GeneratePublicID("sgs"),
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
	pubReplyID := contracts.GeneratePublicID("acr")
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

// DispatchAsyncCommand persists the command to the transactional outbox
// (aggregate_type = target topic, which the relay uses as the Kafka topic).
// The command is only QUEUED_IN_OUTBOX: delivery happens asynchronously.
func (s *EventCommunicationService) DispatchAsyncCommand(
	ctx context.Context,
	req contracts.DispatchAsyncCommandRequest,
) (contracts.DispatchAsyncCommandResult, error) {
	req.TargetTopic = strings.TrimSpace(req.TargetTopic)
	req.ReplyToTopic = strings.TrimSpace(req.ReplyToTopic)
	req.CommandName = strings.TrimSpace(req.CommandName)
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.SourceService = strings.TrimSpace(req.SourceService)
	req.ExternalID = strings.TrimSpace(req.ExternalID)

	switch {
	case req.TargetTopic == "":
		return contracts.DispatchAsyncCommandResult{}, fmt.Errorf("%w: target_topic cannot be empty", ErrInvalidCommand)
	case req.ReplyToTopic == "":
		return contracts.DispatchAsyncCommandResult{}, fmt.Errorf("%w: reply_to_topic cannot be empty", ErrInvalidCommand)
	case req.TenantID == "":
		return contracts.DispatchAsyncCommandResult{}, fmt.Errorf("%w: tenant_id cannot be empty", ErrInvalidCommand)
	case req.SourceService == "":
		return contracts.DispatchAsyncCommandResult{}, fmt.Errorf("%w: source_service cannot be empty", ErrInvalidCommand)
	case req.CommandName == "":
		return contracts.DispatchAsyncCommandResult{}, fmt.Errorf("%w: command_name cannot be empty", ErrInvalidCommand)
	}

	var cmdPayload json.RawMessage = json.RawMessage("{}")
	if strings.TrimSpace(req.CommandPayload) != "" {
		if !json.Valid([]byte(req.CommandPayload)) {
			return contracts.DispatchAsyncCommandResult{}, fmt.Errorf("%w: command_payload must be valid JSON", ErrInvalidCommand)
		}
		cmdPayload = json.RawMessage(req.CommandPayload)
	}

	cmdID := "cmd_" + uuid.New().String()[:18]
	externalID := req.ExternalID
	if externalID == "" {
		externalID = cmdID
	}
	corrUUID, err := uuid.Parse(req.CorrelationID)
	if err != nil {
		corrUUID = uuid.New()
	}
	publicID, err := utils.GeneratePublicID(constants.PrefixOutbox)
	if err != nil {
		return contracts.DispatchAsyncCommandResult{}, err
	}
	envelope, err := json.Marshal(map[string]any{
		"command_id":      cmdID,
		"command_name":    req.CommandName,
		"reply_to_topic":  req.ReplyToTopic,
		"timeout_seconds": req.TimeoutSeconds,
		"payload":         cmdPayload,
	})
	if err != nil {
		return contracts.DispatchAsyncCommandResult{}, err
	}

	if err := s.repo.SaveOutboundCommand(ctx, ports.OutboundCommand{
		ID:            uuid.New(),
		PublicID:      publicID,
		TenantID:      req.TenantID,
		SourceService: req.SourceService,
		ExternalID:    externalID,
		CorrelationID: corrUUID,
		CausationID:   cmdID,
		AggregateType: req.TargetTopic,
		AggregateID:   cmdID,
		EventType:     req.CommandName,
		SchemaVersion: constants.APIVersionV1,
		Payload:       string(envelope),
		Headers:       map[string]string{"reply_to_topic": req.ReplyToTopic},
	}); err != nil {
		return contracts.DispatchAsyncCommandResult{}, err
	}

	return contracts.DispatchAsyncCommandResult{
		CommandID:    cmdID,
		Status:       "QUEUED_IN_OUTBOX",
		ReplyToTopic: req.ReplyToTopic,
	}, nil
}

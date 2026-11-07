package tests

import (
	"errors"
	"context"
	"testing"

	"github.com/google/uuid"

	"event-driven-platform/src/features/event-driven-communication/contracts"
	"event-driven-platform/src/features/event-driven-communication/domain/machines"
	"event-driven-platform/src/features/event-driven-communication/domain/rules"
	"event-driven-platform/src/features/event-driven-communication/infrastructure/ports"
	"event-driven-platform/src/features/event-driven-communication/service"
)

func TestSagaStepOrderingValidation(t *testing.T) {
	validSteps := []contracts.SagaStepDefinition{
		{
			StepIndex:        0,
			StepName:         "ReserveCredit",
			StepType:         contracts.StepTypeCompensable,
			CompensationName: "ReleaseCredit",
			TimeoutSeconds:   5,
		},
		{
			StepIndex:      1,
			StepName:       "CapturePayment",
			StepType:       contracts.StepTypePivot,
			TimeoutSeconds: 5,
		},
		{
			StepIndex:      2,
			StepName:       "SendReceiptEmail",
			StepType:       contracts.StepTypeRetriable,
			TimeoutSeconds: 5,
		},
	}
	if err := rules.ValidateSagaStepOrdering(validSteps); err != nil {
		t.Fatalf("expected valid step ordering: %v", err)
	}

	invalidSteps := []contracts.SagaStepDefinition{
		{
			StepIndex:      0,
			StepName:       "CapturePayment",
			StepType:       contracts.StepTypePivot,
			TimeoutSeconds: 5,
		},
		{
			StepIndex:        1,
			StepName:         "ReserveCredit",
			StepType:         contracts.StepTypeCompensable,
			CompensationName: "ReleaseCredit",
			TimeoutSeconds:   5,
		},
	}
	if err := rules.ValidateSagaStepOrdering(invalidSteps); err == nil {
		t.Fatalf("expected failure for compensable placed after pivot")
	}
}

func TestLateReplyClassification(t *testing.T) {
	if !rules.ClassifyLateReply(contracts.SagaStateCompensating, contracts.StepStatePending) {
		t.Fatalf("expected late reply for compensating saga")
	}
	if !rules.ClassifyLateReply(contracts.SagaStateInProgress, contracts.StepStateTimedOut) {
		t.Fatalf("expected late reply for timed out step")
	}
	if rules.ClassifyLateReply(contracts.SagaStateInProgress, contracts.StepStateSent) {
		t.Fatalf("expected normal reply for in-progress step")
	}
}

func TestSagaStateMachineTransitions(t *testing.T) {
	sm := machines.NewSagaStateMachine()
	inst := contracts.SagaInstance{
		State:            contracts.SagaStateInProgress,
		CurrentStepIndex: 0,
	}

	next, comp, err := sm.TransitionStep(inst, contracts.OutcomeSucceeded, 3)
	if err != nil {
		t.Fatalf("expected step advance: %v", err)
	}
	if next.CurrentStepIndex != 1 || comp {
		t.Fatalf("expected next step 1 without compensation")
	}

	finalStep, comp, err := sm.TransitionStep(next, contracts.OutcomeSucceeded, 2)
	if err != nil {
		t.Fatalf("expected step finalization: %v", err)
	}
	if finalStep.State != contracts.SagaStateSucceeded || comp {
		t.Fatalf("expected SUCCEEDED saga state")
	}

	failingInst := contracts.SagaInstance{
		State:            contracts.SagaStateInProgress,
		CurrentStepIndex: 1,
	}
	failedNext, comp, err := sm.TransitionStep(failingInst, contracts.OutcomeFailed, 3)
	if err != nil {
		t.Fatalf("expected failure transition: %v", err)
	}
	if failedNext.State != contracts.SagaStateCompensating || !comp {
		t.Fatalf("expected COMPENSATING with comp=true")
	}
}

type memoryCommRepo struct {
	outbound    []ports.OutboundCommand
	definitions map[string]contracts.SagaDefinition
	instances   map[string]contracts.SagaInstance
	steps       map[string][]contracts.SagaStepRecord
	replies     map[string]contracts.AsyncCommandReply
}

func newMemoryCommRepo() *memoryCommRepo {
	return &memoryCommRepo{
		definitions: make(map[string]contracts.SagaDefinition),
		instances:   make(map[string]contracts.SagaInstance),
		steps:       make(map[string][]contracts.SagaStepRecord),
		replies:     make(map[string]contracts.AsyncCommandReply),
	}
}

func (m *memoryCommRepo) SaveSagaDefinition(_ context.Context, def contracts.SagaDefinition) error {
	m.definitions[def.PublicID] = def
	return nil
}

func (m *memoryCommRepo) GetSagaDefinition(_ context.Context, _ string, defID string) (contracts.SagaDefinition, error) {
	if def, ok := m.definitions[defID]; ok {
		return def, nil
	}
	return contracts.SagaDefinition{
		PublicID:   defID,
		TotalSteps: 1,
		Steps: []contracts.SagaStepDefinition{
			{
				StepIndex:      0,
				StepName:       "DefaultStep",
				StepType:       contracts.StepTypePivot,
				TimeoutSeconds: 5,
			},
		},
	}, nil
}

func (m *memoryCommRepo) CreateSagaInstance(_ context.Context, inst contracts.SagaInstance, steps []contracts.SagaStepRecord) error {
	m.instances[inst.PublicID] = inst
	m.steps[inst.PublicID] = steps
	return nil
}

func (m *memoryCommRepo) GetSagaInstance(_ context.Context, _ string, instID string) (contracts.SagaInstance, error) {
	if inst, ok := m.instances[instID]; ok {
		return inst, nil
	}
	return contracts.SagaInstance{}, nil
}

func (m *memoryCommRepo) UpdateSagaInstanceState(_ context.Context, params ports.UpdateSagaInstanceParams) error {
	if inst, ok := m.instances[params.InstanceID]; ok {
		inst.State = params.State
		inst.CurrentStepIndex = params.NextStepIndex
		m.instances[params.InstanceID] = inst
	}
	return nil
}

func (m *memoryCommRepo) GetSagaSteps(_ context.Context, _ string, instID string) ([]contracts.SagaStepRecord, error) {
	return m.steps[instID], nil
}

func (m *memoryCommRepo) UpdateStepState(_ context.Context, params ports.UpdateStepParams) error {
	steps := m.steps[params.InstanceID]
	if params.StepIndex >= 0 && params.StepIndex < len(steps) {
		steps[params.StepIndex].State = params.State
		steps[params.StepIndex].ReplyPayload = params.ReplyPayload
		m.steps[params.InstanceID] = steps
	}
	return nil
}

func (m *memoryCommRepo) SaveOutboundCommand(_ context.Context, cmd ports.OutboundCommand) error {
	m.outbound = append(m.outbound, cmd)
	return nil
}

func (m *memoryCommRepo) SaveCommandReply(_ context.Context, reply contracts.AsyncCommandReply) error {
	m.replies[reply.CommandID] = reply
	return nil
}

func TestSagaServiceLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryCommRepo()
	svc := service.NewEventCommunicationService(repo)

	defCmd := contracts.SagaDefinition{
		TenantID:      "tenant_1",
		SourceService: "order-service",
		ExternalID:    "ext_def_1",
		CorrelationID: uuid.New(),
		SagaName:      "order_fulfillment_saga",
		Steps: []contracts.SagaStepDefinition{
			{
				StepIndex:        0,
				StepName:         "ReserveInventory",
				StepType:         contracts.StepTypeCompensable,
				CompensationName: "ReleaseInventory",
				TimeoutSeconds:   10,
			},
			{
				StepIndex:      1,
				StepName:       "CapturePayment",
				StepType:       contracts.StepTypePivot,
				TimeoutSeconds: 10,
			},
		},
	}
	def, err := svc.RegisterSagaDefinition(ctx, defCmd)
	if err != nil {
		t.Fatalf("failed registering definition: %v", err)
	}

	startRes, err := svc.StartSaga(ctx, contracts.StartSagaCommand{
		TenantID:         "tenant_1",
		SourceService:    "order-service",
		ExternalID:       "ext_order_123",
		CorrelationID:    uuid.New().String(),
		SagaDefinitionID: def.PublicID,
		InitialPayload:   `{"order_id":"123"}`,
		TimeoutSeconds:   60,
	})
	if err != nil {
		t.Fatalf("failed starting saga: %v", err)
	}

	replyRes, err := svc.ProcessStepReply(ctx, contracts.ProcessStepReplyCommand{
		TenantID:       "tenant_1",
		SourceService:  "inventory-service",
		SagaInstanceID: startRes.SagaInstanceID,
		StepIndex:      0,
		CommandID:      "cmd_inv_1",
		Outcome:        contracts.OutcomeSucceeded,
		ReplyPayload:   `{"reserved":true}`,
	})
	if err != nil {
		t.Fatalf("failed processing step reply: %v", err)
	}
	if replyRes.NextStepIndex != 1 {
		t.Fatalf("expected next step index 1, got %d", replyRes.NextStepIndex)
	}

	compRes, err := svc.TriggerCompensation(ctx, contracts.TriggerCompensationCommand{
		TenantID:       "tenant_1",
		SourceService:  "order-service",
		SagaInstanceID: startRes.SagaInstanceID,
		Reason:         "Customer cancelled checkout",
	})
	if err != nil {
		t.Fatalf("failed triggering compensation: %v", err)
	}
	if compRes.SagaState != contracts.SagaStateCompensated {
		t.Fatalf("expected COMPENSATED state, got %s", compRes.SagaState)
	}
}

func TestDispatchAsyncCommandPersistsOutbox(t *testing.T) {
	repo := newMemoryCommRepo()
	svc := service.NewEventCommunicationService(repo)

	res, err := svc.DispatchAsyncCommand(context.Background(), contracts.DispatchAsyncCommandRequest{
		TenantID: "t1", SourceService: "svc", TargetTopic: "orders.cmd", ReplyToTopic: "orders.reply",
		CommandName: "ReserveStock", CommandPayload: `{"sku":"a"}`,
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if res.Status != "QUEUED_IN_OUTBOX" || len(repo.outbound) != 1 {
		t.Fatalf("expected one outbox record, got status=%s n=%d", res.Status, len(repo.outbound))
	}
	if repo.outbound[0].AggregateType != "orders.cmd" || repo.outbound[0].AggregateID != res.CommandID {
		t.Fatalf("unexpected outbox record: %+v", repo.outbound[0])
	}

	_, err = svc.DispatchAsyncCommand(context.Background(), contracts.DispatchAsyncCommandRequest{
		TenantID: "t1", SourceService: "svc", TargetTopic: "x", ReplyToTopic: "y", CommandName: "c", CommandPayload: "{bad",
	})
	if !errors.Is(err, service.ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand, got %v", err)
	}
}

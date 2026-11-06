package ports

import (
	"context"

	"event-driven-platform/src/features/event-driven-communication/contracts"
)

type UpdateSagaInstanceParams struct {
	TenantID      string
	InstanceID    string
	State         contracts.SagaState
	NextStepIndex int
}

type UpdateStepParams struct {
	TenantID     string
	InstanceID   string
	StepIndex    int
	State        contracts.StepState
	ReplyPayload string
}

type EventCommunicationRepositoryPort interface {
	SaveSagaDefinition(ctx context.Context, def contracts.SagaDefinition) error
	GetSagaDefinition(ctx context.Context, tenantID string, defID string) (contracts.SagaDefinition, error)
	CreateSagaInstance(ctx context.Context, inst contracts.SagaInstance, steps []contracts.SagaStepRecord) error
	GetSagaInstance(ctx context.Context, tenantID string, instID string) (contracts.SagaInstance, error)
	UpdateSagaInstanceState(ctx context.Context, params UpdateSagaInstanceParams) error
	GetSagaSteps(ctx context.Context, tenantID string, instID string) ([]contracts.SagaStepRecord, error)
	UpdateStepState(ctx context.Context, params UpdateStepParams) error
	SaveCommandReply(ctx context.Context, reply contracts.AsyncCommandReply) error
}

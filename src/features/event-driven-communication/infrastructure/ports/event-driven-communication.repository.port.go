package ports

import (
	"context"
	"errors"

	"github.com/google/uuid"

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

// ErrDuplicateCommand is returned when external_id was already used for the tenant and source service.
var ErrDuplicateCommand = errors.New("a command with this external_id already exists for the tenant and source service")

// OutboundCommand is a command persisted to the transactional outbox. The relay
// publishes it to the Kafka topic named by AggregateType.
type OutboundCommand struct {
	ID            uuid.UUID
	PublicID      string
	TenantID      string
	SourceService string
	ExternalID    string
	CorrelationID uuid.UUID
	CausationID   string
	AggregateType string // target topic
	AggregateID   string // command id
	EventType     string // command name
	SchemaVersion string
	Payload       string // JSON object
	Headers       map[string]string
}

type EventCommunicationRepositoryPort interface {
	SaveOutboundCommand(ctx context.Context, cmd OutboundCommand) error
	SaveSagaDefinition(ctx context.Context, def contracts.SagaDefinition) error
	GetSagaDefinition(ctx context.Context, tenantID string, defID string) (contracts.SagaDefinition, error)
	CreateSagaInstance(ctx context.Context, inst contracts.SagaInstance, steps []contracts.SagaStepRecord) error
	GetSagaInstance(ctx context.Context, tenantID string, instID string) (contracts.SagaInstance, error)
	UpdateSagaInstanceState(ctx context.Context, params UpdateSagaInstanceParams) error
	GetSagaSteps(ctx context.Context, tenantID string, instID string) ([]contracts.SagaStepRecord, error)
	UpdateStepState(ctx context.Context, params UpdateStepParams) error
	SaveCommandReply(ctx context.Context, reply contracts.AsyncCommandReply) error
}


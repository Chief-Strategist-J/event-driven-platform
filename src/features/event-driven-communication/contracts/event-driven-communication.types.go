package contracts

import (
	"time"

	"github.com/google/uuid"
)

type StepType string

const (
	StepTypeCompensable StepType = "COMPENSABLE"
	StepTypePivot       StepType = "PIVOT"
	StepTypeRetriable   StepType = "RETRIABLE"
)

type SagaState string

const (
	SagaStateInProgress         SagaState = "IN_PROGRESS"
	SagaStateSucceeded          SagaState = "SUCCEEDED"
	SagaStateCompensating       SagaState = "COMPENSATING"
	SagaStateCompensated        SagaState = "COMPENSATED"
	SagaStateFailed             SagaState = "FAILED"
	SagaStateManualIntervention SagaState = "MANUAL_INTERVENTION"
)

type StepState string

const (
	StepStatePending      StepState = "PENDING"
	StepStateSent         StepState = "SENT"
	StepStateSucceeded    StepState = "SUCCEEDED"
	StepStateFailed       StepState = "FAILED"
	StepStateTimedOut     StepState = "TIMED_OUT"
	StepStateCompensating StepState = "COMPENSATING"
	StepStateCompensated  StepState = "COMPENSATED"
)

type CommandOutcome string

const (
	OutcomeSucceeded CommandOutcome = "SUCCEEDED"
	OutcomeRejected  CommandOutcome = "REJECTED"
	OutcomeFailed    CommandOutcome = "FAILED"
	OutcomeTimedOut  CommandOutcome = "TIMED_OUT"
)

type SagaStepDefinition struct {
	StepIndex        int      `json:"step_index"`
	StepName         string   `json:"step_name"`
	StepType         StepType `json:"step_type"`
	TargetTopic      string   `json:"target_topic"`
	TimeoutSeconds   int      `json:"timeout_seconds"`
	CompensationName string   `json:"compensation_name"`
}

type SagaDefinition struct {
	ID            uuid.UUID            `json:"id"`
	PublicID      string               `json:"public_id"`
	TenantID      string               `json:"tenant_id"`
	SourceService string               `json:"source_service"`
	ExternalID    string               `json:"external_id"`
	CorrelationID uuid.UUID            `json:"correlation_id"`
	SagaName      string               `json:"saga_name"`
	TotalSteps    int                  `json:"total_steps"`
	Steps         []SagaStepDefinition `json:"steps"`
	IsActive      bool                 `json:"is_active"`
	CreatedAt     time.Time            `json:"created_at"`
	UpdatedAt     time.Time            `json:"updated_at"`
}

type SagaInstance struct {
	ID               uuid.UUID `json:"id"`
	PublicID         string    `json:"public_id"`
	TenantID         string    `json:"tenant_id"`
	SourceService    string    `json:"source_service"`
	ExternalID       string    `json:"external_id"`
	CorrelationID    uuid.UUID `json:"correlation_id"`
	SagaDefinitionID string    `json:"saga_definition_id"`
	CurrentStepIndex int       `json:"current_step_index"`
	State            SagaState `json:"state"`
	DeadlineAt       time.Time `json:"deadline_at"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type SagaStepRecord struct {
	ID                  uuid.UUID `json:"id"`
	PublicID            string    `json:"public_id"`
	TenantID            string    `json:"tenant_id"`
	SourceService       string    `json:"source_service"`
	ExternalID          string    `json:"external_id"`
	CorrelationID       uuid.UUID `json:"correlation_id"`
	SagaInstanceID      string    `json:"saga_instance_id"`
	StepIndex           int       `json:"step_index"`
	StepName            string    `json:"step_name"`
	StepType            StepType  `json:"step_type"`
	State               StepState `json:"state"`
	CommandPayload      string    `json:"command_payload"`
	ReplyPayload        string    `json:"reply_payload"`
	CompensationPayload string    `json:"compensation_payload"`
	DeadlineAt          time.Time `json:"deadline_at"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type AsyncCommandReply struct {
	ID            uuid.UUID      `json:"id"`
	PublicID      string         `json:"public_id"`
	TenantID      string         `json:"tenant_id"`
	SourceService string         `json:"source_service"`
	ExternalID    string         `json:"external_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	CommandID     string         `json:"command_id"`
	Outcome       CommandOutcome `json:"outcome"`
	IsLateReply   bool           `json:"is_late_reply"`
	ReplyPayload  string         `json:"reply_payload"`
	CreatedAt     time.Time      `json:"created_at"`
}


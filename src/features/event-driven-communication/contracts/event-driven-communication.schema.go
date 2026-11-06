package contracts

type StartSagaCommand struct {
	TenantID         string `json:"tenant_id"`
	SourceService    string `json:"source_service"`
	ExternalID       string `json:"external_id"`
	CorrelationID    string `json:"correlation_id"`
	SagaDefinitionID string `json:"saga_definition_id"`
	InitialPayload   string `json:"initial_payload"`
	TimeoutSeconds   int    `json:"timeout_seconds"`
}

type StartSagaResult struct {
	SagaInstanceID string    `json:"saga_instance_id"`
	CurrentStep    string    `json:"current_step"`
	State          SagaState `json:"state"`
}

type ProcessStepReplyCommand struct {
	TenantID       string         `json:"tenant_id"`
	SourceService  string         `json:"source_service"`
	SagaInstanceID string         `json:"saga_instance_id"`
	StepIndex      int            `json:"step_index"`
	CommandID      string         `json:"command_id"`
	Outcome        CommandOutcome `json:"outcome"`
	ReplyPayload   string         `json:"reply_payload"`
}

type ProcessStepReplyResult struct {
	SagaInstanceID string    `json:"saga_instance_id"`
	NextStepIndex  int       `json:"next_step_index"`
	SagaState      SagaState `json:"saga_state"`
	Compensation   bool      `json:"compensation_triggered"`
}

type TriggerCompensationCommand struct {
	TenantID       string `json:"tenant_id"`
	SourceService  string `json:"source_service"`
	SagaInstanceID string `json:"saga_instance_id"`
	Reason         string `json:"reason"`
}

type TriggerCompensationResult struct {
	SagaInstanceID       string    `json:"saga_instance_id"`
	CompensatedStepCount int       `json:"compensated_step_count"`
	SagaState            SagaState `json:"saga_state"`
}

type DispatchAsyncCommandRequest struct {
	TenantID       string `json:"tenant_id"`
	SourceService  string `json:"source_service"`
	ExternalID     string `json:"external_id"`
	CorrelationID  string `json:"correlation_id"`
	TargetTopic    string `json:"target_topic"`
	ReplyToTopic   string `json:"reply_to_topic"`
	CommandName    string `json:"command_name"`
	CommandPayload string `json:"command_payload"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type DispatchAsyncCommandResult struct {
	CommandID    string `json:"command_id"`
	Status       string `json:"status"`
	ReplyToTopic string `json:"reply_to_topic"`
}


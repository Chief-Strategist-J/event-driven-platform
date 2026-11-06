package contracts

import (
	"fmt"
	"strings"

	"event-driven-platform/src/shared/errors"
)

func NormalizeRegisterConnectorInput(raw RegisterConnectorInput) (RegisterConnectorInput, error) {
	tenantID := strings.TrimSpace(raw.TenantID)
	if tenantID == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("tenant_id cannot be blank")
	}

	sourceService := strings.TrimSpace(raw.SourceService)
	if sourceService == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("source_service cannot be blank")
	}

	externalID := strings.TrimSpace(raw.ExternalID)
	if externalID == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("external_id cannot be blank")
	}

	correlationID := strings.TrimSpace(raw.CorrelationID)
	if correlationID == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("correlation_id cannot be blank")
	}

	connName := strings.TrimSpace(raw.ConnectorName)
	if connName == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("connector_name cannot be blank")
	}

	slotName := strings.TrimSpace(raw.SlotName)
	if slotName == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("slot_name cannot be blank")
	}

	pubName := strings.TrimSpace(raw.PublicationName)
	if pubName == "" {
		return RegisterConnectorInput{}, errors.NewValidationError("publication_name cannot be blank")
	}

	mode := CDCMode(strings.ToUpper(strings.TrimSpace(string(raw.Mode))))
	if mode != ModeOutbox && mode != ModeRawTable {
		return RegisterConnectorInput{}, errors.NewValidationError(fmt.Sprintf("invalid cdc mode: %s", raw.Mode))
	}

	return RegisterConnectorInput{
		TenantID:        tenantID,
		SourceService:   sourceService,
		ExternalID:      externalID,
		CorrelationID:   correlationID,
		ConnectorName:   connName,
		SlotName:        slotName,
		PublicationName: pubName,
		Mode:            mode,
	}, nil
}

func NormalizeDispatchSignalInput(raw DispatchSignalInput) (DispatchSignalInput, error) {
	tenantID := strings.TrimSpace(raw.TenantID)
	if tenantID == "" {
		return DispatchSignalInput{}, errors.NewValidationError("tenant_id cannot be blank")
	}

	sourceService := strings.TrimSpace(raw.SourceService)
	if sourceService == "" {
		return DispatchSignalInput{}, errors.NewValidationError("source_service cannot be blank")
	}

	externalID := strings.TrimSpace(raw.ExternalID)
	if externalID == "" {
		return DispatchSignalInput{}, errors.NewValidationError("external_id cannot be blank")
	}

	correlationID := strings.TrimSpace(raw.CorrelationID)
	if correlationID == "" {
		return DispatchSignalInput{}, errors.NewValidationError("correlation_id cannot be blank")
	}

	connName := strings.TrimSpace(raw.ConnectorName)
	if connName == "" {
		return DispatchSignalInput{}, errors.NewValidationError("connector_name cannot be blank")
	}

	sigType := CDCSignalType(strings.ToUpper(strings.TrimSpace(string(raw.SignalType))))
	if sigType != SignalExecuteSnapshot && sigType != SignalPauseSnapshot && sigType != SignalResumeSnapshot && sigType != SignalCheckpoint {
		return DispatchSignalInput{}, errors.NewValidationError(fmt.Sprintf("invalid cdc signal type: %s", raw.SignalType))
	}

	payload := raw.Payload
	if payload == nil {
		payload = make(map[string]interface{})
	}

	return DispatchSignalInput{
		TenantID:      tenantID,
		SourceService: sourceService,
		ExternalID:    externalID,
		CorrelationID: correlationID,
		ConnectorName: connName,
		SignalType:    sigType,
		Payload:       payload,
	}, nil
}

func NormalizeHeartbeatTickInput(raw HeartbeatTickInput) (HeartbeatTickInput, error) {
	tenantID := strings.TrimSpace(raw.TenantID)
	if tenantID == "" {
		return HeartbeatTickInput{}, errors.NewValidationError("tenant_id cannot be blank")
	}
	sourceService := strings.TrimSpace(raw.SourceService)
	if sourceService == "" {
		return HeartbeatTickInput{}, errors.NewValidationError("source_service cannot be blank")
	}
	externalID := strings.TrimSpace(raw.ExternalID)
	if externalID == "" {
		return HeartbeatTickInput{}, errors.NewValidationError("external_id cannot be blank")
	}
	correlationID := strings.TrimSpace(raw.CorrelationID)
	if correlationID == "" {
		return HeartbeatTickInput{}, errors.NewValidationError("correlation_id cannot be blank")
	}
	connName := strings.TrimSpace(raw.ConnectorName)
	if connName == "" {
		return HeartbeatTickInput{}, errors.NewValidationError("connector_name cannot be blank")
	}
	lsn := strings.TrimSpace(raw.LSNPosition)
	if lsn == "" {
		return HeartbeatTickInput{}, errors.NewValidationError("lsn_position cannot be blank")
	}

	return HeartbeatTickInput{
		TenantID:      tenantID,
		SourceService: sourceService,
		ExternalID:    externalID,
		CorrelationID: correlationID,
		ConnectorName: connName,
		LSNPosition:   lsn,
	}, nil
}


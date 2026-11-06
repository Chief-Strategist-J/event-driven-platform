package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/machines"
	"event-driven-platform/src/features/cdc-integration/domain/rules"
	"event-driven-platform/src/features/cdc-integration/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/utils"
)

type CDCService struct {
	repo    ports.CDCRepositoryPort
	connect ports.KafkaConnectPort
	machine *machines.CDCConnectorMachine
}

func NewCDCService(
	repo ports.CDCRepositoryPort,
	connect ports.KafkaConnectPort,
	machine *machines.CDCConnectorMachine,
) *CDCService {
	return &CDCService{
		repo:    repo,
		connect: connect,
		machine: machine,
	}
}

func (s *CDCService) RegisterConnector(ctx context.Context, raw contracts.RegisterConnectorInput) (*contracts.ConnectorInventoryRecord, error) {
	norm, err := contracts.NormalizeRegisterConnectorInput(raw)
	if err != nil {
		return nil, err
	}

	if valErr := rules.ValidateConnectorConfiguration(norm); valErr != nil {
		return nil, valErr
	}

	existing, _ := s.repo.GetConnector(ctx, norm.ConnectorName)
	if existing != nil {
		return nil, errors.NewConflictError(fmt.Sprintf("cdc connector already registered: %s", norm.ConnectorName))
	}

	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("cdc_%s", strings.ReplaceAll(id, "-", "")[:20])
	now := time.Now().UTC()

	rec := contracts.ConnectorInventoryRecord{
		ID:               id,
		PublicID:         publicID,
		TenantID:         norm.TenantID,
		SourceService:    norm.SourceService,
		ExternalID:       norm.ExternalID,
		CorrelationID:    norm.CorrelationID,
		ConnectorName:    norm.ConnectorName,
		SlotName:         norm.SlotName,
		PublicationName:  norm.PublicationName,
		Mode:             norm.Mode,
		State:            contracts.StateRunning,
		WALRetainedBytes: 0,
		WALLagBytes:      0,
		LastHeartbeatAt:  now,
		Version:          1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if repoErr := s.repo.RegisterConnector(ctx, rec); repoErr != nil {
		return nil, repoErr
	}

	if connectErr := s.connect.CreateConnector(ctx, norm); connectErr != nil {
		_ = s.repo.UpdateConnectorState(ctx, norm.ConnectorName, contracts.StateFailed, 1)
		return nil, connectErr
	}

	return &rec, nil
}

func (s *CDCService) GetConnector(ctx context.Context, connectorName string) (*contracts.ConnectorInventoryRecord, error) {
	trimmed := strings.TrimSpace(connectorName)
	if trimmed == "" {
		return nil, errors.NewValidationError("connector_name cannot be blank")
	}
	return s.repo.GetConnector(ctx, trimmed)
}

func (s *CDCService) PauseConnector(ctx context.Context, connectorName string) error {
	trimmed := strings.TrimSpace(connectorName)
	rec, err := s.repo.GetConnector(ctx, trimmed)
	if err != nil {
		return err
	}

	nextState, transErr := s.machine.Transition(rec.State, contracts.StatePaused)
	if transErr != nil {
		return transErr
	}

	if updateErr := s.repo.UpdateConnectorState(ctx, trimmed, nextState, rec.Version); updateErr != nil {
		return updateErr
	}

	return s.connect.PauseConnector(ctx, trimmed)
}

func (s *CDCService) ResumeConnector(ctx context.Context, connectorName string) error {
	trimmed := strings.TrimSpace(connectorName)
	rec, err := s.repo.GetConnector(ctx, trimmed)
	if err != nil {
		return err
	}

	nextState, transErr := s.machine.Transition(rec.State, contracts.StateRunning)
	if transErr != nil {
		return transErr
	}

	if updateErr := s.repo.UpdateConnectorState(ctx, trimmed, nextState, rec.Version); updateErr != nil {
		return updateErr
	}

	return s.connect.ResumeConnector(ctx, trimmed)
}

func (s *CDCService) DispatchSignal(ctx context.Context, raw contracts.DispatchSignalInput) (*contracts.SignalRecord, error) {
	norm, err := contracts.NormalizeDispatchSignalInput(raw)
	if err != nil {
		return nil, err
	}

	rec, connErr := s.repo.GetConnector(ctx, norm.ConnectorName)
	if connErr != nil {
		return nil, connErr
	}

	if sigErr := rules.ValidateSignalForMode(rec.Mode, norm.SignalType); sigErr != nil {
		return nil, sigErr
	}

	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("sig_%s", strings.ReplaceAll(id, "-", "")[:20])

	sig := contracts.SignalRecord{
		ID:            id,
		PublicID:      publicID,
		TenantID:      norm.TenantID,
		SourceService: norm.SourceService,
		ExternalID:    norm.ExternalID,
		CorrelationID: norm.CorrelationID,
		ConnectorName: norm.ConnectorName,
		SignalType:    norm.SignalType,
		Payload:       norm.Payload,
		Processed:     false,
		CreatedAt:     time.Now().UTC(),
	}

	if recErr := s.repo.RecordSignal(ctx, sig); recErr != nil {
		return nil, recErr
	}

	return &sig, nil
}

func (s *CDCService) EmitHeartbeat(ctx context.Context, raw contracts.HeartbeatTickInput) error {
	norm, err := contracts.NormalizeHeartbeatTickInput(raw)
	if err != nil {
		return err
	}

	_, connErr := s.repo.GetConnector(ctx, norm.ConnectorName)
	if connErr != nil {
		return connErr
	}

	return s.repo.RecordHeartbeat(ctx, norm)
}

func (s *CDCService) EvaluateWALLagCircuitBreaker(ctx context.Context, connectorName string, retainedBytes int64, genRateBytesPerSec int64, diskFreeBytes int64) (rules.WALCircuitBreakerResult, error) {
	trimmed := strings.TrimSpace(connectorName)
	rec, err := s.repo.GetConnector(ctx, trimmed)
	if err != nil {
		return rules.WALCircuitBreakerResult{}, err
	}

	result := rules.EvaluateWALCircuitBreaker(retainedBytes, genRateBytesPerSec, diskFreeBytes)
	_ = s.repo.UpdateWALLag(ctx, trimmed, retainedBytes, retainedBytes)

	if result.Tripped && result.RecommendedAction == "EMERGENCY_PAUSE_CONNECTOR" && rec.State == contracts.StateRunning {
		_ = s.PauseConnector(ctx, trimmed)
	}

	return result, nil
}

func (s *CDCService) SendCanary(ctx context.Context, input contracts.CanaryCheckInput) error {
	trimmedConn := strings.TrimSpace(input.ConnectorName)
	if trimmedConn == "" {
		return errors.NewValidationError("connector_name cannot be blank")
	}

	_, connErr := s.repo.GetConnector(ctx, trimmedConn)
	if connErr != nil {
		return connErr
	}

	return s.repo.RecordCanarySent(ctx, input)
}

func (s *CDCService) ReconcileCanary(ctx context.Context, canaryEventID string, sentAt time.Time, maxAllowedMs int64) (contracts.CanaryCheckResult, error) {
	trimmedID := strings.TrimSpace(canaryEventID)
	if trimmedID == "" {
		return contracts.CanaryCheckResult{}, errors.NewValidationError("canary_event_id cannot be blank")
	}

	latency := time.Since(sentAt).Milliseconds()
	if latency < 0 {
		latency = 0
	}

	if err := s.repo.RecordCanaryReceived(ctx, trimmedID, latency); err != nil {
		return contracts.CanaryCheckResult{}, err
	}

	return rules.EvaluateCanarySLA(latency, maxAllowedMs), nil
}


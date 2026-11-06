package adapters

import (
	"context"
	"fmt"
	"sync"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/shared/errors"
)

type MockKafkaConnectAdapter struct {
	mu         sync.RWMutex
	connectors map[string]contracts.CDCConnectorState
}

func NewMockKafkaConnectAdapter() *MockKafkaConnectAdapter {
	return &MockKafkaConnectAdapter{
		connectors: make(map[string]contracts.CDCConnectorState),
	}
}

func (m *MockKafkaConnectAdapter) CreateConnector(_ context.Context, input contracts.RegisterConnectorInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[input.ConnectorName]; exists {
		return errors.NewConflictError(fmt.Sprintf("connector already exists in connect cluster: %s", input.ConnectorName))
	}
	m.connectors[input.ConnectorName] = contracts.StateRunning
	return nil
}

func (m *MockKafkaConnectAdapter) PauseConnector(_ context.Context, connectorName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[connectorName]; !exists {
		return errors.NewNotFoundError(fmt.Sprintf("connector not found: %s", connectorName))
	}
	m.connectors[connectorName] = contracts.StatePaused
	return nil
}

func (m *MockKafkaConnectAdapter) ResumeConnector(_ context.Context, connectorName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[connectorName]; !exists {
		return errors.NewNotFoundError(fmt.Sprintf("connector not found: %s", connectorName))
	}
	m.connectors[connectorName] = contracts.StateRunning
	return nil
}

func (m *MockKafkaConnectAdapter) RestartConnector(_ context.Context, connectorName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[connectorName]; !exists {
		return errors.NewNotFoundError(fmt.Sprintf("connector not found: %s", connectorName))
	}
	m.connectors[connectorName] = contracts.StateRunning
	return nil
}

func (m *MockKafkaConnectAdapter) DeleteConnector(_ context.Context, connectorName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connectors[connectorName]; !exists {
		return errors.NewNotFoundError(fmt.Sprintf("connector not found: %s", connectorName))
	}
	delete(m.connectors, connectorName)
	return nil
}


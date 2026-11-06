package contracts

import (
	"time"
)

type CDCMode string

const (
	ModeOutbox   CDCMode = "M_OUTBOX"
	ModeRawTable CDCMode = "M_RAWTABLE"
)

type CDCConnectorState string

const (
	StateUnregistered   CDCConnectorState = "UNREGISTERED"
	StateProvisioning   CDCConnectorState = "PROVISIONING"
	StateRunning        CDCConnectorState = "RUNNING"
	StatePaused         CDCConnectorState = "PAUSED"
	StateFailed         CDCConnectorState = "FAILED"
	StateDecommissioned CDCConnectorState = "DECOMMISSIONED"
)

type CDCSignalType string

const (
	SignalExecuteSnapshot CDCSignalType = "EXECUTE_SNAPSHOT"
	SignalPauseSnapshot   CDCSignalType = "PAUSE_SNAPSHOT"
	SignalResumeSnapshot  CDCSignalType = "RESUME_SNAPSHOT"
	SignalCheckpoint      CDCSignalType = "CHECKPOINT"
)

type ConnectorInventoryRecord struct {
	ID               string            `json:"id"`
	PublicID         string            `json:"public_id"`
	TenantID         string            `json:"tenant_id"`
	SourceService    string            `json:"source_service"`
	ExternalID       string            `json:"external_id"`
	CorrelationID    string            `json:"correlation_id"`
	ConnectorName    string            `json:"connector_name"`
	SlotName         string            `json:"slot_name"`
	PublicationName  string            `json:"publication_name"`
	Mode             CDCMode           `json:"mode"`
	State            CDCConnectorState `json:"state"`
	WALRetainedBytes int64             `json:"wal_retained_bytes"`
	WALLagBytes      int64             `json:"wal_lag_bytes"`
	LastHeartbeatAt  time.Time         `json:"last_heartbeat_at"`
	Version          int64             `json:"version"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

type RegisterConnectorInput struct {
	TenantID        string  `json:"tenant_id"`
	SourceService   string  `json:"source_service"`
	ExternalID      string  `json:"external_id"`
	CorrelationID   string  `json:"correlation_id"`
	ConnectorName   string  `json:"connector_name"`
	SlotName        string  `json:"slot_name"`
	PublicationName string  `json:"publication_name"`
	Mode            CDCMode `json:"mode"`
}

type DispatchSignalInput struct {
	TenantID      string                 `json:"tenant_id"`
	SourceService string                 `json:"source_service"`
	ExternalID    string                 `json:"external_id"`
	CorrelationID string                 `json:"correlation_id"`
	ConnectorName string                 `json:"connector_name"`
	SignalType    CDCSignalType          `json:"signal_type"`
	Payload       map[string]interface{} `json:"payload"`
}

type SignalRecord struct {
	ID            string                 `json:"id"`
	PublicID      string                 `json:"public_id"`
	TenantID      string                 `json:"tenant_id"`
	SourceService string                 `json:"source_service"`
	ExternalID    string                 `json:"external_id"`
	CorrelationID string                 `json:"correlation_id"`
	ConnectorName string                 `json:"connector_name"`
	SignalType    CDCSignalType          `json:"signal_type"`
	Payload       map[string]interface{} `json:"payload"`
	Processed     bool                   `json:"processed"`
	ProcessedAt   *time.Time             `json:"processed_at,omitempty"`
	CreatedAt     time.Time              `json:"created_at"`
}

type HeartbeatTickInput struct {
	TenantID      string `json:"tenant_id"`
	SourceService string `json:"source_service"`
	ExternalID    string `json:"external_id"`
	CorrelationID string `json:"correlation_id"`
	ConnectorName string `json:"connector_name"`
	LSNPosition   string `json:"lsn_position"`
}

type CanaryCheckInput struct {
	TenantID      string `json:"tenant_id"`
	SourceService string `json:"source_service"`
	ExternalID    string `json:"external_id"`
	CorrelationID string `json:"correlation_id"`
	ConnectorName string `json:"connector_name"`
	CanaryEventID string `json:"canary_event_id"`
}

type CanaryCheckResult struct {
	Reconciled   bool  `json:"reconciled"`
	LatencyMs    int64 `json:"latency_ms"`
	WithinSLO    bool  `json:"within_slo"`
	MaxAllowedMs int64 `json:"max_allowed_ms"`
}


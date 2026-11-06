package contracts

import (
	"time"

	"github.com/google/uuid"
)

type CheckOutcome string

const (
	OutcomeMatch   CheckOutcome = "MATCH"
	OutcomeMismatch CheckOutcome = "MISMATCH"
	OutcomeUnknown CheckOutcome = "UNKNOWN"
)

type DriftClass string

const (
	DriftGhost             DriftClass = "GHOST"
	DriftMissing           DriftClass = "MISSING"
	DriftAttributeMismatch DriftClass = "ATTRIBUTE_MISMATCH"
	DriftVersionLag        DriftClass = "VERSION_LAG"
	DriftManualWrite       DriftClass = "MANUAL_WRITE"
)

type CheckType string

const (
	CheckCanaryProbe        CheckType = "CANARY_PROBE"
	CheckWatermarkSweep     CheckType = "WATERMARK_SWEEP"
	CheckMerkleRangeAudit   CheckType = "MERKLE_RANGE_AUDIT"
	CheckInvariantAssertion CheckType = "INVARIANT_ASSERTION"
)

type IncidentStatus string

const (
	IncidentDetected           IncidentStatus = "DETECTED"
	IncidentInTolerance        IncidentStatus = "IN_TOLERANCE"
	IncidentConfirmed          IncidentStatus = "CONFIRMED"
	IncidentAutoHealing        IncidentStatus = "AUTO_HEALING"
	IncidentResolved           IncidentStatus = "RESOLVED"
	IncidentManualIntervention IncidentStatus = "MANUAL_INTERVENTION"
)

type SilenceStatus string

const (
	SilenceActive  SilenceStatus = "ACTIVE"
	SilenceExpired SilenceStatus = "EXPIRED"
	SilenceRevoked SilenceStatus = "REVOKED"
)

type WatermarkRecord struct {
	ID                  uuid.UUID `json:"id"`
	PublicID            string    `json:"public_id"`
	TenantID            string    `json:"tenant_id"`
	SourceService       string    `json:"source_service"`
	ExternalID          string    `json:"external_id"`
	CorrelationID       uuid.UUID `json:"correlation_id"`
	ViewName            string    `json:"view_name"`
	StreamName          string    `json:"stream_name"`
	PartitionID         int       `json:"partition_id"`
	HighWatermarkOffset int64     `json:"high_watermark_offset"`
	ConsumerOffset      int64     `json:"consumer_offset"`
	OffsetLag           int64     `json:"offset_lag"`
	FreshnessLagMs      int64     `json:"freshness_lag_ms"`
	WatermarkStatus     string    `json:"watermark_status"`
	ObservedAt          time.Time `json:"observed_at"`
	Version             int64     `json:"version"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type DriftIncidentRecord struct {
	ID                 uuid.UUID      `json:"id"`
	PublicID           string         `json:"public_id"`
	TenantID           string         `json:"tenant_id"`
	SourceService      string         `json:"source_service"`
	ExternalID         string         `json:"external_id"`
	CorrelationID      uuid.UUID      `json:"correlation_id"`
	ViewName           string         `json:"view_name"`
	AggregateType      string         `json:"aggregate_type"`
	AggregateID        string         `json:"aggregate_id"`
	DriftClass         DriftClass     `json:"drift_class"`
	SourceVersion      int64          `json:"source_version"`
	ViewVersion        int64          `json:"view_version"`
	DifferencePayload  string         `json:"difference_payload"`
	Status             IncidentStatus `json:"status"`
	HealAttempts       int            `json:"heal_attempts"`
	QuarantinedUntil   time.Time      `json:"quarantined_until"`
	DetectedAt         time.Time      `json:"detected_at"`
	ResolvedAt         time.Time      `json:"resolved_at"`
	Version            int64          `json:"version"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type CheckRunRecord struct {
	ID                  uuid.UUID `json:"id"`
	PublicID            string    `json:"public_id"`
	TenantID            string    `json:"tenant_id"`
	SourceService       string    `json:"source_service"`
	ExternalID          string    `json:"external_id"`
	CorrelationID       uuid.UUID `json:"correlation_id"`
	ViewName            string    `json:"view_name"`
	CheckType           CheckType `json:"check_type"`
	EvaluatedKeys       int64     `json:"evaluated_keys"`
	MatchCount          int64     `json:"match_count"`
	MismatchCount       int64     `json:"mismatch_count"`
	UnknownCount        int64     `json:"unknown_count"`
	CoverageRatio       float64   `json:"coverage_ratio"`
	ExecutionDurationMs int64     `json:"execution_duration_ms"`
	Status              string    `json:"status"`
	CreatedAt           time.Time `json:"created_at"`
}

type SilenceRecord struct {
	ID            uuid.UUID     `json:"id"`
	PublicID      string        `json:"public_id"`
	TenantID      string        `json:"tenant_id"`
	SourceService string        `json:"source_service"`
	ExternalID    string        `json:"external_id"`
	CorrelationID uuid.UUID     `json:"correlation_id"`
	ViewName      string        `json:"view_name"`
	OwnerID       string        `json:"owner_id"`
	Reason        string        `json:"reason"`
	ExpiresAt     time.Time     `json:"expires_at"`
	Status        SilenceStatus `json:"status"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

type ObserveWatermarkInput struct {
	TenantID            string `json:"tenant_id"`
	SourceService       string `json:"source_service"`
	ExternalID          string `json:"external_id"`
	CorrelationID       string `json:"correlation_id"`
	ViewName            string `json:"view_name"`
	StreamName          string `json:"stream_name"`
	PartitionID         int    `json:"partition_id"`
	HighWatermarkOffset int64  `json:"high_watermark_offset"`
	ConsumerOffset      int64  `json:"consumer_offset"`
	FreshnessLagMs      int64  `json:"freshness_lag_ms"`
}

type EvaluateRecordInput struct {
	TenantID            string `json:"tenant_id"`
	ViewName            string `json:"view_name"`
	AggregateType       string `json:"aggregate_type"`
	AggregateID         string `json:"aggregate_id"`
	SourceExists        bool   `json:"source_exists"`
	ViewExists          bool   `json:"view_exists"`
	SourceVersion       int64  `json:"source_version"`
	ViewVersion         int64  `json:"view_version"`
	SourcePayloadHash   string `json:"source_payload_hash"`
	ViewPayloadHash     string `json:"view_payload_hash"`
	IsManualWrite       bool   `json:"is_manual_write"`
	TargetShardsFailed  int    `json:"target_shards_failed"`
	ReplicaLagMs        int64  `json:"replica_lag_ms"`
	ReplicaLagThreshold int64  `json:"replica_lag_threshold"`
	QueryErrorOccurred  bool   `json:"query_error_occurred"`
}

type EvaluationResult struct {
	Outcome           CheckOutcome `json:"outcome"`
	DriftClass        DriftClass   `json:"drift_class,omitempty"`
	DifferencePayload string       `json:"difference_payload,omitempty"`
	RequiresHeal      bool         `json:"requires_heal"`
}

type RegisterSilenceInput struct {
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID string    `json:"correlation_id"`
	ViewName      string    `json:"view_name"`
	OwnerID       string    `json:"owner_id"`
	Reason        string    `json:"reason"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type CanaryProbeInput struct {
	TenantID      string `json:"tenant_id"`
	SourceService string `json:"source_service"`
	CorrelationID string `json:"correlation_id"`
	StreamName    string `json:"stream_name"`
	PartitionID   int    `json:"partition_id"`
	Key           string `json:"key"`
	Payload       string `json:"payload"`
}

type CanaryProbeOutput struct {
	ProbeID        string       `json:"probe_id"`
	PartitionID    int          `json:"partition_id"`
	TransitDurationMs int64     `json:"transit_duration_ms"`
	Outcome        CheckOutcome `json:"outcome"`
}

type RunCheckSweepInput struct {
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	CorrelationID string    `json:"correlation_id"`
	ViewName      string    `json:"view_name"`
	CheckType     CheckType `json:"check_type"`
	EvaluatedKeys int64     `json:"evaluated_keys"`
	MatchCount    int64     `json:"match_count"`
	MismatchCount int64     `json:"mismatch_count"`
	UnknownCount  int64     `json:"unknown_count"`
	DurationMs    int64     `json:"duration_ms"`
}


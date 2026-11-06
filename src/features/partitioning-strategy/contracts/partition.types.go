package contracts

import (
	"time"

	"github.com/google/uuid"
)

type TopicClass string

const (
	TopicClassOrdered   TopicClass = "K_ORDERED"
	TopicClassCompacted TopicClass = "K_COMPACTED"
	TopicClassCopart    TopicClass = "K_COPART"
	TopicClassUnordered TopicClass = "K_UNORDERED"
)

type MigrationPhase string

const (
	MigrationPhasePlanned            MigrationPhase = "PLANNED"
	MigrationPhaseProvisioningTarget MigrationPhase = "PROVISIONING_TARGET"
	MigrationPhaseDualPublishing     MigrationPhase = "DUAL_PUBLISHING"
	MigrationPhaseDrainingSource     MigrationPhase = "DRAINING_SOURCE"
	MigrationPhaseCutover            MigrationPhase = "CUTOVER"
	MigrationPhaseDecommissioned     MigrationPhase = "DECOMMISSIONED"
)

type TopicTopology struct {
	ID                    uuid.UUID  `json:"id"`
	PublicID              string     `json:"public_id"`
	TenantID              string     `json:"tenant_id"`
	SourceService         string     `json:"source_service"`
	ExternalID            string     `json:"external_id"`
	CorrelationID         uuid.UUID  `json:"correlation_id"`
	TopicName             string     `json:"topic_name"`
	TopicClass            TopicClass `json:"topic_class"`
	PartitionCount        int        `json:"partition_count"`
	ReplicationFactor     int        `json:"replication_factor"`
	MinInSyncReplicas     int        `json:"min_insync_replicas"`
	AllowInPlaceExpansion bool       `json:"allow_in_place_expansion"`
	Version               int64      `json:"version"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

type RegisterTopicTopologyInput struct {
	TenantID              string     `json:"tenant_id"`
	SourceService         string     `json:"source_service"`
	ExternalID            string     `json:"external_id"`
	CorrelationID         string     `json:"correlation_id"`
	TopicName             string     `json:"topic_name"`
	TopicClass            TopicClass `json:"topic_class"`
	PartitionCount        int        `json:"partition_count"`
	ReplicationFactor     int        `json:"replication_factor"`
	MinInSyncReplicas     int        `json:"min_insync_replicas"`
	AllowInPlaceExpansion bool       `json:"allow_in_place_expansion"`
}

type CalculatePartitionInput struct {
	TopicName      string     `json:"topic_name"`
	TopicClass     TopicClass `json:"topic_class"`
	PartitionCount int        `json:"partition_count"`
	TenantID       string     `json:"tenant_id"`
	AggregateType  string     `json:"aggregate_type"`
	AggregateID    string     `json:"aggregate_id"`
	RawKeyBytes    []byte     `json:"raw_key_bytes"`
}

type CalculatePartitionOutput struct {
	TopicName          string `json:"topic_name"`
	Partition          int    `json:"partition"`
	PartitionCount     int    `json:"partition_count"`
	NormalizedKey      string `json:"normalized_key"`
	Murmur2Hash        int32  `json:"murmur2_hash"`
	MaskedPositiveHash int32  `json:"masked_positive_hash"`
}

type PartitionThroughput struct {
	PartitionID   int     `json:"partition_id"`
	ThroughputEPS float64 `json:"throughput_eps"`
	LagRecords    int64   `json:"lag_records"`
}

type EvaluateSkewInput struct {
	TenantID      string                `json:"tenant_id"`
	SourceService string                `json:"source_service"`
	ExternalID    string                `json:"external_id"`
	CorrelationID string                `json:"correlation_id"`
	TopicName     string                `json:"topic_name"`
	Partitions    []PartitionThroughput `json:"partitions"`
}

type PartitionSkewMetrics struct {
	ID                  uuid.UUID `json:"id"`
	PublicID            string    `json:"public_id"`
	TenantID            string    `json:"tenant_id"`
	SourceService       string    `json:"source_service"`
	ExternalID          string    `json:"external_id"`
	CorrelationID       uuid.UUID `json:"correlation_id"`
	TopicName           string    `json:"topic_name"`
	MaxThroughputEPS    float64   `json:"max_throughput_eps"`
	MedianThroughputEPS float64   `json:"median_throughput_eps"`
	SkewRatio           float64   `json:"skew_ratio"`
	HotPartitionID      int       `json:"hot_partition_id"`
	IsAlertBreached     bool      `json:"is_alert_breached"`
	MeasuredAt          time.Time `json:"measured_at"`
}

type SkewEvaluationOutput struct {
	TopicName           string  `json:"topic_name"`
	MaxThroughputEPS    float64 `json:"max_throughput_eps"`
	MedianThroughputEPS float64 `json:"median_throughput_eps"`
	SkewRatio           float64 `json:"skew_ratio"`
	HotPartitionID      int     `json:"hot_partition_id"`
	IsAlertBreached     bool    `json:"is_alert_breached"`
	RemediationAction   string  `json:"remediation_action"`
}

type InitiateMigrationInput struct {
	TenantID         string `json:"tenant_id"`
	SourceService    string `json:"source_service"`
	ExternalID       string `json:"external_id"`
	CorrelationID    string `json:"correlation_id"`
	MigrationID      string `json:"migration_id"`
	SourceTopic      string `json:"source_topic"`
	TargetTopic      string `json:"target_topic"`
	SourcePartitions int    `json:"source_partitions"`
	TargetPartitions int    `json:"target_partitions"`
}

type AdvanceMigrationInput struct {
	TenantID      string         `json:"tenant_id"`
	MigrationID   string         `json:"migration_id"`
	ExpectedPhase MigrationPhase `json:"expected_phase"`
	TargetPhase   MigrationPhase `json:"target_phase"`
	CorrelationID string         `json:"correlation_id"`
}

type MigrationRunbookRecord struct {
	ID               uuid.UUID      `json:"id"`
	PublicID         string         `json:"public_id"`
	TenantID         string         `json:"tenant_id"`
	SourceService    string         `json:"source_service"`
	ExternalID       string         `json:"external_id"`
	CorrelationID    uuid.UUID      `json:"correlation_id"`
	MigrationID      string         `json:"migration_id"`
	SourceTopic      string         `json:"source_topic"`
	TargetTopic      string         `json:"target_topic"`
	SourcePartitions int            `json:"source_partitions"`
	TargetPartitions int            `json:"target_partitions"`
	Phase            MigrationPhase `json:"phase"`
	Version          int64          `json:"version"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}


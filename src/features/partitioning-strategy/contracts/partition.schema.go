package contracts

import (
	"strings"

	"event-driven-platform/src/shared/errors"
	"github.com/google/uuid"
)

func NormalizeRegisterTopicTopologyInput(in RegisterTopicTopologyInput) (RegisterTopicTopologyInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.SourceService = strings.TrimSpace(out.SourceService)
	out.ExternalID = strings.TrimSpace(out.ExternalID)
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)
	out.TopicName = strings.TrimSpace(out.TopicName)
	out.TopicClass = TopicClass(strings.TrimSpace(string(out.TopicClass)))

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.SourceService == "" {
		return out, errors.NewValidationError("source_service must not be empty")
	}
	if out.ExternalID == "" {
		return out, errors.NewValidationError("external_id must not be empty")
	}
	if out.TopicName == "" {
		return out, errors.NewValidationError("topic_name must not be empty")
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	switch out.TopicClass {
	case TopicClassOrdered, TopicClassCompacted, TopicClassCopart, TopicClassUnordered:
	default:
		return out, errors.NewValidationError("invalid topic_class")
	}

	if out.PartitionCount <= 0 {
		return out, errors.NewValidationError("partition_count must be strictly greater than 0")
	}
	if out.ReplicationFactor <= 0 {
		out.ReplicationFactor = 3
	}
	if out.MinInSyncReplicas <= 0 {
		out.MinInSyncReplicas = 2
	}

	if out.TopicClass != TopicClassUnordered && out.AllowInPlaceExpansion {
		return out, errors.NewValidationError("in-place partition expansion is forbidden for ordered topic classes")
	}

	return out, nil
}

func NormalizeCalculatePartitionInput(in CalculatePartitionInput) (CalculatePartitionInput, error) {
	out := in
	out.TopicName = strings.TrimSpace(out.TopicName)
	out.TopicClass = TopicClass(strings.TrimSpace(string(out.TopicClass)))
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.AggregateType = strings.TrimSpace(out.AggregateType)
	out.AggregateID = strings.TrimSpace(out.AggregateID)

	if out.TopicName == "" {
		return out, errors.NewValidationError("topic_name must not be empty")
	}
	if out.PartitionCount <= 0 {
		return out, errors.NewValidationError("partition_count must be strictly greater than 0")
	}

	switch out.TopicClass {
	case TopicClassOrdered, TopicClassCompacted, TopicClassCopart, TopicClassUnordered:
	default:
		out.TopicClass = TopicClassOrdered
	}

	if out.TopicClass != TopicClassUnordered {
		if len(out.RawKeyBytes) == 0 && (out.TenantID == "" || out.AggregateID == "") {
			return out, errors.NewValidationError("keyed topics require non-empty tenant_id and aggregate_id")
		}
	}

	return out, nil
}

func NormalizeEvaluateSkewInput(in EvaluateSkewInput) (EvaluateSkewInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.SourceService = strings.TrimSpace(out.SourceService)
	out.ExternalID = strings.TrimSpace(out.ExternalID)
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)
	out.TopicName = strings.TrimSpace(out.TopicName)

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.SourceService == "" {
		return out, errors.NewValidationError("source_service must not be empty")
	}
	if out.ExternalID == "" {
		return out, errors.NewValidationError("external_id must not be empty")
	}
	if out.TopicName == "" {
		return out, errors.NewValidationError("topic_name must not be empty")
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	if len(out.Partitions) == 0 {
		return out, errors.NewValidationError("partitions telemetry list must not be empty")
	}

	return out, nil
}

func NormalizeInitiateMigrationInput(in InitiateMigrationInput) (InitiateMigrationInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.SourceService = strings.TrimSpace(out.SourceService)
	out.ExternalID = strings.TrimSpace(out.ExternalID)
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)
	out.MigrationID = strings.TrimSpace(out.MigrationID)
	out.SourceTopic = strings.TrimSpace(out.SourceTopic)
	out.TargetTopic = strings.TrimSpace(out.TargetTopic)

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.SourceService == "" {
		return out, errors.NewValidationError("source_service must not be empty")
	}
	if out.ExternalID == "" {
		return out, errors.NewValidationError("external_id must not be empty")
	}
	if out.MigrationID == "" {
		return out, errors.NewValidationError("migration_id must not be empty")
	}
	if out.SourceTopic == "" {
		return out, errors.NewValidationError("source_topic must not be empty")
	}
	if out.TargetTopic == "" {
		return out, errors.NewValidationError("target_topic must not be empty")
	}
	if out.SourceTopic == out.TargetTopic {
		return out, errors.NewValidationError("target_topic must be distinct from source_topic")
	}
	if out.SourcePartitions <= 0 {
		return out, errors.NewValidationError("source_partitions must be strictly positive")
	}
	if out.TargetPartitions <= out.SourcePartitions {
		return out, errors.NewValidationError("target_partitions must be strictly greater than source_partitions")
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	return out, nil
}

func NormalizeAdvanceMigrationInput(in AdvanceMigrationInput) (AdvanceMigrationInput, error) {
	out := in
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.MigrationID = strings.TrimSpace(out.MigrationID)
	out.ExpectedPhase = MigrationPhase(strings.TrimSpace(string(out.ExpectedPhase)))
	out.TargetPhase = MigrationPhase(strings.TrimSpace(string(out.TargetPhase)))
	out.CorrelationID = strings.TrimSpace(out.CorrelationID)

	if out.TenantID == "" {
		return out, errors.NewValidationError("tenant_id must not be empty")
	}
	if out.MigrationID == "" {
		return out, errors.NewValidationError("migration_id must not be empty")
	}
	if out.CorrelationID != "" {
		if _, err := uuid.Parse(out.CorrelationID); err != nil {
			return out, errors.NewValidationError("correlation_id must be a valid UUID")
		}
	} else {
		out.CorrelationID = uuid.New().String()
	}

	return out, nil
}


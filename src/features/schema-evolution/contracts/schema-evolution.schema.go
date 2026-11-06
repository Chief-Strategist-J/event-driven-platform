package contracts

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantID      = errors.New("tenant_id must not be empty or whitespace")
	ErrInvalidSourceService = errors.New("source_service must not be empty or whitespace")
	ErrInvalidSubjectName   = errors.New("subject_name must not be empty or whitespace")
	ErrInvalidCorrelationID = errors.New("correlation_id must be a valid non-nil UUID")
	ErrInvalidSchemaVersion = errors.New("schema_version must be positive")
	ErrInvalidDefinition    = errors.New("schema_definition must not be empty")
	ErrInvalidTopicName     = errors.New("topic name must not be empty or whitespace")
	ErrInvalidSunsetDeadline = errors.New("sunset_deadline must be in the future")
	ErrInvalidConsumerGroup = errors.New("consumer_group must not be empty or whitespace")
)

func NormalizeRegisterSchemaInput(in RegisterSchemaInput) (RegisterSchemaInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.SubjectName = strings.TrimSpace(in.SubjectName)
	if in.SubjectName == "" {
		return in, ErrInvalidSubjectName
	}
	if in.SchemaVersion <= 0 {
		return in, ErrInvalidSchemaVersion
	}
	in.SchemaDefinition = strings.TrimSpace(in.SchemaDefinition)
	if in.SchemaDefinition == "" {
		return in, ErrInvalidDefinition
	}
	if in.CompatibilityMode == "" {
		in.CompatibilityMode = CompatibilityFullTransitive
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	if in.ExternalID == "" {
		in.ExternalID = uuid.New().String()
	}
	return in, nil
}

func NormalizeCheckConsumerReadinessInput(in CheckConsumerReadinessInput) (CheckConsumerReadinessInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SubjectName = strings.TrimSpace(in.SubjectName)
	if in.SubjectName == "" {
		return in, ErrInvalidSubjectName
	}
	if in.TargetVersion <= 0 {
		return in, ErrInvalidSchemaVersion
	}
	return in, nil
}

func NormalizeInitiateDualPublishInput(in InitiateDualPublishInput) (InitiateDualPublishInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.SubjectName = strings.TrimSpace(in.SubjectName)
	if in.SubjectName == "" {
		return in, ErrInvalidSubjectName
	}
	in.LegacyTopic = strings.TrimSpace(in.LegacyTopic)
	if in.LegacyTopic == "" {
		return in, ErrInvalidTopicName
	}
	in.ModernTopic = strings.TrimSpace(in.ModernTopic)
	if in.ModernTopic == "" {
		return in, ErrInvalidTopicName
	}
	if !in.SunsetDeadline.After(time.Now().UTC()) {
		return in, ErrInvalidSunsetDeadline
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	return in, nil
}

func NormalizeEvaluateDecommissionInput(in EvaluateDecommissionInput) (EvaluateDecommissionInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SubjectName = strings.TrimSpace(in.SubjectName)
	if in.SubjectName == "" {
		return in, ErrInvalidSubjectName
	}
	return in, nil
}

func NormalizeRecordIncidentInput(in RecordIncidentInput) (RecordIncidentInput, error) {
	in.TenantID = strings.TrimSpace(in.TenantID)
	if in.TenantID == "" {
		return in, ErrInvalidTenantID
	}
	in.SourceService = strings.TrimSpace(in.SourceService)
	if in.SourceService == "" {
		return in, ErrInvalidSourceService
	}
	in.SubjectName = strings.TrimSpace(in.SubjectName)
	if in.SubjectName == "" {
		return in, ErrInvalidSubjectName
	}
	in.ConsumerGroup = strings.TrimSpace(in.ConsumerGroup)
	if in.ConsumerGroup == "" {
		return in, ErrInvalidConsumerGroup
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.New().String()
	} else if _, err := uuid.Parse(in.CorrelationID); err != nil {
		return in, ErrInvalidCorrelationID
	}
	return in, nil
}


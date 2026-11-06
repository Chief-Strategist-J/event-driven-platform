package contracts

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantID     = errors.New("tenant_id must not be empty or contain leading/trailing whitespace")
	ErrInvalidSourceService = errors.New("source_service must not be empty or whitespace")
	ErrInvalidExternalID   = errors.New("external_id must not be empty or whitespace")
	ErrInvalidPolicyName   = errors.New("policy_name must not be empty or whitespace")
	ErrInvalidPrincipalID  = errors.New("principal_id must not be empty or whitespace")
	ErrInvalidPrincipalType = errors.New("principal_type must be SERVICE, USER, ROLE, or GROUP")
	ErrInvalidResourceType = errors.New("resource_type must be TOPIC, CONSUMER_GROUP, FIELD, or SCHEMA")
	ErrInvalidPattern      = errors.New("resource_pattern must not be empty or whitespace")
	ErrInvalidOperation    = errors.New("operation must be PUBLISH, SUBSCRIBE, VIEW_PII, MANAGE, or *")
	ErrInvalidEffect       = errors.New("effect must be ALLOW or DENY")
	ErrInvalidPolicyID     = errors.New("public_id must match format ^pol_[0-9a-z]{16,26}$")
	ErrInvalidAuditID      = errors.New("public_id must match format ^aal_[0-9a-z]{16,26}$")
	ErrAccessDenied        = errors.New("access denied: default deny or explicit deny policy enforced")
	ErrSingleWriterViolation = errors.New("AZ-04 violation: exactly one producer principal with Write allowed per topic")
	ErrIdentityHeaderForged = errors.New("AZ-01 violation: identity headers stripped from untrusted source")
	ErrAudienceMismatch    = errors.New("AZ-02 violation: token not audience-restricted to target service")

	PolicyPublicIDRegex = regexp.MustCompile(`^pol_[0-9a-z]{16,26}$`)
	AuditPublicIDRegex  = regexp.MustCompile(`^aal_[0-9a-z]{16,26}$`)
)

type PrincipalType string

const (
	PrincipalService PrincipalType = "SERVICE"
	PrincipalUser    PrincipalType = "USER"
	PrincipalRole    PrincipalType = "ROLE"
	PrincipalGroup   PrincipalType = "GROUP"
)

type ResourceType string

const (
	ResourceTopic         ResourceType = "TOPIC"
	ResourceConsumerGroup ResourceType = "CONSUMER_GROUP"
	ResourceField         ResourceType = "FIELD"
	ResourceSchema        ResourceType = "SCHEMA"
)

type Operation string

const (
	OpPublish   Operation = "PUBLISH"
	OpSubscribe Operation = "SUBSCRIBE"
	OpViewPII   Operation = "VIEW_PII"
	OpManage    Operation = "MANAGE"
	OpAll       Operation = "*"
)

type PolicyEffect string

const (
	EffectAllow PolicyEffect = "ALLOW"
	EffectDeny  PolicyEffect = "DENY"
)

type PolicyState string

const (
	PolicyStateActive   PolicyState = "ACTIVE"
	PolicyStateDisabled PolicyState = "DISABLED"
	PolicyStateRevoked  PolicyState = "REVOKED"
)

type Decision string

const (
	DecisionAllowed Decision = "ALLOWED"
	DecisionDenied  Decision = "DENIED"
)

type AuthPolicyRecord struct {
	ID              uuid.UUID     `json:"id"`
	PublicID        string        `json:"public_id"`
	TenantID        string        `json:"tenant_id"`
	SourceService   string        `json:"source_service"`
	ExternalID      string        `json:"external_id"`
	CorrelationID   uuid.UUID     `json:"correlation_id"`
	PolicyName      string        `json:"policy_name"`
	PrincipalID     string        `json:"principal_id"`
	PrincipalType   PrincipalType `json:"principal_type"`
	ResourceType    ResourceType  `json:"resource_type"`
	ResourcePattern string        `json:"resource_pattern"`
	Operation       Operation     `json:"operation"`
	Effect          PolicyEffect  `json:"effect"`
	State           PolicyState   `json:"state"`
	CreatedAt       time.Time     `json:"created_at"`
	UpdatedAt       time.Time     `json:"updated_at"`
}

type AuthAuditRecord struct {
	ID              uuid.UUID     `json:"id"`
	PublicID        string        `json:"public_id"`
	TenantID        string        `json:"tenant_id"`
	SourceService   string        `json:"source_service"`
	ExternalID      string        `json:"external_id"`
	CorrelationID   uuid.UUID     `json:"correlation_id"`
	PrincipalID     string        `json:"principal_id"`
	PrincipalType   PrincipalType `json:"principal_type"`
	ResourceType    ResourceType  `json:"resource_type"`
	ResourceName    string        `json:"resource_name"`
	Operation       Operation     `json:"operation"`
	Decision        Decision      `json:"decision"`
	MatchedPolicyID string        `json:"matched_policy_id"`
	Reason          string        `json:"reason"`
	EvaluatedAt     time.Time     `json:"evaluated_at"`
}

type CreatePolicyInput struct {
	TenantID        string        `json:"tenant_id"`
	SourceService   string        `json:"source_service"`
	ExternalID      string        `json:"external_id"`
	CorrelationID   uuid.UUID     `json:"correlation_id"`
	PolicyName      string        `json:"policy_name"`
	PrincipalID     string        `json:"principal_id"`
	PrincipalType   PrincipalType `json:"principal_type"`
	ResourceType    ResourceType  `json:"resource_type"`
	ResourcePattern string        `json:"resource_pattern"`
	Operation       Operation     `json:"operation"`
	Effect          PolicyEffect  `json:"effect"`
}

type EvaluateAccessInput struct {
	TenantID      string        `json:"tenant_id"`
	SourceService string        `json:"source_service"`
	ExternalID    string        `json:"external_id"`
	CorrelationID uuid.UUID     `json:"correlation_id"`
	PrincipalID   string        `json:"principal_id"`
	PrincipalType PrincipalType `json:"principal_type"`
	ResourceType  ResourceType  `json:"resource_type"`
	ResourceName  string        `json:"resource_name"`
	Operation     Operation     `json:"operation"`
}

type EvaluateAccessOutput struct {
	Decision        Decision `json:"decision"`
	MatchedPolicyID string   `json:"matched_policy_id"`
	Reason          string   `json:"reason"`
	EvaluatedAt     string   `json:"evaluated_at"`
}

type EventAuthorizationRepository interface {
	SavePolicy(ctx context.Context, policy *AuthPolicyRecord) error
	GetPolicyByPublicID(ctx context.Context, publicID string) (*AuthPolicyRecord, error)
	FindMatchingPolicies(ctx context.Context, tenantID, principalID string, resType ResourceType, op Operation) ([]*AuthPolicyRecord, error)
	GetTopicWriterCount(ctx context.Context, tenantID, topicPattern string) (int, error)
	UpdatePolicyState(ctx context.Context, publicID string, state PolicyState) error

	SaveAuditLog(ctx context.Context, audit *AuthAuditRecord) error
	GetAuditLogByPublicID(ctx context.Context, publicID string) (*AuthAuditRecord, error)
}

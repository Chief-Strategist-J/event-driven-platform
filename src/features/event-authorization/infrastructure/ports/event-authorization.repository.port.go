package ports

import (
	"context"
	"event-driven-platform/src/features/event-authorization/contracts"
)

type EventAuthorizationRepositoryPort interface {
	SavePolicy(ctx context.Context, policy *contracts.AuthPolicyRecord) error
	GetPolicyByPublicID(ctx context.Context, publicID string) (*contracts.AuthPolicyRecord, error)
	FindMatchingPolicies(ctx context.Context, tenantID, principalID string, resType contracts.ResourceType, op contracts.Operation) ([]*contracts.AuthPolicyRecord, error)
	GetTopicWriterCount(ctx context.Context, tenantID, topicPattern string) (int, error)
	UpdatePolicyState(ctx context.Context, publicID string, state contracts.PolicyState) error

	SaveAuditLog(ctx context.Context, audit *contracts.AuthAuditRecord) error
	GetAuditLogByPublicID(ctx context.Context, publicID string) (*contracts.AuthAuditRecord, error)
}

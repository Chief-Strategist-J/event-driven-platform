package contracts

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func GeneratePublicID(prefix string) (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate secure random id: %w", err)
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b)), nil
}

func ValidatePolicyPublicID(publicID string) bool {
	return PolicyPublicIDRegex.MatchString(publicID)
}

func ValidateAuditPublicID(publicID string) bool {
	return AuditPublicIDRegex.MatchString(publicID)
}

func NormalizeCreatePolicyInput(in *CreatePolicyInput) (*AuthPolicyRecord, error) {
	if in == nil {
		return nil, fmt.Errorf("input must not be nil")
	}
	tID := strings.TrimSpace(in.TenantID)
	if tID == "" {
		return nil, ErrInvalidTenantID
	}
	src := strings.TrimSpace(in.SourceService)
	if src == "" {
		return nil, ErrInvalidSourceService
	}
	ext := strings.TrimSpace(in.ExternalID)
	if ext == "" {
		return nil, ErrInvalidExternalID
	}
	pName := strings.TrimSpace(in.PolicyName)
	if pName == "" {
		return nil, ErrInvalidPolicyName
	}
	princID := strings.TrimSpace(in.PrincipalID)
	if princID == "" {
		return nil, ErrInvalidPrincipalID
	}
	switch in.PrincipalType {
	case PrincipalService, PrincipalUser, PrincipalRole, PrincipalGroup:
	default:
		return nil, ErrInvalidPrincipalType
	}
	switch in.ResourceType {
	case ResourceTopic, ResourceConsumerGroup, ResourceField, ResourceSchema:
	default:
		return nil, ErrInvalidResourceType
	}
	pattern := strings.TrimSpace(in.ResourcePattern)
	if pattern == "" {
		return nil, ErrInvalidPattern
	}
	switch in.Operation {
	case OpPublish, OpSubscribe, OpViewPII, OpManage, OpAll:
	default:
		return nil, ErrInvalidOperation
	}
	switch in.Effect {
	case EffectAllow, EffectDeny:
	default:
		return nil, ErrInvalidEffect
	}

	pubID, err := GeneratePublicID("pol")
	if err != nil {
		return nil, err
	}

	corrID := in.CorrelationID
	if corrID == uuid.Nil {
		corrID = uuid.New()
	}

	now := time.Now().UTC()
	return &AuthPolicyRecord{
		ID:              uuid.New(),
		PublicID:        pubID,
		TenantID:        tID,
		SourceService:   src,
		ExternalID:      ext,
		CorrelationID:   corrID,
		PolicyName:      pName,
		PrincipalID:     princID,
		PrincipalType:   in.PrincipalType,
		ResourceType:    in.ResourceType,
		ResourcePattern: pattern,
		Operation:       in.Operation,
		Effect:          in.Effect,
		State:           PolicyStateActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func NormalizeEvaluateAccessInput(in *EvaluateAccessInput) (*EvaluateAccessInput, error) {
	if in == nil {
		return nil, fmt.Errorf("input must not be nil")
	}
	tID := strings.TrimSpace(in.TenantID)
	if tID == "" {
		return nil, ErrInvalidTenantID
	}
	src := strings.TrimSpace(in.SourceService)
	if src == "" {
		return nil, ErrInvalidSourceService
	}
	ext := strings.TrimSpace(in.ExternalID)
	if ext == "" {
		return nil, ErrInvalidExternalID
	}
	princID := strings.TrimSpace(in.PrincipalID)
	if princID == "" {
		return nil, ErrInvalidPrincipalID
	}
	switch in.PrincipalType {
	case PrincipalService, PrincipalUser, PrincipalRole, PrincipalGroup:
	default:
		return nil, ErrInvalidPrincipalType
	}
	switch in.ResourceType {
	case ResourceTopic, ResourceConsumerGroup, ResourceField, ResourceSchema:
	default:
		return nil, ErrInvalidResourceType
	}
	resName := strings.TrimSpace(in.ResourceName)
	if resName == "" {
		return nil, ErrInvalidPattern
	}
	switch in.Operation {
	case OpPublish, OpSubscribe, OpViewPII, OpManage, OpAll:
	default:
		return nil, ErrInvalidOperation
	}

	corrID := in.CorrelationID
	if corrID == uuid.Nil {
		corrID = uuid.New()
	}

	return &EvaluateAccessInput{
		TenantID:      tID,
		SourceService: src,
		ExternalID:    ext,
		CorrelationID: corrID,
		PrincipalID:   princID,
		PrincipalType: in.PrincipalType,
		ResourceType:  in.ResourceType,
		ResourceName:  resName,
		Operation:     in.Operation,
	}, nil
}

package contracts

import (
	"errors"
)

var (
	ErrInvalidTenantID      = errors.New("tenantId is required")
	ErrInvalidSourceService = errors.New("sourceService is required")
	ErrInvalidExternalID    = errors.New("externalId is required")
	ErrInvalidGroupID       = errors.New("groupId is required")
	ErrInvalidMemberID      = errors.New("memberId is required")
	ErrInvalidGeneration    = errors.New("generationId must be >= 0")
	ErrBannedAssignor       = errors.New("eager assignors (RangeAssignor, RoundRobinAssignor) are banned; use CooperativeStickyAssignor")
)

const (
	CanonicalCooperativeStickyAssignor = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
)

func ValidateRegisterMember(input RegisterMemberInput) error {
	if input.TenantID == "" {
		return ErrInvalidTenantID
	}
	if input.SourceService == "" {
		return ErrInvalidSourceService
	}
	if input.ExternalID == "" {
		return ErrInvalidExternalID
	}
	if input.GroupID == "" {
		return ErrInvalidGroupID
	}
	if input.MemberID == "" {
		return ErrInvalidMemberID
	}
	if input.AssignmentStrategy != "" && input.AssignmentStrategy != CanonicalCooperativeStickyAssignor {
		return ErrBannedAssignor
	}
	return nil
}

func ValidateHeartbeat(input HeartbeatInput) error {
	if input.TenantID == "" {
		return ErrInvalidTenantID
	}
	if input.GroupID == "" {
		return ErrInvalidGroupID
	}
	if input.MemberID == "" {
		return ErrInvalidMemberID
	}
	return nil
}

func ValidateRebalanceSync(input RebalanceSyncInput) error {
	if input.TenantID == "" {
		return ErrInvalidTenantID
	}
	if input.SourceService == "" {
		return ErrInvalidSourceService
	}
	if input.ExternalID == "" {
		return ErrInvalidExternalID
	}
	if input.GroupID == "" {
		return ErrInvalidGroupID
	}
	if input.MemberID == "" {
		return ErrInvalidMemberID
	}
	if input.GenerationID < 0 {
		return ErrInvalidGeneration
	}
	return nil
}


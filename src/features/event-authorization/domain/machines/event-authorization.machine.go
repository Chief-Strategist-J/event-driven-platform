package machines

import (
	"errors"
	"event-driven-platform/src/features/event-authorization/contracts"
)

var (
	ErrIllegalPolicyTransition = errors.New("illegal policy state transition")
)

var AllowedPolicyTransitions = map[contracts.PolicyState]map[contracts.PolicyState]bool{
	contracts.PolicyStateActive: {
		contracts.PolicyStateDisabled: true,
		contracts.PolicyStateRevoked:  true,
	},
	contracts.PolicyStateDisabled: {
		contracts.PolicyStateActive:  true,
		contracts.PolicyStateRevoked: true,
	},
	contracts.PolicyStateRevoked: {},
}

func CanTransitionPolicyState(current, next contracts.PolicyState) bool {
	if current == next {
		return true
	}
	allowed, exists := AllowedPolicyTransitions[current]
	if !exists {
		return false
	}
	return allowed[next]
}

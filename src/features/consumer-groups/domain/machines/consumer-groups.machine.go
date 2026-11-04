package machines

import (
	"errors"

	"event-driven-platform/src/features/consumer-groups/contracts"
)

var (
	ErrInvalidMemberTransition = errors.New("invalid consumer group member transition")
)

func TransitionMemberStatus(current contracts.MemberStatus, next contracts.MemberStatus) (contracts.MemberStatus, error) {
	switch current {
	case contracts.MemberStatusJoining:
		if next == contracts.MemberStatusStable || next == contracts.MemberStatusDead {
			return next, nil
		}
	case contracts.MemberStatusStable:
		if next == contracts.MemberStatusRebalancing || next == contracts.MemberStatusDead {
			return next, nil
		}
	case contracts.MemberStatusRebalancing:
		if next == contracts.MemberStatusStable || next == contracts.MemberStatusDead {
			return next, nil
		}
	case contracts.MemberStatusDead:
		if next == contracts.MemberStatusJoining {
			return next, nil
		}
	}
	return current, ErrInvalidMemberTransition
}


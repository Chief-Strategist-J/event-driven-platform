package rules

import (
	"errors"
	"strings"
	"time"

	"event-driven-platform/src/features/consumer-groups/contracts"
)

var (
	ErrEagerAssignorDisallowed = errors.New("eager rebalance assignors are strictly banned per Tier 1 rules")
	ErrMaxPollIntervalExceeded = errors.New("processing duration exceeded 80 percent of max.poll.interval; partition pause required")
	ErrSessionTimeoutBreached  = errors.New("heartbeat age exceeded session.timeout.ms; member marked dead")
)

const (
	SessionTimeoutMs   int64 = 45000
	HeartbeatInterval  int64 = 15000
	MaxPollIntervalMs  int64 = 300000
	MaxPollRecordsCap  int   = 500
	FetchMaxBytesCap   int64 = 52428800
)

func VerifyAssignorStrategy(strategy string) error {
	trimmed := strings.TrimSpace(strategy)
	if strings.Contains(trimmed, "RangeAssignor") || strings.Contains(trimmed, "RoundRobinAssignor") {
		return ErrEagerAssignorDisallowed
	}
	if trimmed == "" || trimmed == contracts.CanonicalCooperativeStickyAssignor {
		return nil
	}
	return ErrEagerAssignorDisallowed
}

func VerifyBatchProcessingWatchdog(durationMs int64) (shouldPause bool) {
	threshold := int64(float64(MaxPollIntervalMs) * 0.8)
	return durationMs >= threshold
}

func EvaluateMemberLiveness(lastHeartbeat time.Time, now time.Time) contracts.MemberStatus {
	elapsedMs := now.Sub(lastHeartbeat).Milliseconds()
	if elapsedMs > SessionTimeoutMs {
		return contracts.MemberStatusDead
	}
	return contracts.MemberStatusStable
}

func ComputeCooperativePartitionMigration(
	current []contracts.PartitionAssignment,
	incoming []contracts.PartitionAssignment,
) (retained []contracts.PartitionAssignment, revoked []contracts.PartitionAssignment, added []contracts.PartitionAssignment) {
	incomingMap := make(map[string]contracts.PartitionAssignment)
	for _, p := range incoming {
		key := p.Topic + ":" + string(rune(p.Partition))
		incomingMap[key] = p
	}

	currentMap := make(map[string]contracts.PartitionAssignment)
	for _, p := range current {
		key := p.Topic + ":" + string(rune(p.Partition))
		currentMap[key] = p
		if _, exists := incomingMap[key]; exists {
			retained = append(retained, p)
		} else {
			revoked = append(revoked, p)
		}
	}

	for key, p := range incomingMap {
		if _, exists := currentMap[key]; !exists {
			added = append(added, p)
		}
	}

	return retained, revoked, added
}

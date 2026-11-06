package rules

import (
	"math"
	"sort"
	"strings"

	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/shared/errors"
)

const (
	Murmur2Seed uint32 = 0x9747b28c
	Murmur2M    uint32 = 0x5bd1e995
	Murmur2R    uint32 = 24
)

var (
	ErrInPlaceExpansionForbidden = errors.NewValidationError("in-place partition count expansion is strictly forbidden for keyed topic classes")
	ErrNullKeyOnKeyedTopic       = errors.NewValidationError("null or empty key is forbidden on keyed topic classes")
	ErrPIIDetectedInKey          = errors.NewValidationError("partition key contains personal data (PII) pattern")
	ErrInvalidPartitionCount     = errors.NewValidationError("partition count must be strictly greater than 0")
)

func ValidateTopicTopology(top contracts.TopicTopology) error {
	if top.PartitionCount <= 0 {
		return ErrInvalidPartitionCount
	}
	if top.TopicClass != contracts.TopicClassUnordered && top.AllowInPlaceExpansion {
		return ErrInPlaceExpansionForbidden
	}
	if top.ReplicationFactor < 1 {
		return errors.NewValidationError("replication factor must be at least 1")
	}
	if top.MinInSyncReplicas < 1 {
		return errors.NewValidationError("min insync replicas must be at least 1")
	}
	if top.MinInSyncReplicas > top.ReplicationFactor {
		return errors.NewValidationError("min insync replicas cannot exceed replication factor")
	}
	return nil
}

func FormatAggregateKey(tenantID, aggregateType, aggregateID string) (string, error) {
	t := strings.TrimSpace(tenantID)
	aType := strings.TrimSpace(aggregateType)
	aID := strings.TrimSpace(aggregateID)

	if t == "" || aType == "" || aID == "" {
		return "", ErrNullKeyOnKeyedTopic
	}

	if strings.Contains(aID, "@") || strings.Contains(aID, "+") || strings.Contains(t, "@") {
		return "", ErrPIIDetectedInKey
	}

	return t + ":" + aType + ":" + aID, nil
}

func Murmur2(data []byte) int32 {
	length := len(data)
	h := Murmur2Seed ^ uint32(length)
	length4 := length / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := uint32(data[i4+0]) |
			(uint32(data[i4+1]) << 8) |
			(uint32(data[i4+2]) << 16) |
			(uint32(data[i4+3]) << 24)
		k *= Murmur2M
		k ^= k >> Murmur2R
		k *= Murmur2M
		h *= Murmur2M
		h ^= k
	}

	rem := length % 4
	offset := length - rem
	switch rem {
	case 3:
		h ^= uint32(data[offset+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[offset+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[offset+0])
		h *= Murmur2M
	}

	h ^= h >> 13
	h *= Murmur2M
	h ^= h >> 15

	return int32(h)
}

func ComputeMurmur2Partition(key []byte, partitionCount int) (int, int32, int32, error) {
	if partitionCount <= 0 {
		return 0, 0, 0, ErrInvalidPartitionCount
	}
	rawHash := Murmur2(key)
	maskedPositive := rawHash & 0x7fffffff
	partition := int(maskedPositive) % partitionCount
	return partition, rawHash, maskedPositive, nil
}

func CalculateSkewMetrics(topicName string, partitions []contracts.PartitionThroughput) (contracts.SkewEvaluationOutput, error) {
	if len(partitions) == 0 {
		return contracts.SkewEvaluationOutput{}, errors.NewValidationError("no partition telemetry provided")
	}

	rates := make([]float64, len(partitions))
	var maxRate float64 = -1.0
	var hotPartitionID int = 0

	for i, p := range partitions {
		rates[i] = p.ThroughputEPS
		if p.ThroughputEPS > maxRate {
			maxRate = p.ThroughputEPS
			hotPartitionID = p.PartitionID
		}
	}

	sort.Float64s(rates)
	var medianRate float64
	n := len(rates)
	if n%2 == 1 {
		medianRate = rates[n/2]
	} else {
		medianRate = (rates[(n/2)-1] + rates[n/2]) / 2.0
	}

	if medianRate <= 0.0 {
		medianRate = 1.0
	}

	skewRatio := math.Round((maxRate/medianRate)*10000) / 10000

	isAlertBreached := skewRatio > 3.0
	remediation := "MAINTAIN_CURRENT_ROUTING"
	if isAlertBreached {
		remediation = "ISOLATE_HOT_AGGREGATE_TO_DEDICATED_TOPIC"
	}

	return contracts.SkewEvaluationOutput{
		TopicName:           topicName,
		MaxThroughputEPS:    maxRate,
		MedianThroughputEPS: medianRate,
		SkewRatio:           skewRatio,
		HotPartitionID:      hotPartitionID,
		IsAlertBreached:     isAlertBreached,
		RemediationAction:   remediation,
	}, nil
}

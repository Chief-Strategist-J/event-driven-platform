package adapters

import (
	"event-driven-platform/src/features/partitioning-strategy/domain/rules"
	"event-driven-platform/src/features/partitioning-strategy/infrastructure/ports"
)

type Murmur2HasherAdapter struct{}

func NewMurmur2HasherAdapter() ports.HasherPort {
	return &Murmur2HasherAdapter{}
}

func (a *Murmur2HasherAdapter) HashBytes(data []byte) int32 {
	return rules.Murmur2(data)
}

func (a *Murmur2HasherAdapter) CalculatePartition(keyBytes []byte, partitionCount int) (int, int32, int32, error) {
	return rules.ComputeMurmur2Partition(keyBytes, partitionCount)
}


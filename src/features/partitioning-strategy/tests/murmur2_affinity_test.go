package tests

import (
	"fmt"
	"testing"

	"event-driven-platform/src/features/partitioning-strategy/domain/rules"
)

func TestMurmur2_DeterministicHashingAndBitmask(t *testing.T) {
	key := []byte("tenant_alpha:orders:ord_998877")

	hash1 := rules.Murmur2(key)
	hash2 := rules.Murmur2(key)
	if hash1 != hash2 {
		t.Fatalf("expected identical hash for identical bytes, got %d vs %d", hash1, hash2)
	}

	p1, raw1, masked1, err := rules.ComputeMurmur2Partition(key, 16)
	if err != nil {
		t.Fatalf("expected partition calculation to succeed, got: %v", err)
	}
	if p1 < 0 || p1 >= 16 {
		t.Fatalf("partition %d out of range [0, 15]", p1)
	}
	if masked1 < 0 {
		t.Fatalf("masked hash must be non-negative, got %d", masked1)
	}
	if raw1 != hash1 {
		t.Fatalf("raw hash mismatch, expected %d got %d", hash1, raw1)
	}
}

func TestMurmur2_InPlacePartitionRemappingRisk(t *testing.T) {
	key := []byte("tenant_bravo:ledgers:acc_001")

	pOld, _, _, err := rules.ComputeMurmur2Partition(key, 12)
	if err != nil {
		t.Fatalf("error computing partition for 12: %v", err)
	}

	pNew, _, _, err := rules.ComputeMurmur2Partition(key, 18)
	if err != nil {
		t.Fatalf("error computing partition for 18: %v", err)
	}

	foundRemap := false
	for i := 0; i < 50; i++ {
		testKey := []byte(fmt.Sprintf("tenant_test:accounts:acc_%d", i))
		p12, _, _, _ := rules.ComputeMurmur2Partition(testKey, 12)
		p18, _, _, _ := rules.ComputeMurmur2Partition(testKey, 18)
		if p12 != p18 {
			foundRemap = true
			break
		}
	}

	if !foundRemap {
		t.Fatalf("expected partition expansion to remap key hashes across partition sets")
	}

	_ = pOld
	_ = pNew
}


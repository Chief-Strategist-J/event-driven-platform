package rules

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
)

func EvaluateThreeValuedConsistency(in contracts.EvaluateRecordInput) contracts.EvaluationResult {
	if in.QueryErrorOccurred || in.TargetShardsFailed > 0 {
		return contracts.EvaluationResult{
			Outcome:           contracts.OutcomeUnknown,
			DifferencePayload: "TARGET_QUERY_OR_SHARD_DEGRADED",
			RequiresHeal:      false,
		}
	}

	if in.ReplicaLagThreshold > 0 && in.ReplicaLagMs > in.ReplicaLagThreshold {
		return contracts.EvaluationResult{
			Outcome:           contracts.OutcomeUnknown,
			DifferencePayload: "SOURCE_REPLICA_LAG_EXCEEDED",
			RequiresHeal:      false,
		}
	}

	if in.IsManualWrite {
		return contracts.EvaluationResult{
			Outcome:           contracts.OutcomeMismatch,
			DriftClass:        contracts.DriftManualWrite,
			DifferencePayload: "UNAUTHORIZED_OUT_OF_PIPELINE_MUTATION",
			RequiresHeal:      false,
		}
	}

	if in.SourceExists && !in.ViewExists {
		return contracts.EvaluationResult{
			Outcome:           contracts.OutcomeMismatch,
			DriftClass:        contracts.DriftMissing,
			DifferencePayload: "RECORD_ABSENT_IN_VIEW",
			RequiresHeal:      true,
		}
	}

	if !in.SourceExists && in.ViewExists {
		return contracts.EvaluationResult{
			Outcome:           contracts.OutcomeMismatch,
			DriftClass:        contracts.DriftGhost,
			DifferencePayload: "RECORD_GHOST_IN_VIEW",
			RequiresHeal:      true,
		}
	}

	if in.SourceExists && in.ViewExists {
		if in.SourceVersion == in.ViewVersion && in.SourcePayloadHash == in.ViewPayloadHash {
			return contracts.EvaluationResult{
				Outcome:      contracts.OutcomeMatch,
				RequiresHeal: false,
			}
		}

		if in.SourceVersion > in.ViewVersion {
			return contracts.EvaluationResult{
				Outcome:           contracts.OutcomeMismatch,
				DriftClass:        contracts.DriftVersionLag,
				DifferencePayload: fmt.Sprintf("VIEW_LAGGING: source_v=%d, view_v=%d", in.SourceVersion, in.ViewVersion),
				RequiresHeal:      true,
			}
		}

		return contracts.EvaluationResult{
			Outcome:           contracts.OutcomeMismatch,
			DriftClass:        contracts.DriftAttributeMismatch,
			DifferencePayload: fmt.Sprintf("PAYLOAD_HASH_MISMATCH: source_hash=%s, view_hash=%s", in.SourcePayloadHash, in.ViewPayloadHash),
			RequiresHeal:      true,
		}
	}

	return contracts.EvaluationResult{
		Outcome:      contracts.OutcomeMatch,
		RequiresHeal: false,
	}
}

func CalculateWatermarkStatus(freshnessLagMs int64, sloMs int64, offsetLag int64) string {
	if offsetLag == 0 && freshnessLagMs <= sloMs {
		return "IN_SYNC"
	}
	if freshnessLagMs > 5*sloMs || offsetLag > 1000 {
		return "STALLED"
	}
	return "LAGGING"
}

func CalculateCoverageRatio(match int64, mismatch int64, unknown int64) float64 {
	total := match + mismatch + unknown
	if total == 0 {
		return 1.0000
	}
	return float64(match+mismatch) / float64(total)
}

func EvaluateRepeatHeal(healAttempts int, maxAllowed int) bool {
	return healAttempts < maxAllowed
}

func ComputeMerkleBucketHash(keys []string, payloadHashes []string) string {
	n := len(keys)
	if n == 0 {
		h := sha256.Sum256([]byte("EMPTY_BUCKET"))
		return hex.EncodeToString(h[:])
	}

	type kv struct {
		key  string
		hash string
	}
	items := make([]kv, n)
	for i := 0; i < n; i++ {
		items[i] = kv{key: keys[i], hash: payloadHashes[i]}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})

	var modularSum uint64 = 0
	var keyXOR uint64 = 0

	for _, it := range items {
		recordDigest := sha256.Sum256([]byte(it.key + ":" + it.hash))
		recordInt := binary.BigEndian.Uint64(recordDigest[:8])
		keyDigest := sha256.Sum256([]byte(it.key))
		keyInt := binary.BigEndian.Uint64(keyDigest[:8])

		modularSum += recordInt
		keyXOR ^= keyInt
	}

	hasher := sha256.New()
	sumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sumBytes, modularSum)
	hasher.Write(sumBytes)

	countBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(countBytes, uint64(n))
	hasher.Write(countBytes)

	xorBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(xorBytes, keyXOR)
	hasher.Write(xorBytes)

	finalHash := hasher.Sum(nil)
	return hex.EncodeToString(finalHash)
}


package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/shared/errors"
)

var (
	ErrViewDecommissioned = errors.NewConflictError("materialized view is decommissioned")
	ErrInvalidViewName    = errors.NewValidationError("invalid view name format")
)

func ValidateViewRegistration(reg contracts.ViewRegistryRecord) error {
	if reg.ViewName == "" {
		return ErrInvalidViewName
	}
	if reg.FreshnessSLOMs <= 0 {
		return errors.NewValidationError("freshness SLO must be strictly positive")
	}
	switch reg.ViewClass {
	case contracts.ViewClassState, contracts.ViewClassAgg, contracts.ViewClassJoin,
		contracts.ViewClassSearch, contracts.ViewClassKV, contracts.ViewClassReport:
	default:
		return errors.NewValidationError("unsupported view class")
	}
	switch reg.PrimaryStore {
	case contracts.StorageTargetPostgres, contracts.StorageTargetRedis, contracts.StorageTargetOpenSearch:
	default:
		return errors.NewValidationError("unsupported primary storage target")
	}
	return nil
}

func EvaluateProjectionVersion(
	currentVersion int64,
	lastHash string,
	incoming contracts.ApplyProjectionInput,
) (contracts.ProjectionEvaluation, error) {
	hasher := sha256.New()
	hasher.Write([]byte(incoming.PayloadRaw))
	incomingHash := hex.EncodeToString(hasher.Sum(nil))

	if incoming.AggregateSeq <= currentVersion {
		if incoming.AggregateSeq == currentVersion && incomingHash == lastHash {
			return contracts.ProjectionEvaluation{
				Decision:      contracts.DecisionDuplicate,
				ExpectedSeq:   currentVersion + 1,
				IncomingSeq:   incoming.AggregateSeq,
				PayloadHash:   incomingHash,
				GapSize:       0,
				IsQuarantined: false,
			}, nil
		}
		return contracts.ProjectionEvaluation{
			Decision:      contracts.DecisionStaleDrop,
			ExpectedSeq:   currentVersion + 1,
			IncomingSeq:   incoming.AggregateSeq,
			PayloadHash:   incomingHash,
			GapSize:       0,
			IsQuarantined: false,
		}, nil
	}

	if incoming.AggregateSeq == currentVersion+1 {
		return contracts.ProjectionEvaluation{
			Decision:      contracts.DecisionApply,
			ExpectedSeq:   currentVersion + 1,
			IncomingSeq:   incoming.AggregateSeq,
			PayloadHash:   incomingHash,
			GapSize:       0,
			IsQuarantined: false,
		}, nil
	}

	gap := incoming.AggregateSeq - (currentVersion + 1)
	return contracts.ProjectionEvaluation{
		Decision:      contracts.DecisionGapHold,
		ExpectedSeq:   currentVersion + 1,
		IncomingSeq:   incoming.AggregateSeq,
		PayloadHash:   incomingHash,
		GapSize:       gap,
		IsQuarantined: true,
	}, nil
}

func CalculateFreshnessLag(materializedAt time.Time, now time.Time) time.Duration {
	if materializedAt.IsZero() {
		return 0
	}
	lag := now.Sub(materializedAt)
	if lag < 0 {
		return 0
	}
	return lag
}

func IsFreshnessSLOBreached(materializedAt time.Time, sloMs int64, now time.Time) bool {
	if sloMs <= 0 || materializedAt.IsZero() {
		return false
	}
	lag := CalculateFreshnessLag(materializedAt, now)
	return lag.Milliseconds() > sloMs
}

func FormatConsistencyToken(viewName string, seq int64) string {
	return fmt.Sprintf("cv_tok_%s_%d", viewName, seq)
}

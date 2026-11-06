package tests

import (
	"testing"
	"time"

	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/features/materialized-views/domain/rules"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateViewRegistration(t *testing.T) {
	valid := contracts.ViewRegistryRecord{
		ViewName:       "customer_360",
		FreshnessSLOMs: 500,
		ViewClass:      contracts.ViewClassState,
		PrimaryStore:   contracts.StorageTargetPostgres,
	}
	err := rules.ValidateViewRegistration(valid)
	assert.NoError(t, err)

	invalidName := valid
	invalidName.ViewName = ""
	assert.Error(t, rules.ValidateViewRegistration(invalidName))

	invalidSLO := valid
	invalidSLO.FreshnessSLOMs = 0
	assert.Error(t, rules.ValidateViewRegistration(invalidSLO))

	invalidClass := valid
	invalidClass.ViewClass = "INVALID_CLASS"
	assert.Error(t, rules.ValidateViewRegistration(invalidClass))

	invalidStore := valid
	invalidStore.PrimaryStore = "CASSANDRA"
	assert.Error(t, rules.ValidateViewRegistration(invalidStore))
}

func TestEvaluateProjectionVersion_Apply(t *testing.T) {
	input := contracts.ApplyProjectionInput{
		TenantID:      "tenant-alpha",
		ViewName:      "customer_360",
		AggregateType: "customer",
		AggregateID:   "cust-001",
		AggregateSeq:  5,
		EventID:       uuid.New().String(),
		PayloadRaw:    `{"name":"Alice","tier":"PLATINUM"}`,
	}

	eval, err := rules.EvaluateProjectionVersion(4, "some-prior-hash", input)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionApply, eval.Decision)
	assert.Equal(t, int64(5), eval.ExpectedSeq)
	assert.Equal(t, int64(5), eval.IncomingSeq)
	assert.False(t, eval.IsQuarantined)
	assert.NotEmpty(t, eval.PayloadHash)
}

func TestEvaluateProjectionVersion_DuplicateNoop(t *testing.T) {
	payload := `{"name":"Alice","tier":"PLATINUM"}`
	input := contracts.ApplyProjectionInput{
		TenantID:      "tenant-alpha",
		ViewName:      "customer_360",
		AggregateType: "customer",
		AggregateID:   "cust-001",
		AggregateSeq:  4,
		EventID:       uuid.New().String(),
		PayloadRaw:    payload,
	}

	firstEval, err := rules.EvaluateProjectionVersion(3, "prior-hash", input)
	require.NoError(t, err)

	dupEval, err := rules.EvaluateProjectionVersion(4, firstEval.PayloadHash, input)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionDuplicate, dupEval.Decision)
	assert.False(t, dupEval.IsQuarantined)
}

func TestEvaluateProjectionVersion_StaleDrop(t *testing.T) {
	input := contracts.ApplyProjectionInput{
		TenantID:      "tenant-alpha",
		ViewName:      "customer_360",
		AggregateType: "customer",
		AggregateID:   "cust-001",
		AggregateSeq:  3,
		EventID:       uuid.New().String(),
		PayloadRaw:    `{"name":"Alice","tier":"SILVER"}`,
	}

	eval, err := rules.EvaluateProjectionVersion(5, "hash-at-v5", input)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionStaleDrop, eval.Decision)
	assert.False(t, eval.IsQuarantined)
}

func TestEvaluateProjectionVersion_GapHold(t *testing.T) {
	input := contracts.ApplyProjectionInput{
		TenantID:      "tenant-alpha",
		ViewName:      "customer_360",
		AggregateType: "customer",
		AggregateID:   "cust-001",
		AggregateSeq:  8,
		EventID:       uuid.New().String(),
		PayloadRaw:    `{"name":"Alice","tier":"DIAMOND"}`,
	}

	eval, err := rules.EvaluateProjectionVersion(4, "hash-at-v4", input)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionGapHold, eval.Decision)
	assert.Equal(t, int64(5), eval.ExpectedSeq)
	assert.Equal(t, int64(8), eval.IncomingSeq)
	assert.Equal(t, int64(3), eval.GapSize)
	assert.True(t, eval.IsQuarantined)
}

func TestFreshnessLagAndSLO(t *testing.T) {
	now := time.Now().UTC()
	matAt := now.Add(-2 * time.Second)

	lag := rules.CalculateFreshnessLag(matAt, now)
	assert.InDelta(t, 2000, lag.Milliseconds(), 50)

	assert.True(t, rules.IsFreshnessSLOBreached(matAt, 1000, now))
	assert.False(t, rules.IsFreshnessSLOBreached(matAt, 5000, now))
	assert.False(t, rules.IsFreshnessSLOBreached(time.Time{}, 1000, now))
}

func TestFormatConsistencyToken(t *testing.T) {
	token := rules.FormatConsistencyToken("orders_summary", 42)
	assert.Equal(t, "cv_tok_orders_summary_42", token)
}

func TestEvaluateProjectionVersion_IntegrityConflict(t *testing.T) {
	input := contracts.ApplyProjectionInput{
		TenantID:      "tenant-alpha",
		ViewName:      "customer_360",
		AggregateType: "customer",
		AggregateID:   "cust-001",
		AggregateSeq:  4,
		EventID:       uuid.New().String(),
		PayloadRaw:    `{"name":"Alice","tier":"DIAMOND_CHANGED"}`,
	}

	eval, err := rules.EvaluateProjectionVersion(4, "original-hash-at-v4", input)
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionIntegrityConflict, eval.Decision)
	assert.True(t, eval.IsQuarantined)
}

func TestParseConsistencyToken(t *testing.T) {
	viewName, seq, err := rules.ParseConsistencyToken("cv_tok_orders_summary_104")
	require.NoError(t, err)
	assert.Equal(t, "orders_summary", viewName)
	assert.Equal(t, int64(104), seq)

	_, _, err = rules.ParseConsistencyToken("invalid_prefix_104")
	assert.Error(t, err)

	_, _, err = rules.ParseConsistencyToken("cv_tok_orders_bad")
	assert.Error(t, err)
}

func TestEvaluatePeriodEventApplication(t *testing.T) {
	isAdj, err := rules.EvaluatePeriodEventApplication(contracts.PeriodStatusOpen, 100.0)
	require.NoError(t, err)
	assert.False(t, isAdj)

	isAdj, err = rules.EvaluatePeriodEventApplication(contracts.PeriodStatusFrozen, 100.0)
	require.NoError(t, err)
	assert.True(t, isAdj)

	isAdj, err = rules.EvaluatePeriodEventApplication(contracts.PeriodStatusClosed, 100.0)
	require.NoError(t, err)
	assert.True(t, isAdj)

	_, err = rules.EvaluatePeriodEventApplication("UNKNOWN", 100.0)
	assert.Error(t, err)
}

func TestCalculateContributionDelta(t *testing.T) {
	assert.Equal(t, 50.0, rules.CalculateContributionDelta(50.0, false))
	assert.Equal(t, -50.0, rules.CalculateContributionDelta(50.0, true))
}


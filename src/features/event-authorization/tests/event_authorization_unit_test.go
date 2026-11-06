package tests

import (
	"net/http"
	"testing"

	"event-driven-platform/src/features/event-authorization/contracts"
	"event-driven-platform/src/features/event-authorization/domain/machines"
	"event-driven-platform/src/features/event-authorization/domain/rules"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventAuthorizationRules_SanitizeInboundHeaders(t *testing.T) {
	req, err := http.NewRequest("GET", "/api/v1/orders", nil)
	require.NoError(t, err)

	req.Header.Set("X-User-ID", "attacker_user_99")
	req.Header.Set("X-Roles", "admin,superoperator")
	req.Header.Set("X-Tenant-ID", "rogue_tenant")
	req.Header.Set("Authorization", "Bearer legitimate_token")

	rules.SanitizeInboundHeaders(req.Header)

	assert.Empty(t, req.Header.Get("X-User-ID"), "AZ-01: Inbound plain user header must be stripped")
	assert.Empty(t, req.Header.Get("X-Roles"), "AZ-01: Inbound plain role header must be stripped")
	assert.Empty(t, req.Header.Get("X-Tenant-ID"), "AZ-01: Inbound plain tenant header must be stripped")
	assert.NotEmpty(t, req.Header.Get("Authorization"), "Bearer token must be preserved")
}

func TestEventAuthorizationRules_PatternAndOperationMatching(t *testing.T) {
	assert.True(t, rules.MatchResourcePattern("orders.*", "orders.order.placed.v1"))
	assert.True(t, rules.MatchResourcePattern("orders.order.placed.v1", "orders.order.placed.v1"))
	assert.True(t, rules.MatchResourcePattern("*", "payments.processed.v1"))
	assert.False(t, rules.MatchResourcePattern("orders.*", "payments.processed.v1"))

	assert.True(t, rules.MatchOperation(contracts.OpAll, contracts.OpPublish))
	assert.True(t, rules.MatchOperation(contracts.OpPublish, contracts.OpPublish))
	assert.False(t, rules.MatchOperation(contracts.OpSubscribe, contracts.OpPublish))
}

func TestEventAuthorizationRules_AudienceRestriction(t *testing.T) {
	assert.NoError(t, rules.ValidateTokenAudience("order-service", "order-service"))
	assert.ErrorIs(t, rules.ValidateTokenAudience("billing-service", "order-service"), contracts.ErrAudienceMismatch)
}

func TestEventAuthorizationRules_PolicyEvaluation(t *testing.T) {
	p1 := &contracts.AuthPolicyRecord{
		PublicID:        "pol_01",
		ResourcePattern: "orders.*",
		Operation:       contracts.OpPublish,
		Effect:          contracts.EffectAllow,
		State:           contracts.PolicyStateActive,
	}
	p2 := &contracts.AuthPolicyRecord{
		PublicID:        "pol_02",
		ResourcePattern: "orders.fraudulent.*",
		Operation:       contracts.OpPublish,
		Effect:          contracts.EffectDeny,
		State:           contracts.PolicyStateActive,
	}

	policies := []*contracts.AuthPolicyRecord{p1, p2}

	dec, matched, _ := rules.EvaluatePolicies(policies, "orders.order.placed.v1", contracts.OpPublish)
	assert.Equal(t, contracts.DecisionAllowed, dec)
	assert.Equal(t, "pol_01", matched)

	decDeny, matchedDeny, _ := rules.EvaluatePolicies(policies, "orders.fraudulent.test", contracts.OpPublish)
	assert.Equal(t, contracts.DecisionDenied, decDeny)
	assert.Equal(t, "pol_02", matchedDeny)

	decDefault, matchedDefault, _ := rules.EvaluatePolicies(policies, "payments.charge.v1", contracts.OpPublish)
	assert.Equal(t, contracts.DecisionDenied, decDefault)
	assert.Equal(t, "NONE", matchedDefault)
}

func TestEventAuthorizationMachine_Transitions(t *testing.T) {
	assert.True(t, machines.CanTransitionPolicyState(contracts.PolicyStateActive, contracts.PolicyStateDisabled))
	assert.True(t, machines.CanTransitionPolicyState(contracts.PolicyStateActive, contracts.PolicyStateRevoked))
	assert.True(t, machines.CanTransitionPolicyState(contracts.PolicyStateDisabled, contracts.PolicyStateActive))
	assert.True(t, machines.CanTransitionPolicyState(contracts.PolicyStateDisabled, contracts.PolicyStateRevoked))
	assert.False(t, machines.CanTransitionPolicyState(contracts.PolicyStateRevoked, contracts.PolicyStateActive))
}

func TestEventAuthorizationContracts_Normalization(t *testing.T) {
	_, err := contracts.NormalizeCreatePolicyInput(nil)
	assert.Error(t, err)

	_, err = contracts.NormalizeCreatePolicyInput(&contracts.CreatePolicyInput{
		TenantID:        "  ",
		PolicyName:      "test",
		PrincipalID:     "srv_1",
		PrincipalType:   contracts.PrincipalService,
		ResourceType:    contracts.ResourceTopic,
		ResourcePattern: "orders.*",
		Operation:       contracts.OpPublish,
		Effect:          contracts.EffectAllow,
	})
	assert.ErrorIs(t, err, contracts.ErrInvalidTenantID)

	rec, err := contracts.NormalizeCreatePolicyInput(&contracts.CreatePolicyInput{
		TenantID:        "tenant_01",
		SourceService:   "policy-manager",
		ExternalID:      "ext_pol_01",
		CorrelationID:   uuid.New(),
		PolicyName:      "allow_order_publish",
		PrincipalID:     "srv_order",
		PrincipalType:   contracts.PrincipalService,
		ResourceType:    contracts.ResourceTopic,
		ResourcePattern: "orders.*",
		Operation:       contracts.OpPublish,
		Effect:          contracts.EffectAllow,
	})
	require.NoError(t, err)
	assert.True(t, contracts.ValidatePolicyPublicID(rec.PublicID))
	assert.Equal(t, contracts.PolicyStateActive, rec.State)
}

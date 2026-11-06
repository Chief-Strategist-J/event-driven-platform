package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"event-driven-platform/src/features/event-authorization/contracts"
	"event-driven-platform/src/features/event-authorization/infrastructure/adapters"
	"event-driven-platform/src/features/event-authorization/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func getAuthTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("skipping live database test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping live database test: %v", err)
	}

	return db
}

func TestEventAuthorization_PostgresLifecycleAndEvaluation(t *testing.T) {
	db := getAuthTestDB(t)
	defer db.Close()
	ctx := context.Background()

	repo := adapters.NewPostgresEventAuthorizationRepository(db)
	svc := service.NewEventAuthorizationService(repo)

	tenantID := fmt.Sprintf("tenant_auth_%d", time.Now().UnixNano())
	servicePrincipal := "service_checkout_worker"
	topicName := "checkout.orders.placed.v1"

	// 1. Evaluate without policy -> Must trigger DEFAULT DENY
	outDefault, err := svc.EvaluateAccess(ctx, &contracts.EvaluateAccessInput{
		TenantID:      tenantID,
		SourceService: "checkout-service",
		ExternalID:    "eval_ext_01",
		CorrelationID: uuid.New(),
		PrincipalID:   servicePrincipal,
		PrincipalType: contracts.PrincipalService,
		ResourceType:  contracts.ResourceTopic,
		ResourceName:  topicName,
		Operation:     contracts.OpPublish,
	})
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionDenied, outDefault.Decision)
	assert.Equal(t, "NONE", outDefault.MatchedPolicyID)

	// 2. Provision ALLOW Policy
	pol, err := svc.ProvisionPolicy(ctx, &contracts.CreatePolicyInput{
		TenantID:        tenantID,
		SourceService:   "policy-manager",
		ExternalID:      "pol_ext_01",
		CorrelationID:   uuid.New(),
		PolicyName:      "allow_checkout_publish",
		PrincipalID:     servicePrincipal,
		PrincipalType:   contracts.PrincipalService,
		ResourceType:    contracts.ResourceTopic,
		ResourcePattern: "checkout.orders.*",
		Operation:       contracts.OpPublish,
		Effect:          contracts.EffectAllow,
	})
	require.NoError(t, err)
	assert.True(t, contracts.ValidatePolicyPublicID(pol.PublicID))

	// 3. Test AZ-04 Single Writer Invariant: Second distinct service attempting write on same pattern fails
	_, errSecondWriter := svc.ProvisionPolicy(ctx, &contracts.CreatePolicyInput{
		TenantID:        tenantID,
		SourceService:   "policy-manager",
		ExternalID:      "pol_ext_02",
		CorrelationID:   uuid.New(),
		PolicyName:      "allow_rogue_writer",
		PrincipalID:     "service_rogue_worker",
		PrincipalType:   contracts.PrincipalService,
		ResourceType:    contracts.ResourceTopic,
		ResourcePattern: "checkout.orders.*",
		Operation:       contracts.OpPublish,
		Effect:          contracts.EffectAllow,
	})
	assert.ErrorIs(t, errSecondWriter, contracts.ErrSingleWriterViolation)

	// 4. Evaluate with policy -> Must ALLOW
	outAllow, err := svc.EvaluateAccess(ctx, &contracts.EvaluateAccessInput{
		TenantID:      tenantID,
		SourceService: "checkout-service",
		ExternalID:    "eval_ext_02",
		CorrelationID: uuid.New(),
		PrincipalID:   servicePrincipal,
		PrincipalType: contracts.PrincipalService,
		ResourceType:  contracts.ResourceTopic,
		ResourceName:  topicName,
		Operation:     contracts.OpPublish,
	})
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionAllowed, outAllow.Decision)
	assert.Equal(t, pol.PublicID, outAllow.MatchedPolicyID)

	// 5. Revoke policy and verify that evaluation immediately returns DENIED
	err = svc.UpdatePolicyState(ctx, pol.PublicID, contracts.PolicyStateRevoked)
	require.NoError(t, err)

	outRevoked, err := svc.EvaluateAccess(ctx, &contracts.EvaluateAccessInput{
		TenantID:      tenantID,
		SourceService: "checkout-service",
		ExternalID:    "eval_ext_03",
		CorrelationID: uuid.New(),
		PrincipalID:   servicePrincipal,
		PrincipalType: contracts.PrincipalService,
		ResourceType:  contracts.ResourceTopic,
		ResourceName:  topicName,
		Operation:     contracts.OpPublish,
	})
	require.NoError(t, err)
	assert.Equal(t, contracts.DecisionDenied, outRevoked.Decision)
}

func TestEventAuthorization_PhysicalCheckConstraints(t *testing.T) {
	db := getAuthTestDB(t)
	defer db.Close()
	ctx := context.Background()

	t.Run("RejectMalformedPolicyPublicID", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO auth_policy_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				policy_name, principal_id, principal_type, resource_type, resource_pattern,
				operation, effect, state
			) VALUES (
				gen_random_uuid(), 'bad_pol', 't1', 'src', 'ext1', gen_random_uuid(),
				'pname', 'pr1', 'SERVICE', 'TOPIC', 'orders.*', 'PUBLISH', 'ALLOW', 'ACTIVE'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_auth_policy_public_id_format")
	})

	t.Run("RejectInvalidOperation", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO auth_policy_ledger (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				policy_name, principal_id, principal_type, resource_type, resource_pattern,
				operation, effect, state
			) VALUES (
				gen_random_uuid(), 'pol_1234567890abcdef', 't1', 'src', 'ext2', gen_random_uuid(),
				'pname2', 'pr1', 'SERVICE', 'TOPIC', 'orders.*', 'INVALID_OP', 'ALLOW', 'ACTIVE'
			);
		`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_auth_policy_operation")
	})

	t.Run("RejectAuditLogMutationViaTrigger", func(t *testing.T) {
		pubID := "aal_1234567890abcdef"
		_, err := db.ExecContext(ctx, `
			INSERT INTO auth_audit_log (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				principal_id, principal_type, resource_type, resource_name,
				operation, decision, matched_policy_id, reason
			) VALUES (
				gen_random_uuid(), $1, 't1', 'src', 'ext1', gen_random_uuid(),
				'pr1', 'SERVICE', 'TOPIC', 'topic1', 'PUBLISH', 'ALLOWED', 'NONE', 'test reason'
			);
		`, pubID)
		require.NoError(t, err)

		_, errUpdate := db.ExecContext(ctx, `UPDATE auth_audit_log SET decision = 'DENIED' WHERE public_id = $1;`, pubID)
		require.Error(t, errUpdate)
		assert.Contains(t, errUpdate.Error(), "auth_audit_log is strictly immutable")
	})
}

func TestEventAuthorization_TracingCompliance(t *testing.T) {
	db := getAuthTestDB(t)
	defer db.Close()
	ctx := context.Background()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)

	repo := adapters.NewPostgresEventAuthorizationRepository(db)
	svc := service.NewEventAuthorizationService(repo)

	tenantID := fmt.Sprintf("tenant_auth_tr_%d", time.Now().UnixNano())
	_, err := svc.EvaluateAccess(ctx, &contracts.EvaluateAccessInput{
		TenantID:      tenantID,
		SourceService: "order-service",
		ExternalID:    "ext_eval_tr",
		CorrelationID: uuid.New(),
		PrincipalID:   "srv_test",
		PrincipalType: contracts.PrincipalService,
		ResourceType:  contracts.ResourceTopic,
		ResourceName:  "orders.v1",
		Operation:     contracts.OpPublish,
	})
	require.NoError(t, err)

	spans := exp.GetSpans()
	require.NotEmpty(t, spans)

	var hasEvalSpan bool
	for _, s := range spans {
		if s.Name == "EventAuthorizationService.EvaluateAccess" {
			hasEvalSpan = true
			break
		}
	}
	assert.True(t, hasEvalSpan, "Expected EventAuthorizationService.EvaluateAccess span")
}

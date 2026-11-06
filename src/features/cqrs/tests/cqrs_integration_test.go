package tests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/features/cqrs/infrastructure/adapters"
	"event-driven-platform/src/features/cqrs/service"
)

func getCQRSTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_URL")
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
		t.Skipf("skipping live database test: postgres unreachable: %v", err)
	}

	return db
}

func cleanHex(u uuid.UUID) string {
	return strings.ReplaceAll(u.String(), "-", "")
}

func TestCQRS_PhysicalCheckConstraints(t *testing.T) {
	db := getCQRSTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := "tenant_cqrs_ck_" + cleanHex(uuid.New())[:8]

	t.Run("RejectMalformedCommandPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO cqrs_command_idempotency_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			idempotency_key, command_name, aggregate_type, aggregate_id, request_fingerprint,
			status, response_payload, consistency_token, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_public_id", tenantID, "order-service", "ext_01", uuid.New(),
			"idem_1", "CreateOrder", "Order", "ord_1", strings.Repeat("a", 64),
			"IN_PROGRESS", "{}", "", 1,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_command_idempotency_public_id_format")
	})

	t.Run("RejectInvalidCommandStatusEnum", func(t *testing.T) {
		id := uuid.New()
		publicID := "cmd_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO cqrs_command_idempotency_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			idempotency_key, command_name, aggregate_type, aggregate_id, request_fingerprint,
			status, response_payload, consistency_token, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_02", uuid.New(),
			"idem_2", "CreateOrder", "Order", "ord_2", strings.Repeat("a", 64),
			"INVALID_STATUS", "{}", "", 1,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_command_idempotency_status")
	})

	t.Run("RejectInvalidFingerprintLength", func(t *testing.T) {
		id := uuid.New()
		publicID := "cmd_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO cqrs_command_idempotency_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			idempotency_key, command_name, aggregate_type, aggregate_id, request_fingerprint,
			status, response_payload, consistency_token, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_03", uuid.New(),
			"idem_3", "CreateOrder", "Order", "ord_3", "short_fingerprint",
			"IN_PROGRESS", "{}", "", 1,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_command_idempotency_fingerprint_length")
	})

	t.Run("RejectMalformedAggregatePublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO cqrs_aggregate_root_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			aggregate_type, aggregate_id, current_version, state_payload, is_deleted, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_agg_id", tenantID, "order-service", "ext_04", uuid.New(),
			"Order", "ord_4", 0, "{}", false, 1,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_aggregate_root_public_id_format")
	})

	t.Run("RejectNegativeAggregateVersion", func(t *testing.T) {
		id := uuid.New()
		publicID := "agg_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO cqrs_aggregate_root_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			aggregate_type, aggregate_id, current_version, state_payload, is_deleted, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_05", uuid.New(),
			"Order", "ord_5", -1, "{}", false, 1,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_aggregate_root_version_nonneg")
	})

	t.Run("RejectMalformedSigningKeyPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO cqrs_token_signing_key_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			key_version, secret_key_hash, raw_secret_value, status, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_ctk_id", tenantID, "order-service", "ext_06", uuid.New(),
			1, strings.Repeat("b", 64), "secret_val_12345678901234567890", "ACTIVE", time.Now().Add(time.Hour),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_token_signing_key_public_id_format")
	})

	t.Run("RejectNonPositiveKeyVersion", func(t *testing.T) {
		id := uuid.New()
		publicID := "ctk_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO cqrs_token_signing_key_registry (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			key_version, secret_key_hash, raw_secret_value, status, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "order-service", "ext_07", uuid.New(),
			0, strings.Repeat("b", 64), "secret_val_12345678901234567890", "ACTIVE", time.Now().Add(time.Hour),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_cqrs_token_signing_key_version_pos")
	})
}

func TestCQRS_PostgresLifecycleAndIdempotency(t *testing.T) {
	db := getCQRSTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresCQRSRepositoryAdapter(db)
	svc := service.NewCQRSService(repo)

	tenantID := "tenant_cqrs_life_" + cleanHex(uuid.New())[:8]
	signingKey := "signing_secret_for_cqrs_test_32chars"

	keyInput := contracts.RegisterSigningKeyInput{
		TenantID:       tenantID,
		SourceService:  "cqrs-test-suite",
		CorrelationID:  uuid.New().String(),
		KeyVersion:     1,
		RawSecretValue: signingKey,
		ExpiresAt:      time.Now().Add(24 * time.Hour),
	}
	keyRec, err := svc.RegisterSigningKey(ctx, keyInput)
	require.NoError(t, err)
	assert.NotEmpty(t, keyRec.PublicID)
	assert.Equal(t, 1, keyRec.KeyVersion)

	idempotencyKey := "idem_cmd_" + cleanHex(uuid.New())[:12]
	cmdPayload := `{"item":"Laptop","price":1200}`
	cmd1 := contracts.DispatchCommandInput{
		TenantID:        tenantID,
		SourceService:   "order-service",
		ExternalID:      "ext_cmd_01",
		CorrelationID:   uuid.New().String(),
		IdempotencyKey:  idempotencyKey,
		CommandName:     "PlaceOrder",
		AggregateType:   "Order",
		AggregateID:     "ord_1001",
		ExpectedVersion: 0,
		PayloadRaw:      cmdPayload,
		PrincipalID:     "usr_alice",
	}

	res1, err := svc.DispatchCommand(ctx, cmd1)
	require.NoError(t, err)
	assert.Equal(t, contracts.CommandStatusCompleted, res1.Status)
	assert.Equal(t, int64(1), res1.NewVersion)
	assert.NotEmpty(t, res1.ConsistencyToken)

	res1Duplicate, err := svc.DispatchCommand(ctx, cmd1)
	require.NoError(t, err)
	assert.Equal(t, contracts.CommandStatusCompleted, res1Duplicate.Status)
	assert.Equal(t, res1.ConsistencyToken, res1Duplicate.ConsistencyToken)
	assert.Equal(t, res1.NewVersion, res1Duplicate.NewVersion)

	cmdConflicting := cmd1
	cmdConflicting.PayloadRaw = `{"item":"Laptop","price":999}`
	_, errConflict := svc.DispatchCommand(ctx, cmdConflicting)
	require.Error(t, errConflict)
	assert.Equal(t, service.ErrIdempotencyFingerprintMismatch, errConflict)

	cmdStaleVersion := contracts.DispatchCommandInput{
		TenantID:        tenantID,
		SourceService:   "order-service",
		ExternalID:      "ext_cmd_02",
		CorrelationID:   uuid.New().String(),
		IdempotencyKey:  "idem_cmd_" + cleanHex(uuid.New())[:12],
		CommandName:     "CancelOrder",
		AggregateType:   "Order",
		AggregateID:     "ord_1001",
		ExpectedVersion: 2, // Current is 1!
		PayloadRaw:      `{"reason":"customer_request"}`,
		PrincipalID:     "usr_alice",
	}
	_, errStale := svc.DispatchCommand(ctx, cmdStaleVersion)
	require.Error(t, errStale)
	assert.Equal(t, service.ErrExpectedVersionMismatch, errStale)

	queryInput := contracts.ResolveQueryInput{
		TenantID:            tenantID,
		PrincipalID:         "usr_alice",
		ViewName:            "customer_orders_view",
		AggregateType:       "Order",
		AggregateID:         "ord_1001",
		ConsistencyTokenRaw: res1.ConsistencyToken,
		TimeoutBudgetMs:     500,
	}
	qRes, err := svc.ResolveQuery(ctx, queryInput)
	require.NoError(t, err)
	assert.Equal(t, contracts.ResolutionImmediate, qRes.ConsistencyStatus)
	assert.Equal(t, int64(1), qRes.ExpectedVersion)

	authInput := contracts.AuthoritativeReadInput{
		TenantID:            tenantID,
		PrincipalID:         "usr_alice",
		AggregateType:       "Order",
		AggregateID:         "ord_1001",
		ConsistencyTokenRaw: res1.ConsistencyToken,
	}
	authRes, err := svc.AuthoritativeRead(ctx, authInput)
	require.NoError(t, err)
	require.NotNil(t, authRes)
	assert.Equal(t, int64(1), authRes.CurrentVersion)
}

func TestCQRS_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	db := getCQRSTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresCQRSRepositoryAdapter(db)
	svc := service.NewCQRSService(repo)

	tenantID := "tenant_cqrs_trace_" + cleanHex(uuid.New())[:8]

	_, err := svc.RegisterSigningKey(ctx, contracts.RegisterSigningKeyInput{
		TenantID:       tenantID,
		SourceService:  "trace-suite",
		CorrelationID:  uuid.New().String(),
		KeyVersion:     1,
		RawSecretValue: "tracing_secret_test_key_32_bytes",
		ExpiresAt:      time.Now().Add(time.Hour),
	})
	require.NoError(t, err)

	_, err = svc.DispatchCommand(ctx, contracts.DispatchCommandInput{
		TenantID:        tenantID,
		SourceService:   "trace-suite",
		ExternalID:      "ext_cmd_trace",
		CorrelationID:   uuid.New().String(),
		IdempotencyKey:  "idem_trace_" + cleanHex(uuid.New())[:12],
		CommandName:     "TestTracingCommand",
		AggregateType:   "TraceAggregate",
		AggregateID:     "tagg_1",
		ExpectedVersion: 0,
		PayloadRaw:      `{"test":"tracing"}`,
		PrincipalID:     "usr_trace",
	})
	require.NoError(t, err)

	spans := exporter.GetSpans()
	var hasServiceSpan, hasRepoSpan bool
	for _, span := range spans {
		if span.Name == "CQRSService.DispatchCommand" {
			hasServiceSpan = true
		}
		if span.Name == "PostgresCQRSRepositoryAdapter.GetIdempotencyRecord" ||
			span.Name == "PostgresCQRSRepositoryAdapter.CreateIdempotencyRecord" {
			hasRepoSpan = true
		}
	}

	assert.True(t, hasServiceSpan, "expected CQRSService.DispatchCommand span in trace exporter")
	assert.True(t, hasRepoSpan, "expected PostgresCQRSRepositoryAdapter span in trace exporter")
}

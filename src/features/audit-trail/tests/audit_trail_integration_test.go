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

	"event-driven-platform/src/features/audit-trail/contracts"
	"event-driven-platform/src/features/audit-trail/domain/rules"
	"event-driven-platform/src/features/audit-trail/infrastructure/adapters"
	"event-driven-platform/src/features/audit-trail/service"
)

func getAuditTestDB(t *testing.T) *sql.DB {
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

func TestAuditTrail_PhysicalCheckConstraints(t *testing.T) {
	db := getAuditTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := "tenant_aud_ck_" + cleanHex(uuid.New())[:8]

	t.Run("RejectMalformedAuditPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO audit_trail_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			shard_id, sequence_number, actor_id, actor_type, action_name,
			target_type, target_id, outcome, compliance_class,
			prev_record_hash, record_hash, canonical_payload, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_aud_id", tenantID, "audit-service", "ext_01", uuid.New(),
			"shard_1", 1, "usr_1", "USER", "orders.order.cancelled",
			"order", "ord_1", "SUCCEEDED", "CLASS_A",
			rules.GenesisHash, rules.GenesisHash, "{}", time.Now(),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_audit_trail_public_id_format")
	})

	t.Run("RejectInvalidActorTypeEnum", func(t *testing.T) {
		id := uuid.New()
		publicID := "aud_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO audit_trail_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			shard_id, sequence_number, actor_id, actor_type, action_name,
			target_type, target_id, outcome, compliance_class,
			prev_record_hash, record_hash, canonical_payload, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "audit-service", "ext_02", uuid.New(),
			"shard_1", 1, "usr_1", "ROBOT", "orders.order.cancelled",
			"order", "ord_1", "SUCCEEDED", "CLASS_A",
			rules.GenesisHash, rules.GenesisHash, "{}", time.Now(),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_audit_trail_actor_type")
	})

	t.Run("RejectNonPositiveSequenceNumber", func(t *testing.T) {
		id := uuid.New()
		publicID := "aud_" + cleanHex(uuid.New())[:20]
		query := `INSERT INTO audit_trail_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			shard_id, sequence_number, actor_id, actor_type, action_name,
			target_type, target_id, outcome, compliance_class,
			prev_record_hash, record_hash, canonical_payload, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`

		_, err := db.ExecContext(ctx, query,
			id, publicID, tenantID, "audit-service", "ext_03", uuid.New(),
			"shard_1", 0, "usr_1", "USER", "orders.order.cancelled",
			"order", "ord_1", "SUCCEEDED", "CLASS_A",
			rules.GenesisHash, rules.GenesisHash, "{}", time.Now(),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_audit_trail_sequence_pos")
	})

	t.Run("RejectMutationAttemptViaUpdateTrigger", func(t *testing.T) {
		id := uuid.New()
		publicID := "aud_" + cleanHex(uuid.New())[:20]
		insertQuery := `INSERT INTO audit_trail_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			shard_id, sequence_number, actor_id, actor_type, action_name,
			target_type, target_id, outcome, compliance_class,
			prev_record_hash, record_hash, canonical_payload, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`

		_, err := db.ExecContext(ctx, insertQuery,
			id, publicID, tenantID, "audit-service", "ext_mut", uuid.New(),
			"shard_1", 1, "usr_1", "USER", "orders.order.cancelled",
			"order", "ord_1", "SUCCEEDED", "CLASS_A",
			rules.GenesisHash, rules.GenesisHash, "{}", time.Now(),
		)
		require.NoError(t, err)

		updateQuery := `UPDATE audit_trail_ledger SET outcome = 'FAILED' WHERE id = $1 AND tenant_id = $2`
		_, errUpdate := db.ExecContext(ctx, updateQuery, id, tenantID)
		require.Error(t, errUpdate)
		assert.Contains(t, errUpdate.Error(), "Audit records are strictly immutable")
	})

	t.Run("RejectMalformedCheckpointPublicID", func(t *testing.T) {
		id := uuid.New()
		query := `INSERT INTO audit_trail_checkpoint_ledger (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			shard_id, checkpoint_seq, start_sequence, end_sequence, record_count,
			merkle_root_hash, head_record_hash, anchor_signature, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

		_, err := db.ExecContext(ctx, query,
			id, "malformed_ack_id", tenantID, "audit-service", "ext_04", uuid.New(),
			"shard_1", 1, 1, 10, 10,
			rules.GenesisHash, rules.GenesisHash, strings.Repeat("d", 64), "ANCHORED",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ck_audit_trail_checkpoint_public_id_format")
	})
}

func TestAuditTrail_PostgresLifecycleAndHashChainIntegrity(t *testing.T) {
	db := getAuditTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresAuditTrailRepositoryAdapter(db)
	svc := service.NewAuditTrailService(repo)

	tenantID := "tenant_aud_life_" + cleanHex(uuid.New())[:8]
	shardID := "shard_orders_" + cleanHex(uuid.New())[:6]
	signingSecret := "super_secret_audit_anchor_key_32chars"

	r1, err := svc.EmitAuditRecord(ctx, contracts.EmitAuditInput{
		TenantID:         tenantID,
		SourceService:    "order-service",
		CorrelationID:    uuid.New().String(),
		ShardID:          shardID,
		ActorID:          "usr_alice",
		ActorType:        contracts.ActorTypeUser,
		ActionName:       "orders.order.created",
		TargetType:       "order",
		TargetID:         "ord_1001",
		Outcome:          contracts.OutcomeSucceeded,
		ComplianceClass:  contracts.ClassA,
		CanonicalPayload: `{"order_id":"ord_1001","amount":1500}`,
		OccurredAt:       time.Now().UTC(),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, r1.PublicID)
	assert.Equal(t, int64(1), r1.SequenceNumber)
	assert.Equal(t, rules.GenesisHash, r1.PrevRecordHash)
	assert.NotEmpty(t, r1.RecordHash)

	r2, err := svc.EmitAuditRecord(ctx, contracts.EmitAuditInput{
		TenantID:         tenantID,
		SourceService:    "order-service",
		CorrelationID:    uuid.New().String(),
		ShardID:          shardID,
		ActorID:          "usr_alice",
		ActorType:        contracts.ActorTypeUser,
		ActionName:       "orders.order.paid",
		TargetType:       "order",
		TargetID:         "ord_1001",
		Outcome:          contracts.OutcomeSucceeded,
		ComplianceClass:  contracts.ClassA,
		CanonicalPayload: `{"order_id":"ord_1001","status":"PAID"}`,
		OccurredAt:       time.Now().UTC(),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), r2.SequenceNumber)
	assert.Equal(t, r1.RecordHash, r2.PrevRecordHash)

	verifyOut, err := svc.VerifyChain(ctx, contracts.VerifyChainInput{
		TenantID:      tenantID,
		ShardID:       shardID,
		SigningSecret: signingSecret,
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.IsIntact)
	assert.Equal(t, int64(2), verifyOut.RecordsVerified)
	assert.Equal(t, int64(2), verifyOut.LatestSequence)
	assert.Equal(t, r2.RecordHash, verifyOut.LatestRecordHash)

	cp, err := svc.CreateCheckpoint(ctx, contracts.CreateCheckpointInput{
		TenantID:      tenantID,
		SourceService: "audit-anchor-service",
		CorrelationID: uuid.New().String(),
		ShardID:       shardID,
		SigningSecret: signingSecret,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, cp.PublicID)
	assert.Equal(t, int64(1), cp.CheckpointSeq)
	assert.Equal(t, int64(1), cp.StartSequence)
	assert.Equal(t, int64(2), cp.EndSequence)
	assert.Equal(t, int64(2), cp.RecordCount)
	assert.Equal(t, r2.RecordHash, cp.HeadRecordHash)
	assert.NotEmpty(t, cp.MerkleRootHash)
	assert.NotEmpty(t, cp.AnchorSignature)
}

func TestAuditTrail_TracingCompliance(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	db := getAuditTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresAuditTrailRepositoryAdapter(db)
	svc := service.NewAuditTrailService(repo)

	tenantID := "tenant_aud_trace_" + cleanHex(uuid.New())[:8]
	shardID := "shard_trace_" + cleanHex(uuid.New())[:6]

	_, err := svc.EmitAuditRecord(ctx, contracts.EmitAuditInput{
		TenantID:         tenantID,
		SourceService:    "trace-suite",
		CorrelationID:    uuid.New().String(),
		ShardID:          shardID,
		ActorID:          "usr_trace",
		ActorType:        contracts.ActorTypeUser,
		ActionName:       "trace.event.captured",
		TargetType:       "trace",
		TargetID:         "trc_01",
		Outcome:          contracts.OutcomeSucceeded,
		ComplianceClass:  contracts.ClassB,
		CanonicalPayload: `{"trace":"ok"}`,
		OccurredAt:       time.Now().UTC(),
	})
	require.NoError(t, err)

	spans := exporter.GetSpans()
	var hasServiceSpan, hasRepoSpan bool
	for _, span := range spans {
		if span.Name == "AuditTrailService.EmitAuditRecord" {
			hasServiceSpan = true
		}
		if span.Name == "PostgresAuditTrailRepositoryAdapter.InsertAuditRecord" ||
			span.Name == "PostgresAuditTrailRepositoryAdapter.GetLatestAuditRecord" {
			hasRepoSpan = true
		}
	}

	assert.True(t, hasServiceSpan, "expected AuditTrailService.EmitAuditRecord span in trace exporter")
	assert.True(t, hasRepoSpan, "expected PostgresAuditTrailRepositoryAdapter span in trace exporter")
}

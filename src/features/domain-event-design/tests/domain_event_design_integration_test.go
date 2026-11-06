package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-driven-platform/src/features/domain-event-design/contracts"
	"event-driven-platform/src/features/domain-event-design/infrastructure/adapters"
	"event-driven-platform/src/features/domain-event-design/service"
)

func getTestDB(t *testing.T) *sql.DB {
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

func TestDomainEvent_PostgresLifecycle(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresDomainEventAdapter(db)
	svc := service.NewDomainEventService(repo)

	tenantID := "tenant_ded_test_" + uuid.New().String()[:8]
	eventType := "billing.invoice.settled.v1"

	regCmd := contracts.RegisterCatalogEventCommand{
		TenantID:          tenantID,
		SourceService:     "billing-service",
		ExternalID:        "ext_reg_1",
		CorrelationID:     uuid.New().String(),
		EventType:         eventType,
		OwningAggregate:   "billing.invoice",
		SemanticsClass:    contracts.SemanticsClassState,
		Visibility:        contracts.EventVisibilityPrivate,
		PIIClassification: contracts.PIIClassNone,
		SchemaFingerprint: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}

	catalogEntry, err := svc.RegisterCatalogEvent(ctx, regCmd)
	require.NoError(t, err)
	assert.Equal(t, eventType, catalogEntry.EventType)
	assert.Equal(t, tenantID, catalogEntry.TenantID)

	buildCmd := contracts.BuildEnvelopeCommand{
		TenantID:           tenantID,
		SourceService:      "billing-service",
		EventType:          eventType,
		AggregateType:      "billing.invoice",
		AggregateID:        "inv_9999",
		ExpectedVersion:    0,
		VersionIncrement:   1,
		EffectiveAt:        time.Now().UTC(),
		CausationID:        "cmd_settle_invoice",
		CorrelationID:      uuid.New().String(),
		CausationDepth:     1,
		TransactionGroupID: "txg_invoice_9999",
		GroupSize:          1,
		Payload:            map[string]any{"amount": 500, "currency": "USD"},
	}

	buildRes, err := svc.BuildCanonicalEnvelope(ctx, buildCmd)
	require.NoError(t, err)
	assert.Equal(t, int64(1), buildRes.Envelope.AggregateVersion)
	assert.Equal(t, contracts.SemanticsClassState, buildRes.Envelope.SemanticsClass)

	conflictCmd := contracts.ReserveAggregateVersionCommand{
		TenantID:         tenantID,
		SourceService:    "billing-service",
		ExternalID:       "inv_9999",
		CorrelationID:    uuid.New().String(),
		AggregateType:    "billing.invoice",
		AggregateID:      "inv_9999",
		ExpectedVersion:  0,
		VersionIncrement: 1,
	}
	_, err = svc.ReserveAggregateVersion(ctx, conflictCmd)
	assert.Error(t, err)
}


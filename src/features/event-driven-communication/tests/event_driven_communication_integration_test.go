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

	"event-driven-platform/src/features/event-driven-communication/contracts"
	"event-driven-platform/src/features/event-driven-communication/infrastructure/adapters"
	"event-driven-platform/src/features/event-driven-communication/service"
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

func TestEventCommunication_PostgresLifecycle(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresEventCommunicationAdapter(db)
	svc := service.NewEventCommunicationService(repo)

	tenantID := "tenant_edc_test_" + uuid.New().String()[:8]
	sagaName := "checkout_flow_" + uuid.New().String()[:8]

	def := contracts.SagaDefinition{
		TenantID:      tenantID,
		SourceService: "checkout-service",
		ExternalID:    "ext_def_" + uuid.New().String()[:8],
		CorrelationID: uuid.New(),
		SagaName:      sagaName,
		Steps: []contracts.SagaStepDefinition{
			{
				StepIndex:        0,
				StepName:         "AuthorizePayment",
				StepType:         contracts.StepTypeCompensable,
				CompensationName: "VoidAuthorization",
				TimeoutSeconds:   15,
			},
			{
				StepIndex:      1,
				StepName:       "CapturePayment",
				StepType:       contracts.StepTypePivot,
				TimeoutSeconds: 15,
			},
		},
	}

	regDef, err := svc.RegisterSagaDefinition(ctx, def)
	require.NoError(t, err)
	assert.NotEmpty(t, regDef.PublicID)

	startRes, err := svc.StartSaga(ctx, contracts.StartSagaCommand{
		TenantID:         tenantID,
		SourceService:    "checkout-service",
		ExternalID:       "order_1001",
		CorrelationID:    uuid.New().String(),
		SagaDefinitionID: regDef.PublicID,
		InitialPayload:   `{"amount": 100}`,
		TimeoutSeconds:   30,
	})
	require.NoError(t, err)
	assert.Equal(t, contracts.SagaStateInProgress, startRes.State)

	stepRes, err := svc.ProcessStepReply(ctx, contracts.ProcessStepReplyCommand{
		TenantID:       tenantID,
		SourceService:  "payment-service",
		SagaInstanceID: startRes.SagaInstanceID,
		StepIndex:      0,
		CommandID:      "cmd_pay_auth_1001",
		Outcome:        contracts.OutcomeSucceeded,
		ReplyPayload:   `{"auth_code":"OK123"}`,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stepRes.NextStepIndex)
	assert.Equal(t, contracts.SagaStateInProgress, stepRes.SagaState)
}

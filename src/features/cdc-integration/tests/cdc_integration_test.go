package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/domain/machines"
	"event-driven-platform/src/features/cdc-integration/infrastructure/adapters"
	"event-driven-platform/src/features/cdc-integration/service"
	"event-driven-platform/src/shared/utils"
)

func getCDCTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("skipping live database test: %v", err)
		return nil
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Skipf("skipping live database test: postgres unreachable: %v", pingErr)
		return nil
	}
	return db
}

func TestCDCIntegration_PostgresLifecycle(t *testing.T) {
	db := getCDCTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresCDCRepositoryAdapter(db)
	connect := adapters.NewMockKafkaConnectAdapter()
	machine := machines.NewCDCConnectorMachine()
	svc := service.NewCDCService(repo, connect, machine)

	uID, _ := utils.GenerateUUIDv7()
	suffix := strings.ReplaceAll(uID, "-", "")[:8]
	connName := fmt.Sprintf("debezium_orders_%s", suffix)
	slotName := fmt.Sprintf("cdc_slot_orders_%s", suffix)
	pubName := fmt.Sprintf("cdc_pub_orders_%s", suffix)
	extID := fmt.Sprintf("ext-cdc-%s", suffix)

	t.Run("RegisterConnector", func(t *testing.T) {
		input := contracts.RegisterConnectorInput{
			TenantID:        "tenant_platform",
			SourceService:   "order-service",
			ExternalID:      extID,
			CorrelationID:   uID,
			ConnectorName:   connName,
			SlotName:        slotName,
			PublicationName: pubName,
			Mode:            contracts.ModeOutbox,
		}

		rec, err := svc.RegisterConnector(ctx, input)
		if err != nil {
			t.Fatalf("failed registering connector in postgres: %v", err)
		}
		if rec.ConnectorName != connName {
			t.Fatalf("expected connector %s, got %s", connName, rec.ConnectorName)
		}

		fetched, fErr := svc.GetConnector(ctx, connName)
		if fErr != nil {
			t.Fatalf("failed fetching connector: %v", fErr)
		}
		if fetched.SlotName != slotName {
			t.Fatalf("expected slot %s, got %s", slotName, fetched.SlotName)
		}
	})

	t.Run("PauseAndResumeConnector", func(t *testing.T) {
		err := svc.PauseConnector(ctx, connName)
		if err != nil {
			t.Fatalf("failed pausing connector: %v", err)
		}

		rec, _ := svc.GetConnector(ctx, connName)
		if rec.State != contracts.StatePaused {
			t.Fatalf("expected state PAUSED, got: %s", rec.State)
		}

		err = svc.ResumeConnector(ctx, connName)
		if err != nil {
			t.Fatalf("failed resuming connector: %v", err)
		}

		rec, _ = svc.GetConnector(ctx, connName)
		if rec.State != contracts.StateRunning {
			t.Fatalf("expected state RUNNING, got: %s", rec.State)
		}
	})

	t.Run("EmitHeartbeat", func(t *testing.T) {
		hbInput := contracts.HeartbeatTickInput{
			TenantID:      "tenant_platform",
			SourceService: "heartbeat-daemon",
			ExternalID:    "hb-ext-01",
			CorrelationID: uID,
			ConnectorName: connName,
			LSNPosition:   "0/16B2D40",
		}
		err := svc.EmitHeartbeat(ctx, hbInput)
		if err != nil {
			t.Fatalf("failed recording heartbeat: %v", err)
		}
	})

	t.Run("RecordSignal", func(t *testing.T) {
		sigInput := contracts.DispatchSignalInput{
			TenantID:      "tenant_platform",
			SourceService: "admin-api",
			ExternalID:    "sig-001",
			CorrelationID: uID,
			ConnectorName: connName,
			SignalType:    contracts.SignalCheckpoint,
			Payload:       map[string]interface{}{"marker": "checkpoint_1"},
		}
		sig, err := svc.DispatchSignal(ctx, sigInput)
		if err != nil {
			t.Fatalf("failed dispatching signal: %v", err)
		}
		if sig.ConnectorName != connName {
			t.Fatalf("expected connector %s on signal, got: %s", connName, sig.ConnectorName)
		}
	})
}

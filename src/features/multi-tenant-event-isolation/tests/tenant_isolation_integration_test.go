package tests

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/machines"
	"event-driven-platform/src/features/multi-tenant-event-isolation/infrastructure/adapters"
	"event-driven-platform/src/features/multi-tenant-event-isolation/service"
	"event-driven-platform/src/shared/utils"
)

func getTenantTestDB(t *testing.T) *sql.DB {
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

func TestTenantCryptoShredding_Integration(t *testing.T) {
	crypto := adapters.NewInMemoryCryptoAdapter()
	ctx := context.Background()

	tenantID := "tenant_fintech_01"
	subjectID := "user_sub_9988"
	periodKey := "2026-Q4"

	dekRecord := contracts.DEKRecord{
		ID:            "dek-uuid-001",
		PublicID:      "dek_000000000000000001",
		TenantID:      tenantID,
		SourceService: "user-service",
		ExternalID:    "ext-sub-9988",
		CorrelationID: "corr-uuid-001",
		SubjectID:     subjectID,
		PeriodKey:     periodKey,
		WrappedDEK:    []byte("super-secret-wrapped-dek-32-byte!"),
		KEKKeyID:      "kms-arn-tenant-fintech",
		Algorithm:     "AES_256_GCM",
		Status:        contracts.KeyStatusActive,
		Version:       1,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	if err := crypto.StoreDEK(ctx, dekRecord); err != nil {
		t.Fatalf("failed storing DEK: %v", err)
	}

	sensitivePayload := []byte(`{"pan":"4111222233334444","ssn":"123-45-6789"}`)
	ciphertext, encErr := crypto.EncryptField(ctx, tenantID, subjectID, periodKey, sensitivePayload)
	if encErr != nil {
		t.Fatalf("failed encrypting sensitive payload: %v", encErr)
	}

	decrypted, decErr := crypto.DecryptField(ctx, tenantID, subjectID, periodKey, ciphertext)
	if decErr != nil {
		t.Fatalf("failed decrypting ciphertext with active DEK: %v", decErr)
	}
	if !bytes.Equal(decrypted, sensitivePayload) {
		t.Fatalf("expected decrypted plaintext to match original, got: %s", string(decrypted))
	}

	if shredErr := crypto.ShredDEK(ctx, tenantID, subjectID, periodKey); shredErr != nil {
		t.Fatalf("failed shredding DEK: %v", shredErr)
	}

	afterShred, postShredErr := crypto.DecryptField(ctx, tenantID, subjectID, periodKey, ciphertext)
	if postShredErr != nil {
		t.Fatalf("expected DecryptField to return ERASED marker without error, got: %v", postShredErr)
	}
	if !bytes.Equal(afterShred, adapters.ErasedMarker) {
		t.Fatalf("expected %s marker after shredding, got: %s", string(adapters.ErasedMarker), string(afterShred))
	}
}

func TestTenantDatabaseConstraints_Integration(t *testing.T) {
	db := getTenantTestDB(t)
	if db == nil {
		return
	}
	defer db.Close()

	ctx := context.Background()
	repo := adapters.NewPostgresTenantRepositoryAdapter(db)
	crypto := adapters.NewInMemoryCryptoAdapter()
	machine := machines.NewTenantLifecycleMachine()
	svc := service.NewMultiTenantService(repo, crypto, machine)

	uniqueID, _ := utils.GenerateUUIDv7()
	tenantID := fmt.Sprintf("tenant_test_%s", strings.ReplaceAll(uniqueID, "-", "")[:12])
	corrID, _ := utils.GenerateUUIDv7()

	t.Run("RegisterAndQueryTenant", func(t *testing.T) {
		input := contracts.RegisterTenantInput{
			TenantID:            tenantID,
			SourceService:       "test-runner",
			ExternalID:          "ext-001",
			CorrelationID:       corrID,
			Tier:                contracts.TierPool,
			HomeRegion:          "us-east-1",
			AllowedRegions:      []string{"us-east-1", "us-west-2"},
			RetentionClass:      "FINANCIAL_7_YEARS",
			MaxThroughputEPS:    2500,
			MaxConcurrencyLanes: 20,
		}

		rec, err := svc.RegisterTenant(ctx, input)
		if err != nil {
			t.Fatalf("failed registering tenant in postgres: %v", err)
		}
		if rec.TenantID != tenantID {
			t.Fatalf("expected tenantID %s, got %s", tenantID, rec.TenantID)
		}

		queried, qErr := svc.GetTenant(ctx, tenantID)
		if qErr != nil {
			t.Fatalf("failed querying tenant from postgres: %v", qErr)
		}
		if queried.PublicID != rec.PublicID {
			t.Fatalf("expected publicID %s, got %s", rec.PublicID, queried.PublicID)
		}
	})

	t.Run("VerifyIngressEnvelopeAllowed", func(t *testing.T) {
		env := contracts.VerifyEnvelopeInput{
			HeaderTenant:  tenantID,
			KeyPrefix:     tenantID,
			PayloadTenant: tenantID,
			CurrentRegion: "us-east-1",
		}
		if err := svc.VerifyIngressEnvelope(ctx, env); err != nil {
			t.Fatalf("expected envelope verification to pass, got: %v", err)
		}
	})

	t.Run("VerifyIngressEnvelopeResidencyRejection", func(t *testing.T) {
		env := contracts.VerifyEnvelopeInput{
			HeaderTenant:  tenantID,
			KeyPrefix:     tenantID,
			PayloadTenant: tenantID,
			CurrentRegion: "ap-southeast-1",
		}
		err := svc.VerifyIngressEnvelope(ctx, env)
		if err == nil {
			t.Fatalf("expected residency rejection for ap-southeast-1, got nil")
		}
	})

	t.Run("LifecycleTransitionActiveToSuspended", func(t *testing.T) {
		updated, err := svc.TransitionLifecycle(ctx, tenantID, contracts.UpdateLifecycleInput{
			TargetState: contracts.LifecycleSuspended,
			Reason:      "non-payment risk review",
		})
		if err != nil {
			t.Fatalf("failed transitioning lifecycle to SUSPENDED: %v", err)
		}
		if updated.LifecycleState != contracts.LifecycleSuspended {
			t.Fatalf("expected SUSPENDED state, got %s", updated.LifecycleState)
		}
		if updated.Version != 2 {
			t.Fatalf("expected version increment to 2, got %d", updated.Version)
		}
	})
}


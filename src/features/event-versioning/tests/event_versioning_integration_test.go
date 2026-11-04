package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/infrastructure/adapters"
	"event-driven-platform/src/features/event-versioning/service"
	"event-driven-platform/src/shared/utils"
)

func getVersioningIntegrationDB(t *testing.T) *sql.DB {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}
	pass := os.Getenv("DB_PASSWORD")
	if pass == "" {
		pass = "postgres"
	}
	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "event_store"
	}

	dsn := fmt.Sprintf("postgres:
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("Skipping integration test: cannot connect to real PostgreSQL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Skipping integration test: PostgreSQL unreachable: %v", err)
	}

	return db
}


func TestEventVersioning_Integration_PhysicalCheckConstraints(t *testing.T) {
	db := getVersioningIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	testUUID, _ := utils.GenerateUUIDv7()

	t.Run("RejectMalformedPublicID", func(t *testing.T) {
		
		_, err := db.ExecContext(ctx, `
			INSERT INTO event_schema_version (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				stream_name, event_type, schema_version, schema_id, format,
				compatibility_mode, schema_definition, payload_hash
			) VALUES (
				$1, 'bad_prefix_123', 'tenant_ck', 'srv', 'ext_01', $1,
				'orders.v1', 'OrderPlaced', 1, 'sch_1', 'JSON_SCHEMA',
				'FULL_TRANSITIVE', '{"type":"object"}'::jsonb,
				'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for malformed public_id, but insert succeeded")
		}
	})

	t.Run("RejectInvalidFormatEnum", func(t *testing.T) {
		
		_, err := db.ExecContext(ctx, `
			INSERT INTO event_schema_version (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				stream_name, event_type, schema_version, schema_id, format,
				compatibility_mode, schema_definition, payload_hash
			) VALUES (
				$1, 'sch_0192e2b08c2979a0971084519f', 'tenant_ck', 'srv', 'ext_02', $1,
				'orders.v1', 'OrderPlaced', 1, 'sch_2', 'XML_SCHEMA',
				'FULL_TRANSITIVE', '{"type":"object"}'::jsonb,
				'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for format enum, but insert succeeded")
		}
	})

	t.Run("RejectUpcasterNonSequentialStep", func(t *testing.T) {
		
		_, err := db.ExecContext(ctx, `
			INSERT INTO event_upcaster_registry (
				id, public_id, tenant_id, source_service, external_id, correlation_id,
				event_type, from_version, to_version, transformer_name
			) VALUES (
				$1, 'upc_0192e2b08c2979a0971084519f', 'tenant_ck', 'srv', 'ext_03', $1,
				'OrderPlaced', 1, 3, 'transform_v1_to_v3'
			);`, testUUID)

		if err == nil {
			t.Fatalf("expected physical check constraint violation for non-sequential upcaster step (to_version != from_version + 1), but insert succeeded")
		}
	})
}


func TestEventVersioning_Integration_ImmutabilityTriggers(t *testing.T) {
	db := getVersioningIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	testUUID, _ := utils.GenerateUUIDv7()
	publicID := "sch_0192e2b08c2979a0971084519a"
	tenantID := fmt.Sprintf("tenant_imm_%d", time.Now().UnixNano())

	
	_, err := db.ExecContext(ctx, `
		INSERT INTO event_schema_version (
			id, public_id, tenant_id, source_service, external_id, correlation_id,
			stream_name, event_type, schema_version, schema_id, format,
			compatibility_mode, schema_definition, payload_hash
		) VALUES (
			$1, $2, $3, 'srv', 'ext_imm', $1,
			'orders.v1', 'OrderPlaced', 1, 'sch_imm_1', 'JSON_SCHEMA',
			'FULL_TRANSITIVE', '{"type":"object"}'::jsonb,
			'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
		);`, testUUID, publicID, tenantID)
	if err != nil {
		t.Fatalf("failed to insert baseline schema record: %v", err)
	}

	
	_, err = db.ExecContext(ctx, `
		UPDATE event_schema_version
		SET public_id = 'sch_0192e2b08c2979a0971084519b'
		WHERE id = $1 AND tenant_id = $2;
	`, testUUID, tenantID)

	if err == nil {
		t.Fatalf("expected immutability trigger exception on updating public_id, but update succeeded")
	}
}


func TestEventVersioning_Integration_PostgresAdapterUpcasterChain(t *testing.T) {
	db := getVersioningIntegrationDB(t)
	defer db.Close()
	ctx := context.Background()

	repo := adapters.NewPostgresEventVersioningAdapter(db)
	svc := service.NewEventVersioningService(repo, nil)

	tenantID := fmt.Sprintf("tenant_chain_%d", time.Now().UnixNano())
	eventType := "CustomerRegistered"

	
	svc.RegisterTransformer("cust_v1_to_v2", func(p map[string]any) (map[string]any, error) {
		res := make(map[string]any)
		for k, v := range p {
			res[k] = v
		}
		if name, ok := res["name"].(string); ok {
			res["full_name"] = name
			delete(res, "name")
		}
		return res, nil
	})

	svc.RegisterTransformer("cust_v2_to_v3", func(p map[string]any) (map[string]any, error) {
		res := make(map[string]any)
		for k, v := range p {
			res[k] = v
		}
		res["tier"] = "SILVER"
		return res, nil
	})

	
	schemaV1 := json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}}}`)
	v1Input := contracts.RegisterSchemaVersionInput{
		TenantID:          tenantID,
		SourceService:     "customer-service",
		ExternalID:        fmt.Sprintf("ext_v1_%d", time.Now().UnixNano()),
		StreamName:        "customers.v1",
		EventType:         eventType,
		SchemaVersion:     1,
		SchemaID:          fmt.Sprintf("sch_cust_v1_%d", time.Now().UnixNano()),
		Format:            contracts.FormatJSONSchema,
		CompatibilityMode: contracts.CompatBackwardTransitive,
		SchemaDefinition:  schemaV1,
	}
	s1, err := svc.RegisterSchemaVersion(ctx, v1Input)
	if err != nil {
		t.Fatalf("failed to register schema v1 in real Postgres: %v", err)
	}
	if s1 == nil || s1.SchemaVersion != 1 {
		t.Fatalf("invalid schema v1 returned: %+v", s1)
	}

	
	v1DupInput := v1Input
	v1DupInput.ExternalID = fmt.Sprintf("ext_v1_dup_%d", time.Now().UnixNano())
	v1DupInput.SchemaID = fmt.Sprintf("sch_cust_dup_%d", time.Now().UnixNano())
	_, err = svc.RegisterSchemaVersion(ctx, v1DupInput)
	if err == nil {
		t.Fatalf("expected unique constraint violation on duplicate (stream_name, event_type, schema_version), but succeeded")
	}

	
	upc1Input := contracts.RegisterUpcasterInput{
		TenantID:        tenantID,
		SourceService:   "customer-service",
		ExternalID:      fmt.Sprintf("ext_upc1_%d", time.Now().UnixNano()),
		EventType:       eventType,
		FromVersion:     1,
		ToVersion:       2,
		TransformerName: "cust_v1_to_v2",
	}
	u1, err := svc.RegisterUpcaster(ctx, upc1Input)
	if err != nil {
		t.Fatalf("failed to register upcaster v1->v2 in real Postgres: %v", err)
	}
	if u1 == nil || u1.ToVersion != 2 {
		t.Fatalf("invalid upcaster returned: %+v", u1)
	}

	
	upc2Input := contracts.RegisterUpcasterInput{
		TenantID:        tenantID,
		SourceService:   "customer-service",
		ExternalID:      fmt.Sprintf("ext_upc2_%d", time.Now().UnixNano()),
		EventType:       eventType,
		FromVersion:     2,
		ToVersion:       3,
		TransformerName: "cust_v2_to_v3",
	}
	u2, err := svc.RegisterUpcaster(ctx, upc2Input)
	if err != nil {
		t.Fatalf("failed to register upcaster v2->v3 in real Postgres: %v", err)
	}
	if u2 == nil || u2.ToVersion != 3 {
		t.Fatalf("invalid upcaster returned: %+v", u2)
	}

	
	initialPayload := json.RawMessage(`{"name":"Alice Wonderland"}`)
	upcastResult, err := svc.UpcastEvent(ctx, contracts.UpcastStepInput{
		TenantID:      tenantID,
		EventType:     eventType,
		SourceVersion: 1,
		TargetVersion: 3,
		Payload:       initialPayload,
	})
	if err != nil {
		t.Fatalf("failed to upcast event through real Postgres upcaster chain: %v", err)
	}

	if upcastResult.FinalVersion != 3 {
		t.Fatalf("expected finalVersion 3, got %d", upcastResult.FinalVersion)
	}
	if upcastResult.StepsApplied != 2 {
		t.Fatalf("expected 2 applied upcasters, got %d", upcastResult.StepsApplied)
	}

	var parsed map[string]any
	if err := json.Unmarshal(upcastResult.Payload, &parsed); err != nil {
		t.Fatalf("failed to parse upcasted payload: %v", err)
	}

	if parsed["full_name"] != "Alice Wonderland" {
		t.Fatalf("expected full_name 'Alice Wonderland', got %v", parsed["full_name"])
	}
	if parsed["tier"] != "SILVER" {
		t.Fatalf("expected tier 'SILVER', got %v", parsed["tier"])
	}
	if _, exists := parsed["name"]; exists {
		t.Fatalf("expected 'name' key to be deleted by v1->v2 transform, but found it")
	}
}

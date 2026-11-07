package tests

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"event-driven-platform/messaging/engine/producers"
	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	"event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
)

func headerTestInput(externalID string, version int64) contracts.CreateOutboxRecordInput {
	corrID, _ := uuid.NewV7()
	return contracts.CreateOutboxRecordInput{
		TenantID:         "tenant_corp_01",
		SourceService:    "orders-service",
		ExternalID:       externalID,
		CorrelationID:    corrID.String(),
		CausationID:      "cmd_h",
		AggregateType:    "order",
		AggregateID:      "ord_h",
		AggregateVersion: version,
		EventType:        "order.created",
		SchemaVersion:    "1.0.0",
		Payload:          []byte(`{"ok":true}`),
		Headers:          map[string]string{"x-custom": "abc"},
	}
}

func TestOutboxDuplicateExternalIDIsConflict(t *testing.T) {
	ctx := context.Background()
	svc := service.NewOutboxService(adapters.NewMemoryOutboxRepositoryAdapter(), producers.NewMemoryProducerAdapter())

	if _, err := svc.StageEvent(ctx, headerTestInput("ext_dup", 1), ""); err != nil {
		t.Fatalf("first stage: %v", err)
	}
	_, err := svc.StageEvent(ctx, headerTestInput("ext_dup", 2), "")
	var appErr *appErrors.ApplicationError
	if !errors.As(err, &appErr) || appErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 ApplicationError, got %v", err)
	}
}

func TestOutboxRelayCarriesHeaders(t *testing.T) {
	ctx := context.Background()
	producer := producers.NewMemoryProducerAdapter()
	svc := service.NewOutboxService(adapters.NewMemoryOutboxRepositoryAdapter(), producer)

	if _, err := svc.StageEvent(ctx, headerTestInput("ext_hdr", 1), ""); err != nil {
		t.Fatalf("stage: %v", err)
	}
	n, err := svc.RelayBatch(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}
	h := producer.Messages[0].Headers
	if h["x-custom"] != "abc" || h[constants.HeaderEventType] != "order.created" {
		t.Fatalf("headers not propagated: %v", h)
	}
	if len(h) != 4 {
		t.Fatalf("expected 4 headers (custom + event_type + schema_version + source_service), got %v", h)
	}
}

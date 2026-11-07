package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	consumerContracts "event-driven-platform/src/features/consumer-groups/contracts"
	cqrsfeature "event-driven-platform/src/features/cqrs"
	cqrsContracts "event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/shared/types"
)

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) types.ApiResponse {
	t.Helper()
	var resp types.ApiResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode envelope: %v (%s)", err, rec.Body.String())
	}
	return resp
}

func assertBadRequest(t *testing.T, rec *httptest.ResponseRecorder, wantMsg string) {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	resp := decodeEnvelope(t, rec)
	if resp.Success {
		t.Fatalf("success must be false on 400")
	}
	if resp.Error == nil || !strings.Contains(resp.Error.Message, wantMsg) {
		t.Fatalf("want message containing %q, got %+v", wantMsg, resp.Error)
	}
}

// Missing tenant scope must be rejected before any service call, so nil services are safe here.
func TestMissingTenantScopeReturns400(t *testing.T) {
	cases := []struct {
		name string
		path string
		call func(w http.ResponseWriter, r *http.Request)
	}{
		{"retries due", "/api/v1/retries/due", (&RetryTopicsHandler{}).GetDueRetries},
		{"events held", "/api/v1/events/held", (&EventOrderingHandler{}).ListHeld},
		{"watermark", "/api/v1/consistency/views/v/watermark", (&ConsistencyMonitoringHandler{}).HandleGetViewWatermarks},
		{"incidents", "/api/v1/consistency/views/v/incidents", (&ConsistencyMonitoringHandler{}).HandleGetActiveIncidents},
		{"sweeps", "/api/v1/consistency/views/v/sweeps", (&ConsistencyMonitoringHandler{}).HandleListCheckRuns},
		{"queries resolve", "/api/v1/queries/resolve", (&CQRSHandler{}).HandleResolveQuery},
		{"queries authoritative", "/api/v1/queries/authoritative-own", (&CQRSHandler{}).HandleAuthoritativeRead},
		{"audits query", "/api/v1/audits/query?shard_id=s1", (&AuditTrailHandler{}).HandleQueryAudits},
		{"dlq list", "/api/v1/dlq", (&DeadLetterHandler{}).List},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c.call(rec, httptest.NewRequest(http.MethodGet, c.path, nil))
			assertBadRequest(t, rec, "tenant")
		})
	}
}

func TestDLQRedriveDiscardRejectNonUUID(t *testing.T) {
	h := &DeadLetterHandler{}
	cases := map[string]struct {
		fn   func(http.ResponseWriter, *http.Request)
		body string
	}{
		"redrive": {h.Redrive, `{"tenantId":"t1","quarantineId":"not-a-uuid"}`},
		"discard": {h.Discard, `{"tenantId":"t1","quarantineId":"not-a-uuid","reason":"x"}`},
	}
	for name, c := range cases {
		fn, body := c.fn, c.body
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			fn(rec, httptest.NewRequest(http.MethodPost, "/api/v1/dlq/"+name, strings.NewReader(body)))
			assertBadRequest(t, rec, "quarantineId must be a valid UUID")
			if strings.Contains(rec.Body.String(), "pq:") {
				t.Fatal("driver text leaked")
			}
		})
	}
}

func TestConsumerGroupsWriteServiceError(t *testing.T) {
	h := &ConsumerGroupsHandler{}

	rec := httptest.NewRecorder()
	h.writeServiceError(rec, errors.New(`pq: duplicate key value violates unique constraint "uk_x"`), time.Now(), "", "", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unknown error: want 500, got %d", rec.Code)
	}
	if resp := decodeEnvelope(t, rec); resp.Success || resp.Error == nil || resp.Error.Message != "internal error" {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.writeServiceError(rec, consumerContracts.ErrInvalidTenantID, time.Now(), "", "", "")
	assertBadRequest(t, rec, "tenantId is required")

	rec = httptest.NewRecorder()
	h.writeServiceError(rec, errors.Join(errors.New("ctx"), consumerContracts.ErrBannedAssignor), time.Now(), "", "", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("wrapped domain error: want 400, got %d", rec.Code)
	}
}

type memCQRSRepo struct{ key *cqrsContracts.TokenSigningKeyRecord }

func (m *memCQRSRepo) GetIdempotencyRecord(context.Context, string, string) (*cqrsContracts.CommandRecord, error) {
	return nil, nil
}
func (m *memCQRSRepo) CreateIdempotencyRecord(context.Context, cqrsContracts.CommandRecord) error {
	return nil
}
func (m *memCQRSRepo) UpdateIdempotencyRecord(context.Context, uuid.UUID, string, cqrsContracts.CommandStatus, string, string) error {
	return nil
}
func (m *memCQRSRepo) GetAggregateForUpdate(context.Context, string, string, string) (*cqrsContracts.AggregateRecord, error) {
	return nil, nil
}
func (m *memCQRSRepo) UpsertAggregateMutation(context.Context, cqrsContracts.AggregateRecord) error {
	return nil
}
func (m *memCQRSRepo) GetActiveSigningKey(context.Context, string) (*cqrsContracts.TokenSigningKeyRecord, error) {
	return m.key, nil
}
func (m *memCQRSRepo) RegisterSigningKey(_ context.Context, r cqrsContracts.TokenSigningKeyRecord) error {
	m.key = &r
	return nil
}

func TestCQRSNoSigningKeyAndKeyResponse(t *testing.T) {
	repo := &memCQRSRepo{}
	h := NewCQRSHandler(cqrsfeature.NewCQRSService(repo))

	resolve := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/queries/resolve?tenant_id=t1&view_name=v&aggregate_type=A&aggregate_id=1", nil)
		if token != "" {
			req.Header.Set("x-consistency-token", token)
		}
		rec := httptest.NewRecorder()
		h.HandleResolveQuery(rec, req)
		return rec
	}

	if rec := resolve(""); rec.Code != http.StatusOK {
		t.Fatalf("no token needs no key: want 200, got %d %s", rec.Code, rec.Body.String())
	}
	rec := resolve("garbage")
	if rec.Code != http.StatusConflict {
		t.Fatalf("token without key: want 409, got %d %s", rec.Code, rec.Body.String())
	}
	if resp := decodeEnvelope(t, rec); resp.Success || !strings.Contains(resp.Error.Message, "no active signing key") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}

	body := `{"tenant_id":"t1","source_service":"svc","key_version":1,"raw_secret_value":"0123456789abcdef0123","expires_at":"` +
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}`
	rec = httptest.NewRecorder()
	h.HandleRegisterSigningKey(rec, httptest.NewRequest(http.MethodPost, "/api/v1/commands/keys", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("register key: want 201, got %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret_key_hash") || strings.Contains(rec.Body.String(), "0123456789abcdef0123") {
		t.Fatalf("secret material echoed: %s", rec.Body.String())
	}

	// Key exists now: a malformed token is a client error (400), not a missing-key conflict.
	if rec := resolve("garbage"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad token with key: want 400, got %d %s", rec.Code, rec.Body.String())
	}
}

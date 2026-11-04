package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
)

func TestTraceAndContextMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := TenantFromContext(r.Context())
		if tenant != "tenant_abc" {
			t.Errorf("expected tenant_abc, got %s", tenant)
		}
		reqID := RequestIDFromContext(r.Context())
		if reqID != "req_123" {
			t.Errorf("expected req_123, got %s", reqID)
		}
		w.WriteHeader(http.StatusOK)
	})

	wrapped := TraceAndContextMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/schemas", nil)
	req.Header.Set(constants.HeaderXTenantID, "tenant_abc")
	req.Header.Set(constants.HeaderXRequestID, "req_123")
	rec := httptest.NewRecorder()

	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	if rec.Header().Get(constants.HeaderXRequestID) != "req_123" {
		t.Errorf("expected echoed request id header")
	}
}

func TestErrorRecoveryMiddleware(t *testing.T) {
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("unexpected state corruption")
	})

	wrapped := ErrorRecoveryMiddleware(TraceAndContextMiddleware(panicHandler))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/schemas", nil)
	rec := httptest.NewRecorder()

	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error on panic, got %d", rec.Code)
	}

	var resp types.ApiResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding error response envelope: %v", err)
	}

	if resp.Success != false {
		t.Errorf("expected success=false")
	}
	if resp.Error.Code != constants.ErrCodeInternalError {
		t.Errorf("expected %s, got %s", constants.ErrCodeInternalError, resp.Error.Code)
	}
}

func TestFormatErrorResponse_ApplicationError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)

	appErr := appErrors.NewValidationError("field is invalid")
	FormatErrorResponse(rec, req, appErr)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
	}

	var resp types.ApiResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding envelope: %v", err)
	}
	if resp.Error.Code != constants.ErrCodeValidationFailed {
		t.Errorf("expected VALIDATION_FAILED, got %s", resp.Error.Code)
	}
}

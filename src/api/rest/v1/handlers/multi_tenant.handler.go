package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	multiTenantFeature "event-driven-platform/src/features/multi-tenant-event-isolation"
	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type MultiTenantHandler struct {
	feat *multiTenantFeature.MultiTenantFeature
}

func NewMultiTenantHandler(feat *multiTenantFeature.MultiTenantFeature) *MultiTenantHandler {
	return &MultiTenantHandler{feat: feat}
}

func (h *MultiTenantHandler) HandleRegisterTenant(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/tenants", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/tenants"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.RegisterTenantInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "tenant-api"
	}
	if input.ExternalID == "" {
		input.ExternalID = input.TenantID
	}

	rec, err := h.feat.Service.RegisterTenant(ctx, input)
	if err != nil {
		status, code := mapTenantError(err)
		h.writeError(w, status, code, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, rec, start, reqID, corrID, causationID)
}

func (h *MultiTenantHandler) HandleGetTenant(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/tenants/{tenant_id}", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	tenantID := r.PathValue("tenant_id")

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/tenants/{tenant_id}"),
		attribute.String("tenant_id", tenantID),
	)

	rec, err := h.feat.Service.GetTenant(ctx, tenantID)
	if err != nil {
		status, code := mapTenantError(err)
		h.writeError(w, status, code, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, rec, start, reqID, corrID, causationID)
}

func (h *MultiTenantHandler) HandleTransitionLifecycle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "PUT /api/v1/tenants/{tenant_id}/lifecycle", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	tenantID := r.PathValue("tenant_id")

	var input contracts.UpdateLifecycleInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	rec, err := h.feat.Service.TransitionLifecycle(ctx, tenantID, input)
	if err != nil {
		status, code := mapTenantError(err)
		h.writeError(w, status, code, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, rec, start, reqID, corrID, causationID)
}

func (h *MultiTenantHandler) HandleVerifyEnvelope(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/tenants/verify-envelope", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.VerifyEnvelopeInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if err := h.feat.Service.VerifyIngressEnvelope(ctx, input); err != nil {
		h.writeError(w, http.StatusForbidden, constants.ErrCodePreconditionFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, map[string]bool{"verified": true}, start, reqID, corrID, causationID)
}

// mapTenantError derives the HTTP status from the typed ApplicationError
// carried by the service/adapters; anything untyped is an internal error.
func mapTenantError(err error) (int, string) {
	var appErr *appErrors.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.StatusCode, appErr.CanonicalCode
	}
	return http.StatusInternalServerError, constants.ErrCodeInternalError
}

func (h *MultiTenantHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
	w.WriteHeader(status)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: status,
		Data:       data,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			CausationID:     causationID,
			Timestamp:       utils.FormatRFC3339UTC(time.Now()),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      constants.APIVersionV1,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *MultiTenantHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
	w.WriteHeader(code)
	resp := types.ApiResponse{
		Success:    false,
		StatusCode: code,
		Data:       nil,
		Error: &types.ResponseError{
			Code:      canonical,
			Message:   msg,
			Retryable: code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable,
		},
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			CausationID:     causationID,
			Timestamp:       utils.FormatRFC3339UTC(time.Now()),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      constants.APIVersionV1,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

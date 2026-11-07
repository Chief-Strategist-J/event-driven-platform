package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type RetryTopicsHandler struct {
	svc *service.RetryTopicsService
}

func NewRetryTopicsHandler(svc *service.RetryTopicsService) *RetryTopicsHandler {
	return &RetryTopicsHandler{svc: svc}
}

func (h *RetryTopicsHandler) EvaluateRetry(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/retries/evaluate", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/retries/evaluate"),
		attribute.String("correlation_id", corrID),
	)

	tenantID := r.Header.Get(constants.HeaderXTenantID)

	var req contracts.EvaluateRetryInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "retry-topics", "malformed retry evaluation request payload", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("stream_ordering", string(req.StreamOrdering)),
		attribute.String("classification", string(req.Classification)),
		attribute.Int("attempt_number", req.AttemptNumber),
		attribute.Int("max_attempts", req.MaxAttempts),
	)

	result, err := h.svc.EvaluateRetry(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "retry-topics", "retry evaluation validation failed", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("retry.disposition", string(result.Disposition)),
		attribute.Int64("retry.backoff_ms", result.BackoffMs),
	)

	observability.LogInfo(ctx, "retry-topics", "retry decision evaluated successfully", tenantID, map[string]any{
		"disposition":    result.Disposition,
		"backoff_ms":     result.BackoffMs,
		"attempt_number": req.AttemptNumber,
	})

	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *RetryTopicsHandler) ScheduleRetry(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/retries/attempts", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/retries/attempts"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.ScheduleRetryInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "retry-topics", "malformed schedule retry request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("handler_id", req.HandlerID),
		attribute.String("event_id", req.EventID),
		attribute.String("aggregate_id", req.AggregateID),
		attribute.Int("attempt_number", req.AttemptNumber),
	)

	rec, err := h.svc.ScheduleRetryAttempt(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "SCHEDULE_FAILED"))
		observability.LogError(ctx, "retry-topics", "failed scheduling retry attempt", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("retry.public_id", rec.PublicID),
		attribute.String("retry.status", string(rec.Status)),
		attribute.String("retry.idempotency_key", rec.IdempotencyKey),
	)

	observability.LogInfo(ctx, "retry-topics", "retry attempt scheduled successfully in ledger", req.TenantID, map[string]any{
		"public_id":       rec.PublicID,
		"event_id":        rec.EventID,
		"attempt_number":  rec.AttemptNumber,
		"idempotency_key": rec.IdempotencyKey,
	})

	h.writeSuccess(w, http.StatusCreated, rec, start, reqID, corrID, causationID)
}

func (h *RetryTopicsHandler) GetDueRetries(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/retries/due", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	tenantID := r.URL.Query().Get("tenant_id")
	if strings.TrimSpace(tenantID) == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "tenant_id query parameter is required", start, reqID, corrID, causationID)
		return
	}
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/retries/due"),
		attribute.String("tenant_id", tenantID),
		attribute.Int("limit", limit),
		attribute.String("correlation_id", corrID),
	)

	records, err := h.svc.GetDueRetries(ctx, tenantID, limit)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "QUERY_FAILED"))
		observability.LogError(ctx, "retry-topics", "failed querying due retries", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(attribute.Int("records_count", len(records)))
	observability.LogInfo(ctx, "retry-topics", "polled due retries successfully", tenantID, map[string]any{
		"count": len(records),
	})

	h.writeSuccess(w, http.StatusOK, records, start, reqID, corrID, causationID)
}

func (h *RetryTopicsHandler) writeSuccess(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *RetryTopicsHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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



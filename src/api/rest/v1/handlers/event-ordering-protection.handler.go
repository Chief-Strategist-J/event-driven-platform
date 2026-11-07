package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/event-ordering-protection/contracts"
	"event-driven-platform/src/features/event-ordering-protection/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type EventOrderingHandler struct {
	svc *service.EventOrderingService
}

func NewEventOrderingHandler(svc *service.EventOrderingService) *EventOrderingHandler {
	return &EventOrderingHandler{svc: svc}
}

func (h *EventOrderingHandler) EvaluateSequence(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/ordering/sequence", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/ordering/sequence"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.IngestEventSequenceInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "event-ordering-protection", "malformed sequence request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("aggregate_type", req.AggregateType),
		attribute.String("aggregate_id", req.AggregateID),
		attribute.Int64("aggregate_version", req.AggregateVersion),
	)

	result, err := h.svc.EvaluateAndApply(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "event-ordering-protection", "sequence progression rejected", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	observability.LogInfo(ctx, "event-ordering-protection", "sequence evaluated", req.TenantID, map[string]any{
		"action":           result.Action,
		"current_version":  result.CurrentVersion,
		"incoming_version": result.IncomingVersion,
		"is_held":          result.IsHeld,
	})
	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *EventOrderingHandler) DrainHeld(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/ordering/drain", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/ordering/drain"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.DrainHeldEventsInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "event-ordering-protection", "malformed drain request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("aggregate_type", req.AggregateType),
		attribute.String("aggregate_id", req.AggregateID),
	)

	result, err := h.svc.DrainHeldEvents(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "DRAIN_FAILED"))
		observability.LogWarn(ctx, "event-ordering-protection", "drain held events failed", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	observability.LogInfo(ctx, "event-ordering-protection", "events drained successfully", req.TenantID, map[string]any{
		"events_drained": result.EventsDrained,
		"new_version":    result.NewVersion,
		"is_unheld":      result.IsUnheld,
	})
	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *EventOrderingHandler) ListHeld(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
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
	group := r.URL.Query().Get("consumer_group")
	handler := r.URL.Query().Get("handler_id")
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 20
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	offset := 0
	if offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	records, err := h.svc.ListHeldTrackers(r.Context(), tenantID, group, handler, limit, offset)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, records, start, reqID, corrID, causationID)
}

func (h *EventOrderingHandler) writeSuccess(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *EventOrderingHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

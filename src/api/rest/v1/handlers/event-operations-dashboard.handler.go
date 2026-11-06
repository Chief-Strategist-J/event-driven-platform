package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"event-driven-platform/src/features/event-operations-dashboard/contracts"
	"event-driven-platform/src/features/event-operations-dashboard/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type EventOperationsDashboardHandler struct {
	svc *service.EventOperationsDashboardService
}

func NewEventOperationsDashboardHandler(svc *service.EventOperationsDashboardService) *EventOperationsDashboardHandler {
	return &EventOperationsDashboardHandler{svc: svc}
}

func (h *EventOperationsDashboardHandler) HandleReportHopTelemetry(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/ops/hops/report", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.ReportHopTelemetryInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	record, err := h.svc.ReportHopTelemetry(ctx, &input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(attribute.String("ops.hop_public_id", record.PublicID))
	h.writeResponse(w, http.StatusOK, record, start, reqID, corrID, causationID)
}

func (h *EventOperationsDashboardHandler) HandleTriageFlow(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/ops/triage/localize", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.TriageFlowInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	out, err := h.svc.TriageFlow(ctx, &input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(attribute.String("ops.localized_hop", string(out.LocalizedHop)))
	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *EventOperationsDashboardHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *EventOperationsDashboardHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	eventualconsistencymonitoring "event-driven-platform/src/features/eventual-consistency-monitoring"
	"event-driven-platform/src/features/eventual-consistency-monitoring/contracts"
	"event-driven-platform/src/features/eventual-consistency-monitoring/infrastructure/ports"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type ConsistencyMonitoringHandler struct {
	svc *eventualconsistencymonitoring.ConsistencyService
}

func NewConsistencyMonitoringHandler(svc *eventualconsistencymonitoring.ConsistencyService) *ConsistencyMonitoringHandler {
	return &ConsistencyMonitoringHandler{svc: svc}
}

func (h *ConsistencyMonitoringHandler) HandleObserveWatermark(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consistency/watermarks", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/consistency/watermarks"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.ObserveWatermarkInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "watermark-collector"
	}

	record, err := h.svc.ObserveWatermark(ctx, input)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusBadRequest, constants.ErrCodeValidationFailed)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleGetViewWatermarks(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/consistency/views/{view_name}/watermark", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	viewName := r.PathValue("view_name")
	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		tenantID = "default"
	}

	records, err := h.svc.GetViewWatermarks(ctx, tenantID, viewName)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusInternalServerError, constants.ErrCodeInternalError)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, records, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleEvaluateRecord(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consistency/evaluate", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var input contracts.EvaluateRecordInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	sourceService := "consistency-evaluator"
	result, err := h.svc.EvaluateAndRecordDrift(ctx, input, sourceService, corrID)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusInternalServerError, constants.ErrCodeInternalError)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleGetActiveIncidents(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/consistency/views/{view_name}/incidents", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	viewName := r.PathValue("view_name")
	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		tenantID = "default"
	}

	incidents, err := h.svc.GetActiveIncidents(ctx, tenantID, viewName)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusInternalServerError, constants.ErrCodeInternalError)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, incidents, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleAttemptHealing(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consistency/incidents/{id}/heal", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	idStr := r.PathValue("id")
	incidentUUID, err := uuid.Parse(idStr)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "invalid incident uuid", start, reqID, corrID, causationID)
		return
	}

	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		tenantID = "default"
	}

	healed, err := h.svc.AttemptHealing(ctx, incidentUUID, tenantID, 2)
	if errors.Is(err, ports.ErrIncidentNotFound) {
		h.writeError(w, http.StatusNotFound, constants.ErrCodeNotFound, "drift incident not found", start, reqID, corrID, causationID)
		return
	}
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusInternalServerError, constants.ErrCodeInternalError)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	statusMap := map[string]any{
		"incident_id": incidentUUID,
		"healed":      healed,
	}
	h.writeResponse(w, http.StatusOK, statusMap, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleRecordCheckSweep(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consistency/sweeps/run", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.RunCheckSweepInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	record, err := h.svc.RecordCheckSweep(ctx, input)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusBadRequest, constants.ErrCodeValidationFailed)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleListCheckRuns(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/consistency/views/{view_name}/sweeps", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	viewName := r.PathValue("view_name")
	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		tenantID = "default"
	}

	records, err := h.svc.ListCheckRuns(ctx, tenantID, viewName, 50)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusInternalServerError, constants.ErrCodeInternalError)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, records, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleRegisterSilence(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consistency/silences", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.RegisterSilenceInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	record, err := h.svc.RegisterSilence(ctx, input)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusBadRequest, constants.ErrCodeValidationFailed)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) HandleDispatchCanaryProbe(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consistency/probes/canary", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.CanaryProbeInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	output, err := h.svc.DispatchCanaryProbe(ctx, input)
	if err != nil {
		st, cd, msg := classifyServiceError(err, http.StatusBadRequest, constants.ErrCodeValidationFailed)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, output, start, reqID, corrID, causationID)
}

func (h *ConsistencyMonitoringHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *ConsistencyMonitoringHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	cdcFeature "event-driven-platform/src/features/cdc-integration"
	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type CDCHandler struct {
	feat *cdcFeature.CDCIntegrationFeature
}

func NewCDCHandler(feat *cdcFeature.CDCIntegrationFeature) *CDCHandler {
	return &CDCHandler{feat: feat}
}

func (h *CDCHandler) HandleRegisterConnector(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/cdc/connectors", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/cdc/connectors"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.RegisterConnectorInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "cdc-admin-api"
	}
	if input.ExternalID == "" {
		input.ExternalID = input.ConnectorName
	}

	rec, err := h.feat.Service.RegisterConnector(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, rec, start, reqID, corrID, causationID)
}

func (h *CDCHandler) HandleGetConnector(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/cdc/connectors/{name}", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	connName := r.PathValue("name")

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/cdc/connectors/{name}"),
		attribute.String("connector_name", connName),
	)

	rec, err := h.feat.Service.GetConnector(ctx, connName)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, rec, start, reqID, corrID, causationID)
}

func (h *CDCHandler) HandleDispatchSignal(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/cdc/signals", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var input contracts.DispatchSignalInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "cdc-admin-api"
	}
	if input.ExternalID == "" {
		input.ExternalID = string(input.SignalType)
	}

	sig, err := h.feat.Service.DispatchSignal(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusAccepted, sig, start, reqID, corrID, causationID)
}

func (h *CDCHandler) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/cdc/heartbeat", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var input contracts.HeartbeatTickInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "cdc-heartbeat-worker"
	}
	if input.ExternalID == "" {
		input.ExternalID = input.LSNPosition
	}

	if err := h.feat.Service.EmitHeartbeat(ctx, input); err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, map[string]string{"status": "HEARTBEAT_RECORDED"}, start, reqID, corrID, causationID)
}

func (h *CDCHandler) writeServiceError(w http.ResponseWriter, err error, start time.Time, reqID, corrID, causationID string) {
	var appErr *appErrors.ApplicationError
	if errors.As(err, &appErr) {
		h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID, causationID)
		return
	}
	h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal cdc error", start, reqID, corrID, causationID)
}

func (h *CDCHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *CDCHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/event-driven-communication/contracts"
	"event-driven-platform/src/features/event-driven-communication/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type EventCommunicationHandler struct {
	svc *service.EventCommunicationService
}

func NewEventCommunicationHandler(svc *service.EventCommunicationService) *EventCommunicationHandler {
	return &EventCommunicationHandler{svc: svc}
}

func (h *EventCommunicationHandler) HandleStartSaga(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/communication/sagas/start", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/communication/sagas/start"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.StartSagaCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if cmd.CorrelationID == "" {
		cmd.CorrelationID = corrID
	}

	res, err := h.svc.StartSaga(ctx, cmd)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusAccepted, res, start, reqID, corrID, causationID)
}

func (h *EventCommunicationHandler) HandleProcessStepReply(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/communication/sagas/reply", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/communication/sagas/reply"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.ProcessStepReplyCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	res, err := h.svc.ProcessStepReply(ctx, cmd)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *EventCommunicationHandler) HandleTriggerCompensation(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/communication/sagas/compensate", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/communication/sagas/compensate"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.TriggerCompensationCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	res, err := h.svc.TriggerCompensation(ctx, cmd)
	if err != nil {
		h.writeError(w, http.StatusUnprocessableEntity, "COMPENSATION_FAILED", err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *EventCommunicationHandler) HandleDispatchAsyncCommand(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/communication/commands/dispatch", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/communication/commands/dispatch"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.DispatchAsyncCommandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if req.CorrelationID == "" {
		req.CorrelationID = corrID
	}

	res, err := h.svc.DispatchAsyncCommand(ctx, req)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusAccepted, res, start, reqID, corrID, causationID)
}

func (h *EventCommunicationHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *EventCommunicationHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

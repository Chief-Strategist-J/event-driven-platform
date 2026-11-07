package handlers

import (
	"errors"
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/idempotent-consumers/contracts"
	"event-driven-platform/src/features/idempotent-consumers/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type IdempotentConsumerHandler struct {
	service *service.ConsumerService
}

func NewIdempotentConsumerHandler(s *service.ConsumerService) *IdempotentConsumerHandler {
	return &IdempotentConsumerHandler{service: s}
}

func (h *IdempotentConsumerHandler) Consume(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consumers/events", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/consumers/events"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.ConsumerIngressRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	payloadBytes, err := json.Marshal(req.Payload)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "invalid payload structure", start, reqID, corrID, causationID)
		return
	}

	semantic := contracts.SemanticClass(req.SemanticClass)
	if req.SemanticClass == "" {
		semantic = contracts.SemanticDelta
	}

	event := contracts.InboundConsumerEvent{
		EventID:          req.EventID,
		TenantID:         req.TenantID,
		HandlerID:        req.HandlerID,
		AggregateType:    req.AggregateType,
		AggregateID:      req.AggregateID,
		AggregateVersion: req.AggregateVersion,
		EventType:        req.EventType,
		SchemaVersion:    req.SchemaVersion,
		SemanticClass:    semantic,
		Payload:          payloadBytes,
		Headers:          req.Headers,
		EventOccurredAt:  time.Now().UTC(),
	}

	res, err := h.service.ConsumeEvent(ctx, event)
	if err != nil {
		var appErr *appErrors.ApplicationError
		if errors.As(err, &appErr) {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID, causationID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal consumer processing error", start, reqID, corrID, causationID)
		return
	}

	statusCode := http.StatusOK
	if res.Action == contracts.ActionHoldGap {
		statusCode = http.StatusConflict
	} else if res.Action == contracts.ActionParkPoison {
		statusCode = http.StatusLocked
	}

	w.WriteHeader(statusCode)
	resp := types.ApiResponse{
		Success:    res.Action == contracts.ActionApply || res.Action == contracts.ActionSkip,
		StatusCode: statusCode,
		Data:       res,
		Error:      nil,
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

func (h *IdempotentConsumerHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


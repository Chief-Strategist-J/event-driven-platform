package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/domain-event-design/contracts"
	"event-driven-platform/src/features/domain-event-design/domain/rules"
	"event-driven-platform/src/features/domain-event-design/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type DomainEventHandler struct {
	svc *service.DomainEventService
}

func NewDomainEventHandler(svc *service.DomainEventService) *DomainEventHandler {
	return &DomainEventHandler{svc: svc}
}

func (h *DomainEventHandler) HandleBuildEnvelope(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/domain-events/envelope/build", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/domain-events/envelope/build"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.BuildEnvelopeCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if cmd.CorrelationID == "" {
		cmd.CorrelationID = corrID
	}
	if cmd.CausationID == "" {
		cmd.CausationID = causationID
	}

	res, err := h.svc.BuildCanonicalEnvelope(ctx, cmd)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if errors.Is(err, rules.ErrOccConflict) {
			statusCode = http.StatusConflict
			errCode = "OCC_CONFLICT"
		} else if errors.Is(err, rules.ErrAggregateTerminal) {
			statusCode = http.StatusUnprocessableEntity
			errCode = "AGGREGATE_TERMINAL"
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *DomainEventHandler) HandleRegisterCatalogEvent(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/domain-events/catalog", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/domain-events/catalog"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.RegisterCatalogEventCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if cmd.CorrelationID == "" {
		cmd.CorrelationID = corrID
	}

	res, err := h.svc.RegisterCatalogEvent(ctx, cmd)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, res, start, reqID, corrID, causationID)
}

func (h *DomainEventHandler) HandleReserveAggregateVersion(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/domain-events/aggregates/reserve-version", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/domain-events/aggregates/reserve-version"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.ReserveAggregateVersionCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if cmd.CorrelationID == "" {
		cmd.CorrelationID = corrID
	}

	res, err := h.svc.ReserveAggregateVersion(ctx, cmd)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if errors.Is(err, rules.ErrOccConflict) {
			statusCode = http.StatusConflict
			errCode = "OCC_CONFLICT"
		} else if errors.Is(err, rules.ErrAggregateTerminal) {
			statusCode = http.StatusUnprocessableEntity
			errCode = "AGGREGATE_TERMINAL"
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *DomainEventHandler) HandleVerifyCausalChain(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/domain-events/causality/verify", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/domain-events/causality/verify"),
		attribute.String("correlation_id", corrID),
	)

	var cmd contracts.VerifyCausalChainCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	res, err := h.svc.VerifyCausalChain(ctx, cmd)
	if err != nil {
		h.writeError(w, http.StatusUnprocessableEntity, "CAUSAL_CYCLE_DETECTED", err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *DomainEventHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *DomainEventHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


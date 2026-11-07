package handlers

import (
	"errors"
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/transactional-outbox/contracts"
	"event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type RelayBatchRequest struct {
	BatchSize int `json:"batchSize"`
}

type RelayBatchResponseData struct {
	PublishedCount int `json:"publishedCount"`
}

type CreateOutboxRequest struct {
	TenantID         string            `json:"tenantId"`
	SourceService    string            `json:"sourceService"`
	ExternalID       string            `json:"externalId"`
	AggregateType    string            `json:"aggregateType"`
	AggregateID      string            `json:"aggregateId"`
	AggregateVersion int64             `json:"aggregateVersion"`
	EventType        string            `json:"eventType"`
	SchemaVersion    string            `json:"schemaVersion"`
	Payload          json.RawMessage   `json:"payload"`
	Headers          map[string]string `json:"headers,omitempty"`
	CDCMode          bool              `json:"cdcMode,omitempty"`
}

type TransactionalOutboxHandler struct {
	service *service.OutboxService
}

func NewTransactionalOutboxHandler(s *service.OutboxService) *TransactionalOutboxHandler {
	return &TransactionalOutboxHandler{service: s}
}

func (h *TransactionalOutboxHandler) CreateOutboxRecord(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/outbox/events", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	idempotencyKey := r.Header.Get(constants.HeaderXIdempotencyKey)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/outbox/events"),
		attribute.String("correlation_id", corrID),
	)

	var req CreateOutboxRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if causationID == "" {
		causationID = reqID
	}

	input := contracts.CreateOutboxRecordInput{
		TenantID:         req.TenantID,
		SourceService:    req.SourceService,
		ExternalID:       req.ExternalID,
		CorrelationID:    corrID,
		CausationID:      causationID,
		AggregateType:    req.AggregateType,
		AggregateID:      req.AggregateID,
		AggregateVersion: req.AggregateVersion,
		EventType:        req.EventType,
		SchemaVersion:    req.SchemaVersion,
		Payload:          req.Payload,
		Headers:          req.Headers,
		CDCMode:          req.CDCMode,
	}

	record, err := h.service.StageEvent(ctx, input, idempotencyKey)
	if err != nil {
		var appErr *appErrors.ApplicationError
		if errors.As(err, &appErr) {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID, causationID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID, causationID)
		return
	}

	w.WriteHeader(http.StatusCreated)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       record,
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

func (h *TransactionalOutboxHandler) RelayBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/outbox/relay", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/outbox/relay"),
		attribute.String("correlation_id", corrID),
	)

	var req RelayBatchRequest
	if r.Body != nil && r.ContentLength > 0 {
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
			return
		}
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = constants.DefaultRelayBatchSize
	}

	count, err := h.service.RelayBatch(ctx, batchSize)
	if err != nil {
		var appErr *appErrors.ApplicationError
		if errors.As(err, &appErr) {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID, causationID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID, causationID)
		return
	}

	w.WriteHeader(http.StatusOK)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data: RelayBatchResponseData{
			PublishedCount: count,
		},
		Error: nil,
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

func (h *TransactionalOutboxHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

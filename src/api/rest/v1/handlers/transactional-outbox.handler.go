package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"event-driven-platform/src/features/transactional-outbox/service"
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

type TransactionalOutboxHandler struct {
	service *service.OutboxService
}

func NewTransactionalOutboxHandler(s *service.OutboxService) *TransactionalOutboxHandler {
	return &TransactionalOutboxHandler{service: s}
}

func (h *TransactionalOutboxHandler) RelayBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)

	var req RelayBatchRequest
	if r.Body != nil && r.ContentLength > 0 {
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
			return
		}
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

	count, err := h.service.RelayBatch(r.Context(), batchSize)
	if err != nil {
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set(constants.HeaderTraceparent, traceparent)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusOK,
		Data: RelayBatchResponseData{
			PublishedCount: count,
		},
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			CausationID:     r.Header.Get(constants.HeaderXCausationID),
			Timestamp:       utils.FormatRFC3339UTC(time.Now()),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *TransactionalOutboxHandler) writeError(w http.ResponseWriter, code int, canonicalCode string, message string, start time.Time, reqID string, corrID string) {
	w.WriteHeader(code)
	resp := types.ApiResponse{
		Success:    false,
		StatusCode: code,
		Error: &types.ResponseError{
			Code:      canonicalCode,
			Message:   message,
			Retryable: false,
		},
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       utils.FormatRFC3339UTC(time.Now()),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}


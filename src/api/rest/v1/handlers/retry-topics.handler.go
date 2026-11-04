package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/service"
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
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var req contracts.EvaluateRetryInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	result, err := h.svc.EvaluateRetry(r.Context(), req)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *RetryTopicsHandler) ScheduleRetry(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var req contracts.ScheduleRetryInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	rec, err := h.svc.ScheduleRetryAttempt(r.Context(), req)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusCreated, rec, start, reqID, corrID, causationID)
}

func (h *RetryTopicsHandler) GetDueRetries(w http.ResponseWriter, r *http.Request) {
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
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	records, err := h.svc.GetDueRetries(r.Context(), tenantID, limit)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID, causationID)
		return
	}

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

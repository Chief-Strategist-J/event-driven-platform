package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	registryService "event-driven-platform/src/features/event-schema-registry/service"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
)

type SchemaRegistryHandler struct {
	service *registryService.SchemaRegistryService
}

func NewSchemaRegistryHandler(s *registryService.SchemaRegistryService) *SchemaRegistryHandler {
	return &SchemaRegistryHandler{service: s}
}

func (h *SchemaRegistryHandler) RegisterSchema(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)

	var input registryContracts.RegisterSchemaInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
		return
	}

	result, err := h.service.RegisterSchema(r.Context(), input)
	if err != nil {
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set(constants.HeaderTraceparent, traceparent)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       result,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	json.NewEncoder(w).Encode(resp)
}

func (h *SchemaRegistryHandler) writeError(w http.ResponseWriter, code int, errCode string, msg string, start time.Time, reqID string, corrID string) {
	w.WriteHeader(code)
	resp := types.ApiResponse{
		Success:    false,
		StatusCode: code,
		Error: &types.ResponseError{
			Code:      errCode,
			Message:   msg,
			Retryable: code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable,
		},
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      "v1",
		},
	}
	json.NewEncoder(w).Encode(resp)
}

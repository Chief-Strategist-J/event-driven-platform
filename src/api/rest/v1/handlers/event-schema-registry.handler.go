package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	eventschemaregistry "event-driven-platform/src/features/event-schema-registry/contracts"
	"event-driven-platform/src/features/event-schema-registry/service"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type EventSchemaRegistryHandler struct {
	service *service.SchemaRegistryService
}

func NewEventSchemaRegistryHandler(s *service.SchemaRegistryService) *EventSchemaRegistryHandler {
	return &EventSchemaRegistryHandler{service: s}
}

func (h *EventSchemaRegistryHandler) RegisterSchema(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	traceparent := r.Header.Get(constants.HeaderTraceparent)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)

	var req eventschemaregistry.RegisterSchemaInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID)
		return
	}

	res, err := h.service.RegisterSchema(r.Context(), req)
	if err != nil {
		if appErr, ok := err.(*appErrors.ApplicationError); ok {
			h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID)
			return
		}
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID)
		return
	}

	w.WriteHeader(http.StatusCreated)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: http.StatusCreated,
		Data:       res,
		Error:      nil,
		Meta: types.ResponseMeta{
			RequestID:       reqID,
			CorrelationID:   corrID,
			CausationID:     traceparent,
			Timestamp:       utils.FormatRFC3339UTC(time.Now()),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      constants.APIVersionV1,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *EventSchemaRegistryHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID string) {
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
			Timestamp:       utils.FormatRFC3339UTC(time.Now()),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
			APIVersion:      constants.APIVersionV1,
		},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/offset-management/contracts"
	offsetFeature "event-driven-platform/src/features/offset-management"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type OffsetManagementHandler struct {
	feat *offsetFeature.OffsetManagementFeature
}

func NewOffsetManagementHandler(feat *offsetFeature.OffsetManagementFeature) *OffsetManagementHandler {
	return &OffsetManagementHandler{feat: feat}
}

func (h *OffsetManagementHandler) HandleCommitOffset(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/offsets/commit", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	var input contracts.CommitOffsetInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	res, err := h.feat.CommitOffset(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "COMMIT_OFFSET_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *OffsetManagementHandler) HandleRewindOffset(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/offsets/rewind", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	var input contracts.RewindOffsetInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	res, err := h.feat.RewindOffset(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "REWIND_OFFSET_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *OffsetManagementHandler) HandleFastForwardOffset(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/offsets/fast-forward", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	if corrID == "" {
		corrID, _ = utils.GenerateUUIDv7()
	}

	var input contracts.FastForwardOffsetInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	res, err := h.feat.FastForwardOffset(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "FAST_FORWARD_OFFSET_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *OffsetManagementHandler) writeSuccess(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *OffsetManagementHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

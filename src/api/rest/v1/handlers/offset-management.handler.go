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
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/offsets/commit"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.CommitOffsetInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "offset-management", "malformed commit offset request", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("topic", input.Topic),
		attribute.Int("partition", int(input.Partition)),
		attribute.Int64("offset", input.Offset),
		attribute.Int64("log_end_offset", input.LogEndOffset),
	)

	res, err := h.feat.CommitOffset(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "COMMIT_OFFSET_FAILED"))
		observability.LogWarn(ctx, "offset-management", "offset commit rejected", input.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("offset.public_id", res.PublicID),
		attribute.Int64("offset.lag", res.Lag),
		attribute.Int64("offset.commit_epoch", res.CommitEpoch),
	)
	observability.LogInfo(ctx, "offset-management", "offset committed successfully", input.TenantID, map[string]any{
		"public_id":    res.PublicID,
		"group_id":     input.GroupID,
		"topic":        input.Topic,
		"partition":    input.Partition,
		"offset":       input.Offset,
		"commit_epoch": res.CommitEpoch,
	})

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
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/offsets/rewind"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.RewindOffsetInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "offset-management", "malformed rewind offset request", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("topic", input.Topic),
		attribute.Int("partition", int(input.Partition)),
		attribute.Int64("target_offset", input.TargetOffset),
		attribute.String("reason", input.Reason),
	)

	res, err := h.feat.RewindOffset(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "REWIND_OFFSET_FAILED"))
		observability.LogWarn(ctx, "offset-management", "offset rewind rejected", input.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("offset.public_id", res.PublicID),
		attribute.Int64("offset.commit_epoch", res.CommitEpoch),
	)
	observability.LogInfo(ctx, "offset-management", "offset rewound successfully", input.TenantID, map[string]any{
		"public_id":     res.PublicID,
		"group_id":      input.GroupID,
		"topic":         input.Topic,
		"partition":     input.Partition,
		"target_offset": input.TargetOffset,
		"reason":        input.Reason,
	})

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
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/offsets/fast-forward"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.FastForwardOffsetInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "offset-management", "malformed fast-forward offset request", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("group_id", input.GroupID),
		attribute.String("topic", input.Topic),
		attribute.Int("partition", int(input.Partition)),
		attribute.Int64("shift_by", input.ShiftBy),
		attribute.String("reason", input.Reason),
	)

	res, err := h.feat.FastForwardOffset(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "FAST_FORWARD_OFFSET_FAILED"))
		observability.LogWarn(ctx, "offset-management", "offset fast-forward rejected", input.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("offset.public_id", res.PublicID),
		attribute.Int64("offset.commit_epoch", res.CommitEpoch),
	)
	observability.LogInfo(ctx, "offset-management", "offset fast-forwarded successfully", input.TenantID, map[string]any{
		"public_id":     res.PublicID,
		"group_id":      input.GroupID,
		"topic":         input.Topic,
		"partition":     input.Partition,
		"shift_by":      input.ShiftBy,
		"target_offset": res.StoredOffset,
		"reason":        input.Reason,
	})

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

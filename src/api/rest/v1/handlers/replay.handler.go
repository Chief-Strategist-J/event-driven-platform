package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	replayFeature "event-driven-platform/src/features/replay-mechanism"
	"event-driven-platform/src/features/replay-mechanism/contracts"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type ReplayHandler struct {
	feat *replayFeature.ReplayFeature
}

func NewReplayHandler(feat *replayFeature.ReplayFeature) *ReplayHandler {
	return &ReplayHandler{feat: feat}
}

func (h *ReplayHandler) HandleCreateReplayRun(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/replay/runs", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/replay/runs"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.CreateReplayRunInput
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

	res, err := h.feat.Service.CreateReplayRun(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "CREATE_REPLAY_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("replay.public_id", res.PublicID),
		attribute.String("replay.state", string(res.State)),
	)
	observability.LogInfo(ctx, "replay-mechanism", "replay run created", input.TenantID, map[string]any{
		"public_id":           res.PublicID,
		"target_projection":   res.TargetProjection,
		"replay_consumer_grp": res.ReplayConsumerGroup,
	})

	h.writeSuccess(w, http.StatusCreated, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) HandleStartStreaming(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/replay/runs/start", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	var body struct {
		RunID string `json:"runId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	res, err := h.feat.Service.StartStreaming(ctx, tenantID, body.RunID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "START_STREAMING_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) HandleAdvanceReplay(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/replay/runs/advance", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	var input contracts.AdvanceReplayInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}
	if input.TenantID == "" {
		input.TenantID = tenantID
	}

	res, err := h.feat.Service.AdvanceReplay(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "ADVANCE_REPLAY_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) HandlePromoteReplay(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/replay/runs/promote", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	var input contracts.PromoteReplayInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}
	if input.TenantID == "" {
		input.TenantID = tenantID
	}

	res, err := h.feat.Service.PromoteReplay(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "PROMOTE_REPLAY_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) HandleCompleteCutover(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/replay/runs/cutover", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	var input contracts.CompleteCutoverInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}
	if input.TenantID == "" {
		input.TenantID = tenantID
	}

	res, err := h.feat.Service.CompleteCutover(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "CUTOVER_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) HandleEvaluateEffect(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/replay/effects/evaluate", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	var input contracts.EvaluateEffectInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}
	if input.TenantID == "" {
		input.TenantID = tenantID
	}

	res, err := h.feat.Service.EvaluateEffect(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "EVALUATE_EFFECT_FAILED"))
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) HandleGetReplayRun(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/replay/runs", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tenantID := r.Header.Get(constants.HeaderXTenantID)

	q := r.URL.Query()
	input := contracts.GetReplayRunInput{
		TenantID: tenantID,
		RunID:    q.Get("run_id"),
	}

	res, err := h.feat.Service.GetReplayRun(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "GET_REPLAY_FAILED"))
		h.writeError(w, http.StatusNotFound, constants.ErrCodeNotFound, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *ReplayHandler) writeSuccess(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *ReplayHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	materializedviews "event-driven-platform/src/features/materialized-views"
	"event-driven-platform/src/features/materialized-views/contracts"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type MaterializedViewsHandler struct {
	svc *materializedviews.ViewService
}

func NewMaterializedViewsHandler(svc *materializedviews.ViewService) *MaterializedViewsHandler {
	return &MaterializedViewsHandler{svc: svc}
}

func (h *MaterializedViewsHandler) HandleRegisterView(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/materialized-views", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/materialized-views"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.RegisterViewInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "view-admin-api"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_view_" + corrID
	}

	record, err := h.svc.RegisterView(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if strings.Contains(err.Error(), "conflict") || strings.Contains(err.Error(), "duplicate") {
			statusCode = http.StatusConflict
			errCode = constants.ErrCodeConflict
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleGetViewByID(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/materialized-views/{id}", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	publicID := r.PathValue("id")
	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/materialized-views/{id}"),
		attribute.String("public_id", publicID),
	)

	record, err := h.svc.GetViewByPublicID(ctx, publicID)
	if err != nil {
		statusCode := http.StatusInternalServerError
		errCode := constants.ErrCodeInternalError
		if strings.Contains(err.Error(), "not found") {
			statusCode = http.StatusNotFound
			errCode = constants.ErrCodeNotFound
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, record, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleApplyProjection(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/materialized-views/apply", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/materialized-views/apply"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.ApplyProjectionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "view-projection-consumer"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_proj_" + corrID
	}

	eval, err := h.svc.ApplyProjection(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if strings.Contains(err.Error(), "conflict") {
			statusCode = http.StatusConflict
			errCode = constants.ErrCodeConflict
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, eval, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleInitiateRebuild(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/materialized-views/rebuild", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/materialized-views/rebuild"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.InitiateRebuildInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "view-rebuild-operator"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_reb_" + corrID
	}

	log, err := h.svc.InitiateRebuild(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if strings.Contains(err.Error(), "conflict") {
			statusCode = http.StatusConflict
			errCode = constants.ErrCodeConflict
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusAccepted, log, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleAdvanceRebuild(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "PUT /api/v1/materialized-views/rebuild/phase", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/materialized-views/rebuild/phase"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.AdvanceRebuildInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	log, err := h.svc.AdvanceRebuild(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if strings.Contains(err.Error(), "conflict") || strings.Contains(err.Error(), "mismatch") || strings.Contains(err.Error(), "terminal") {
			statusCode = http.StatusConflict
			errCode = constants.ErrCodeConflict
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, log, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleApplyContribution(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/materialized-views/contributions", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var input contracts.ApplyContributionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "view-agg-consumer"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_contrib_" + corrID
	}

	record, err := h.svc.ApplyContribution(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if strings.Contains(err.Error(), "conflict") || strings.Contains(err.Error(), "duplicate") {
			statusCode = http.StatusConflict
			errCode = constants.ErrCodeConflict
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleRecordPeriodEvent(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/materialized-views/periods", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	var input contracts.RecordPeriodEventInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "view-reporting-consumer"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_period_" + corrID
	}

	record, err := h.svc.RecordPeriodEvent(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, record, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) HandleQueryView(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/materialized-views/query", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	tokenHeader := r.Header.Get("x-consistency-token")

	var input contracts.QueryViewInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.ConsistencyToken == "" && tokenHeader != "" {
		input.ConsistencyToken = tokenHeader
	}

	out, err := h.svc.QueryViewWithConsistencyToken(ctx, input)
	if err != nil {
		statusCode := http.StatusBadRequest
		errCode := constants.ErrCodeValidationFailed
		if strings.Contains(err.Error(), "not found") {
			statusCode = http.StatusNotFound
			errCode = constants.ErrCodeNotFound
		}
		h.writeError(w, statusCode, errCode, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *MaterializedViewsHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *MaterializedViewsHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

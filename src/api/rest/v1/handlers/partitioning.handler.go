package handlers

import (
	"encoding/json"
	"net/http"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	partitioning "event-driven-platform/src/features/partitioning-strategy"
	"event-driven-platform/src/features/partitioning-strategy/contracts"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/features/partitioning-strategy/domain/rules"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type PartitioningHandler struct {
	svc *partitioning.PartitioningService
}

func NewPartitioningHandler(svc *partitioning.PartitioningService) *PartitioningHandler {
	return &PartitioningHandler{svc: svc}
}

func (h *PartitioningHandler) HandleRegisterTopology(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/partitioning/topologies", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/partitioning/topologies"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.RegisterTopicTopologyInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "partition-admin-api"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_top_" + corrID
	}

	topology, err := h.svc.RegisterTopicTopology(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, topology, start, reqID, corrID, causationID)
}

func (h *PartitioningHandler) HandleCalculatePartition(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/partitioning/keys/calculate", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/partitioning/keys/calculate"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.CalculatePartitionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	out, err := h.svc.CalculatePartition(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *PartitioningHandler) HandleEvaluateSkew(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/partitioning/skew/evaluate", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/partitioning/skew/evaluate"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.EvaluateSkewInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "skew-monitor-agent"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_skw_" + corrID
	}

	out, err := h.svc.EvaluateSkew(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *PartitioningHandler) HandleInitiateMigration(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/partitioning/migrations", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/partitioning/migrations"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.InitiateMigrationInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "migration-controller"
	}
	if input.ExternalID == "" {
		input.ExternalID = "ext_mig_" + corrID
	}

	record, err := h.svc.InitiateMigration(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *PartitioningHandler) HandleAdvanceMigration(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "PUT /api/v1/partitioning/migrations/phase", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/partitioning/migrations/phase"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.AdvanceMigrationInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	record, err := h.svc.AdvanceMigration(ctx, input)
	if err != nil {
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, record, start, reqID, corrID, causationID)
}

// writeServiceError maps typed ApplicationErrors to their own status. A
// forbidden in-place expansion is a state conflict (409); untyped errors are 500.
func (h *PartitioningHandler) writeServiceError(w http.ResponseWriter, err error, start time.Time, reqID, corrID, causationID string) {
	if errors.Is(err, rules.ErrInPlaceExpansionForbidden) {
		h.writeError(w, http.StatusConflict, constants.ErrCodeConflict, err.Error(), start, reqID, corrID, causationID)
		return
	}
	var appErr *appErrors.ApplicationError
	if errors.As(err, &appErr) {
		h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, err.Error(), start, reqID, corrID, causationID)
		return
	}
	h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal server error", start, reqID, corrID, causationID)
}

func (h *PartitioningHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *PartitioningHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


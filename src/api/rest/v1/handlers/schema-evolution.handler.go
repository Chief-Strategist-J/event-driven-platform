package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	schemaevolution "event-driven-platform/src/features/schema-evolution"
	"event-driven-platform/src/features/schema-evolution/contracts"
	schemaservice "event-driven-platform/src/features/schema-evolution/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type SchemaEvolutionHandler struct {
	svc *schemaevolution.SchemaEvolutionService
}

func NewSchemaEvolutionHandler(svc *schemaevolution.SchemaEvolutionService) *SchemaEvolutionHandler {
	return &SchemaEvolutionHandler{svc: svc}
}

func (h *SchemaEvolutionHandler) HandleRegisterSchema(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/register", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/schemas/register"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.RegisterSchemaInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "schema-registry-admin"
	}

	record, err := h.svc.RegisterSchema(ctx, input)
	if err != nil {
		if errors.Is(err, schemaservice.ErrIncompatibleSchema) || errors.Is(err, schemaservice.ErrDuplicateSchemaFingerprint) || errors.Is(err, schemaservice.ErrNonMonotonicVersion) {
			h.writeError(w, http.StatusConflict, constants.ErrCodeConflict, err.Error(), start, reqID, corrID, causationID)
			return
		}
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *SchemaEvolutionHandler) HandleCheckConsumerReadiness(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/schemas/{subject}/readiness", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	subject := r.PathValue("subject")
	tenantID := r.URL.Query().Get("tenant_id")
	if strings.TrimSpace(tenantID) == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "tenant_id query parameter is required", start, reqID, corrID, causationID)
		return
	}
	targetVersionStr := r.URL.Query().Get("target_version")
	if targetVersionStr == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "target_version query parameter is required", start, reqID, corrID, causationID)
		return
	}
	targetVersion, err := strconv.Atoi(targetVersionStr)
	if err != nil || targetVersion <= 0 {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "target_version must be a positive integer", start, reqID, corrID, causationID)
		return
	}

	input := contracts.CheckConsumerReadinessInput{
		TenantID:      tenantID,
		SubjectName:   subject,
		TargetVersion: targetVersion,
	}

	out, err := h.svc.CheckConsumerReadiness(ctx, input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *SchemaEvolutionHandler) HandleInitiateDualPublish(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/dual-publish", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.InitiateDualPublishInput
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

	record, err := h.svc.InitiateDualPublish(ctx, input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *SchemaEvolutionHandler) HandleEvaluateDecommission(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/decommission/check", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.EvaluateDecommissionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	out, err := h.svc.EvaluateDecommission(ctx, input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *SchemaEvolutionHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *SchemaEvolutionHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


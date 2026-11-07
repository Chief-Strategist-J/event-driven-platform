package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type EventVersioningHandler struct {
	svc *service.EventVersioningService
}

func NewEventVersioningHandler(svc *service.EventVersioningService) *EventVersioningHandler {
	return &EventVersioningHandler{svc: svc}
}

func (h *EventVersioningHandler) RegisterSchema(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/versions", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/schemas/versions"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.RegisterSchemaVersionInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "event-versioning", "malformed request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("event_type", req.EventType),
		attribute.Int("schema_version", req.SchemaVersion),
	)

	record, err := h.svc.RegisterSchemaVersion(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "VALIDATION_FAILED"))
		observability.LogWarn(ctx, "event-versioning", "schema registration rejected", req.TenantID, map[string]any{
			"error":         err.Error(),
			"event_type":    req.EventType,
			"schema_version": req.SchemaVersion,
		})
		st, cd, msg := classifyServiceError(err, http.StatusBadRequest, constants.ErrCodeValidationFailed)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	observability.LogInfo(ctx, "event-versioning", "schema registered successfully", req.TenantID, map[string]any{
		"public_id":      record.PublicID,
		"event_type":     record.EventType,
		"schema_version": record.SchemaVersion,
	})
	h.writeSuccess(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *EventVersioningHandler) CheckCompatibility(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/compatibility/check", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/schemas/compatibility/check"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.CheckCompatibilityInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "event-versioning", "malformed compatibility request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("event_type", req.EventType),
		attribute.Int("target_version", req.TargetVersion),
	)

	result, err := h.svc.CheckCompatibility(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "INTERNAL_ERROR"))
		observability.LogError(ctx, "event-versioning", "compatibility check failed", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *EventVersioningHandler) RegisterUpcaster(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/upcasters", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/schemas/upcasters"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.RegisterUpcasterInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "event-versioning", "malformed upcaster request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	record, err := h.svc.RegisterUpcaster(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "UPCASTER_REGISTRATION_FAILED"))
		observability.LogWarn(ctx, "event-versioning", "upcaster registration rejected", req.TenantID, map[string]any{"error": err.Error()})
		st, cd, msg := classifyServiceError(err, http.StatusBadRequest, constants.ErrCodeValidationFailed)
		h.writeError(w, st, cd, msg, start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusCreated, record, start, reqID, corrID, causationID)
}

func (h *EventVersioningHandler) Upcast(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/schemas/upcast", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/schemas/upcast"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.UpcastStepInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "event-versioning", "malformed upcast request payload", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("event_type", req.EventType),
		attribute.Int("source_version", req.SourceVersion),
		attribute.Int("target_version", req.TargetVersion),
	)

	result, err := h.svc.UpcastEvent(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "UPCAST_FAILED"))
		observability.LogWarn(ctx, "event-versioning", "upcast transformation failed", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	observability.LogInfo(ctx, "event-versioning", "event upcast successful", req.TenantID, map[string]any{
		"event_type":     result.EventType,
		"initial_version": result.InitialVersion,
		"final_version":   result.FinalVersion,
		"steps_applied":   result.StepsApplied,
	})
	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *EventVersioningHandler) writeSuccess(w http.ResponseWriter, code int, data any, start time.Time, reqID, corrID, causationID string) {
	w.WriteHeader(code)
	resp := types.ApiResponse{
		Success:    true,
		StatusCode: code,
		Data:       data,
		Error:      nil,
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

func (h *EventVersioningHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

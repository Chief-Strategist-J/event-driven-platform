package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	bpFeature "event-driven-platform/src/features/backpressure"
	"event-driven-platform/src/features/backpressure/contracts"
	"event-driven-platform/src/features/backpressure/domain/machines"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type BackpressureHandler struct {
	feat *bpFeature.BackpressureFeature
}

func NewBackpressureHandler(feat *bpFeature.BackpressureFeature) *BackpressureHandler {
	return &BackpressureHandler{feat: feat}
}

func (h *BackpressureHandler) HandleReportSaturation(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/backpressure/saturation", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/backpressure/saturation"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.ReportSaturationInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "backpressure-ingress", "malformed saturation payload", tenantID, map[string]any{
			"error": err.Error(),
		})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if _, err := uuid.Parse(strings.TrimSpace(input.CorrelationID)); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "correlationId must be a valid UUID", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.Signal.HandlerID),
		attribute.String("dependency_id", input.Signal.DependencyID),
		attribute.String("pause_reason", string(input.PauseReason)),
	)

	res, err := h.feat.Service.ReportSaturation(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "SATURATION_REPORT_FAILED"))
		observability.LogWarn(ctx, "backpressure-ingress", "report saturation rejected", input.TenantID, map[string]any{
			"error": err.Error(),
		})
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("bp.public_id", res.PublicID),
		attribute.String("bp.state", string(res.State)),
		attribute.Int64("bp.concurrency_limit", res.ConcurrencyLimit),
	)
	observability.LogInfo(ctx, "backpressure", "saturation reported", input.TenantID, map[string]any{
		"public_id":    res.PublicID,
		"handler_id":   input.Signal.HandlerID,
		"dependency_id": input.Signal.DependencyID,
		"state":        res.State,
	})

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *BackpressureHandler) HandleReportRecovery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/backpressure/recovery", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/backpressure/recovery"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.ReportRecoveryInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "backpressure-ingress", "malformed recovery payload", tenantID, map[string]any{
			"error": err.Error(),
		})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if _, err := uuid.Parse(strings.TrimSpace(input.CorrelationID)); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "correlationId must be a valid UUID", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", input.TenantID),
		attribute.String("handler_id", input.HandlerID),
		attribute.String("dependency_id", input.DependencyID),
	)

	res, err := h.feat.Service.ReportRecovery(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "RECOVERY_REPORT_FAILED"))
		observability.LogWarn(ctx, "backpressure-ingress", "report recovery rejected", input.TenantID, map[string]any{
			"error": err.Error(),
		})
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("bp.public_id", res.PublicID),
		attribute.String("bp.state", string(res.State)),
	)
	observability.LogInfo(ctx, "backpressure", "recovery ramp started", input.TenantID, map[string]any{
		"handler_id":    input.HandlerID,
		"dependency_id": input.DependencyID,
		"state":         res.State,
	})

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *BackpressureHandler) HandleConfirmRecovery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/backpressure/recovery/confirm", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/backpressure/recovery/confirm"),
		attribute.String("correlation_id", corrID),
	)

	var input contracts.ReportRecoveryInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "backpressure-ingress", "malformed confirm recovery payload", tenantID, map[string]any{
			"error": err.Error(),
		})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	if input.TenantID == "" {
		input.TenantID = tenantID
	}
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if _, err := uuid.Parse(strings.TrimSpace(input.CorrelationID)); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "correlationId must be a valid UUID", start, reqID, corrID, causationID)
		return
	}

	res, err := h.feat.Service.ConfirmRecovery(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "CONFIRM_RECOVERY_FAILED"))
		observability.LogWarn(ctx, "backpressure-ingress", "confirm recovery rejected", input.TenantID, map[string]any{
			"error": err.Error(),
		})
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(attribute.String("bp.state", string(res.State)))
	observability.LogInfo(ctx, "backpressure", "recovery confirmed", tenantID, map[string]any{
		"handler_id":    input.HandlerID,
		"dependency_id": input.DependencyID,
	})

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *BackpressureHandler) HandleGetState(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/backpressure/state", trace.WithSpanKind(trace.SpanKindServer))
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

	q := r.URL.Query()
	input := contracts.GetBackpressureStateInput{
		TenantID:     tenantID,
		HandlerID:    q.Get("handler_id"),
		DependencyID: q.Get("dependency_id"),
	}

	span.SetAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("handler_id", input.HandlerID),
		attribute.String("dependency_id", input.DependencyID),
	)

	res, err := h.feat.Service.GetState(ctx, input)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "GET_STATE_FAILED"))
		observability.LogWarn(ctx, "backpressure-ingress", "get state rejected or not found", tenantID, map[string]any{
			"handler_id":    input.HandlerID,
			"dependency_id": input.DependencyID,
			"error":         err.Error(),
		})
		h.writeServiceError(w, err, start, reqID, corrID, causationID)
		return
	}

	h.writeSuccess(w, http.StatusOK, res, start, reqID, corrID, causationID)
}

func (h *BackpressureHandler) writeServiceError(w http.ResponseWriter, err error, start time.Time, reqID, corrID, causationID string) {
	var appErr *appErrors.ApplicationError
	switch {
	case errors.As(err, &appErr):
		h.writeError(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, start, reqID, corrID, causationID)
	case errors.Is(err, contracts.ErrRecordNotFound):
		h.writeError(w, http.StatusNotFound, constants.ErrCodeNotFound, "backpressure record not found", start, reqID, corrID, causationID)
	case errors.Is(err, machines.ErrInvalidTransition):
		h.writeError(w, http.StatusConflict, constants.ErrCodeConflict, err.Error(), start, reqID, corrID, causationID)
	case errors.Is(err, contracts.ErrSignalNotSaturated), isBackpressureValidationError(err):
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
	default:
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal backpressure error", start, reqID, corrID, causationID)
	}
}

func isBackpressureValidationError(err error) bool {
	for _, e := range []error{
		contracts.ErrMissingTenantID, contracts.ErrMissingSourceService, contracts.ErrMissingExternalID,
		contracts.ErrMissingCorrelationID, contracts.ErrMissingHandlerID, contracts.ErrMissingDependencyID,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

func (h *BackpressureHandler) writeSuccess(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *BackpressureHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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


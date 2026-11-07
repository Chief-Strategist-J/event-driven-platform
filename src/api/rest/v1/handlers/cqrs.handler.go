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

	cqrsfeature "event-driven-platform/src/features/cqrs"
	"event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type CQRSHandler struct {
	svc *cqrsfeature.CQRSService
}

type signingKeyResponse struct {
	ID            uuid.UUID `json:"id"`
	PublicID      string    `json:"public_id"`
	TenantID      string    `json:"tenant_id"`
	SourceService string    `json:"source_service"`
	ExternalID    string    `json:"external_id"`
	CorrelationID uuid.UUID `json:"correlation_id"`
	KeyVersion    int       `json:"key_version"`
	Status        string    `json:"status"`
	ExpiresAt     time.Time `json:"expires_at"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func NewCQRSHandler(svc *cqrsfeature.CQRSService) *CQRSHandler {
	return &CQRSHandler{svc: svc}
}

func (h *CQRSHandler) HandleDispatchCommand(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/commands/dispatch", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	idempKey := r.Header.Get("x-idempotency-key")
	if idempKey == "" {
		idempKey = r.Header.Get("Idempotency-Key")
	}

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/commands/dispatch"),
		attribute.String("correlation_id", corrID),
	)

	if idempKey == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "missing mandatory Idempotency-Key header", start, reqID, corrID, causationID)
		return
	}

	var input contracts.DispatchCommandInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	input.IdempotencyKey = idempKey
	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}
	if input.SourceService == "" {
		input.SourceService = "cqrs-command-gateway"
	}
	if input.PrincipalID == "" {
		input.PrincipalID = r.Header.Get("x-user-id")
		if input.PrincipalID == "" {
			input.PrincipalID = "anonymous"
		}
	}

	out, err := h.svc.DispatchCommand(ctx, input)
	if err != nil {
		if out != nil && out.Status == contracts.CommandStatusConflict {
			h.writeError(w, http.StatusConflict, constants.ErrCodeConflict, err.Error(), start, reqID, corrID, causationID)
			return
		}
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if out.ConsistencyToken != "" {
		w.Header().Set("x-consistency-token", out.ConsistencyToken)
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *CQRSHandler) HandleResolveQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/queries/resolve", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tokenRaw := r.Header.Get("x-consistency-token")

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "tenant_id query parameter is required", start, reqID, corrID, causationID)
		return
	}
	principalID := r.Header.Get("x-user-id")
	if principalID == "" {
		principalID = "anonymous"
	}

	viewName := r.URL.Query().Get("view_name")
	aggregateType := r.URL.Query().Get("aggregate_type")
	aggregateID := r.URL.Query().Get("aggregate_id")

	input := contracts.ResolveQueryInput{
		TenantID:            tenantID,
		PrincipalID:         principalID,
		ViewName:            viewName,
		AggregateType:       aggregateType,
		AggregateID:         aggregateID,
		ConsistencyTokenRaw: tokenRaw,
		TimeoutBudgetMs:     500,
	}

	out, err := h.svc.ResolveQuery(ctx, input)
	if err != nil {
		switch {
		case errors.Is(err, contracts.ErrNoActiveSigningKey):
			h.writeError(w, http.StatusConflict, constants.ErrCodeConflict, err.Error(), start, reqID, corrID, causationID)
		case errors.Is(err, contracts.ErrSigningKeyLookup):
			h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal error", start, reqID, corrID, causationID)
		default:
			h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		}
		return
	}

	if tokenRaw != "" {
		w.Header().Set("Cache-Control", "no-cache, private")
	}

	h.writeResponse(w, http.StatusOK, out, start, reqID, corrID, causationID)
}

func (h *CQRSHandler) HandleRegisterSigningKey(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/commands/keys", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	var input contracts.RegisterSigningKeyInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	if input.CorrelationID == "" {
		input.CorrelationID = corrID
	}

	record, err := h.svc.RegisterSigningKey(ctx, input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	// raw_secret_value is write-only: never echo key material back to the caller.
	h.writeResponse(w, http.StatusCreated, signingKeyResponse{
		ID:            record.ID,
		PublicID:      record.PublicID,
		TenantID:      record.TenantID,
		SourceService: record.SourceService,
		ExternalID:    record.ExternalID,
		CorrelationID: record.CorrelationID,
		KeyVersion:    record.KeyVersion,
		Status:        record.Status,
		ExpiresAt:     record.ExpiresAt,
		CreatedAt:     record.CreatedAt,
		UpdatedAt:     record.UpdatedAt,
	}, start, reqID, corrID, causationID)
}

func (h *CQRSHandler) HandleAuthoritativeRead(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/queries/authoritative-own", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)
	tokenRaw := r.Header.Get("x-consistency-token")

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "tenant_id query parameter is required", start, reqID, corrID, causationID)
		return
	}
	principalID := r.Header.Get("x-user-id")
	if principalID == "" {
		principalID = "anonymous"
	}

	aggregateType := r.URL.Query().Get("aggregate_type")
	aggregateID := r.URL.Query().Get("aggregate_id")

	input := contracts.AuthoritativeReadInput{
		TenantID:            tenantID,
		PrincipalID:         principalID,
		AggregateType:       aggregateType,
		AggregateID:         aggregateID,
		ConsistencyTokenRaw: tokenRaw,
	}

	record, err := h.svc.AuthoritativeRead(ctx, input)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}
	if record == nil {
		h.writeError(w, http.StatusNotFound, constants.ErrCodeNotFound, "aggregate not found in authoritative write model", start, reqID, corrID, causationID)
		return
	}

	w.Header().Set("x-consistency-mode", "AUTHORITATIVE-OWN")
	h.writeResponse(w, http.StatusOK, record, start, reqID, corrID, causationID)
}

func (h *CQRSHandler) writeResponse(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *CQRSHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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

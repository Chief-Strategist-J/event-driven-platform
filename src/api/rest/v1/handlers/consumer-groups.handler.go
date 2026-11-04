package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/consumer-groups/contracts"
	"event-driven-platform/src/features/consumer-groups/service"
	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	"event-driven-platform/src/shared/types"
	"event-driven-platform/src/shared/utils"
)

type ConsumerGroupsHandler struct {
	svc *service.ConsumerGroupsService
}

func NewConsumerGroupsHandler(svc *service.ConsumerGroupsService) *ConsumerGroupsHandler {
	return &ConsumerGroupsHandler{svc: svc}
}

func (h *ConsumerGroupsHandler) RegisterMember(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consumer-groups/members", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/consumer-groups/members"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.RegisterMemberInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "consumer-groups", "malformed member registration request", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("group_id", req.GroupID),
		attribute.String("member_id", req.MemberID),
		attribute.String("assignment_strategy", req.AssignmentStrategy),
	)

	rec, err := h.svc.RegisterMember(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "REGISTRATION_FAILED"))
		observability.LogWarn(ctx, "consumer-groups", "member registration failed", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("member.public_id", rec.PublicID),
		attribute.String("member.status", string(rec.Status)),
	)
	observability.LogInfo(ctx, "consumer-groups", "member registered successfully", req.TenantID, map[string]any{
		"public_id": rec.PublicID,
		"group_id":  rec.GroupID,
		"member_id": rec.MemberID,
		"status":    rec.Status,
	})

	h.writeSuccess(w, http.StatusCreated, rec, start, reqID, corrID, causationID)
}

func (h *ConsumerGroupsHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consumer-groups/heartbeat", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/consumer-groups/heartbeat"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.HeartbeatInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "consumer-groups", "malformed heartbeat request", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("group_id", req.GroupID),
		attribute.String("member_id", req.MemberID),
	)

	result, err := h.svc.ProcessHeartbeat(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "HEARTBEAT_FAILED"))
		observability.LogWarn(ctx, "consumer-groups", "heartbeat processing failed", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("member.status", string(result.Status)),
		attribute.Int64("generation_id", result.GenerationID),
		attribute.Bool("must_rejoin", result.MustRejoin),
	)
	observability.LogInfo(ctx, "consumer-groups", "heartbeat processed successfully", req.TenantID, map[string]any{
		"group_id":      req.GroupID,
		"member_id":     req.MemberID,
		"status":        result.Status,
		"generation_id": result.GenerationID,
		"must_rejoin":   result.MustRejoin,
	})

	h.writeSuccess(w, http.StatusOK, result, start, reqID, corrID, causationID)
}

func (h *ConsumerGroupsHandler) RebalanceSync(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "POST /api/v1/consumer-groups/rebalance", trace.WithSpanKind(trace.SpanKindServer))
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
		attribute.String("http.route", "/api/v1/consumer-groups/rebalance"),
		attribute.String("correlation_id", corrID),
	)

	var req contracts.RebalanceSyncInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "MALFORMED_JSON"))
		observability.LogWarn(ctx, "consumer-groups", "malformed rebalance sync request", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, "malformed request payload", start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("tenant_id", req.TenantID),
		attribute.String("group_id", req.GroupID),
		attribute.String("member_id", req.MemberID),
		attribute.Int64("generation_id", req.GenerationID),
	)

	logRec, err := h.svc.SyncRebalance(ctx, req)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "REBALANCE_FAILED"))
		observability.LogError(ctx, "consumer-groups", "rebalance sync failed", req.TenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusBadRequest, constants.ErrCodeValidationFailed, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(
		attribute.String("rebalance.public_id", logRec.PublicID),
	)
	observability.LogInfo(ctx, "consumer-groups", "rebalance synced successfully", req.TenantID, map[string]any{
		"public_id":     logRec.PublicID,
		"group_id":      logRec.GroupID,
		"generation_id": logRec.GenerationID,
	})

	h.writeSuccess(w, http.StatusOK, logRec, start, reqID, corrID, causationID)
}

func (h *ConsumerGroupsHandler) ListMembers(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, span := observability.StartSpan(r.Context(), "GET /api/v1/consumer-groups/members", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	reqID := r.Header.Get(constants.HeaderXRequestID)
	corrID := r.Header.Get(constants.HeaderXCorrelationID)
	causationID := r.Header.Get(constants.HeaderXCausationID)

	if corrID == "" {
		newUUID, _ := utils.GenerateUUIDv7()
		corrID = newUUID
	}

	tenantID := r.URL.Query().Get("tenant_id")
	groupID := r.URL.Query().Get("group_id")

	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.route", "/api/v1/consumer-groups/members"),
		attribute.String("tenant_id", tenantID),
		attribute.String("group_id", groupID),
		attribute.String("correlation_id", corrID),
	)

	members, err := h.svc.ListMembers(ctx, tenantID, groupID)
	if err != nil {
		observability.RecordError(span, err, attribute.String("error.type", "QUERY_FAILED"))
		observability.LogError(ctx, "consumer-groups", "failed listing members", tenantID, map[string]any{"error": err.Error()})
		h.writeError(w, http.StatusInternalServerError, constants.ErrCodeInternalError, err.Error(), start, reqID, corrID, causationID)
		return
	}

	span.SetAttributes(attribute.Int("members_count", len(members)))
	observability.LogInfo(ctx, "consumer-groups", "listed members successfully", tenantID, map[string]any{
		"count": len(members),
	})

	h.writeSuccess(w, http.StatusOK, members, start, reqID, corrID, causationID)
}

func (h *ConsumerGroupsHandler) writeSuccess(w http.ResponseWriter, status int, data any, start time.Time, reqID, corrID, causationID string) {
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

func (h *ConsumerGroupsHandler) writeError(w http.ResponseWriter, code int, canonical, msg string, start time.Time, reqID, corrID, causationID string) {
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



package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/shared/constants"
	appErrors "event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/types"
)

type contextKey string

const (
	ContextKeyTenantID      contextKey = "tenant_id"
	ContextKeyRequestID     contextKey = "request_id"
	ContextKeyCorrelationID contextKey = "correlation_id"
	ContextKeyCausationID   contextKey = "causation_id"
)

func TraceAndContextMiddleware(next http.Handler) http.Handler {
	propagator := otel.GetTextMapPropagator()
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		spanName := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
		ctx, span := observability.StartSpan(ctx, spanName, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		reqID := r.Header.Get(constants.HeaderXRequestID)
		if reqID == "" {
			newUUID, _ := uuid.NewV7()
			reqID = newUUID.String()
		}

		corrID := r.Header.Get(constants.HeaderXCorrelationID)
		if corrID == "" {
			corrID = reqID
		}

		causationID := r.Header.Get(constants.HeaderXCausationID)
		if causationID == "" {
			causationID = reqID
		}

		tenantID := r.Header.Get(constants.HeaderXTenantID)

		ctx = context.WithValue(ctx, ContextKeyRequestID, reqID)
		ctx = context.WithValue(ctx, ContextKeyCorrelationID, corrID)
		ctx = context.WithValue(ctx, ContextKeyCausationID, causationID)
		ctx = context.WithValue(ctx, ContextKeyTenantID, tenantID)

		w.Header().Set(constants.HeaderXRequestID, reqID)
		w.Header().Set(constants.HeaderXCorrelationID, corrID)

		if span.SpanContext().IsValid() {
			w.Header().Set("X-Trace-Id", span.SpanContext().TraceID().String())
			w.Header().Set("X-Span-Id", span.SpanContext().SpanID().String())
			propagator.Inject(ctx, propagation.HeaderCarrier(w.Header()))
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func ErrorRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				span := trace.SpanFromContext(r.Context())
				err := fmt.Errorf("panic recovered: %v", rec)
				observability.RecordError(span, err)
				observability.LogError(r.Context(), "http-ingress", "panic recovered in request lifecycle", "", map[string]any{
					"error":  err.Error(),
					"method": r.Method,
					"path":   r.URL.Path,
				})

				writeErrorEnvelope(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal server error", r)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func writeErrorEnvelope(w http.ResponseWriter, statusCode int, canonicalCode, message string, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	reqID, _ := r.Context().Value(ContextKeyRequestID).(string)
	corrID, _ := r.Context().Value(ContextKeyCorrelationID).(string)
	causationID, _ := r.Context().Value(ContextKeyCausationID).(string)

	resp := types.ApiResponse{
		Success:    false,
		StatusCode: statusCode,
		Data:       nil,
		Error: &types.ResponseError{
			Code:      canonicalCode,
			Message:   message,
			Retryable: statusCode >= 500,
		},
		Meta: types.ResponseMeta{
			RequestID:     reqID,
			CorrelationID: corrID,
			CausationID:   causationID,
			Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
			APIVersion:    "v1",
		},
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func FormatErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
	if appErr, ok := err.(*appErrors.ApplicationError); ok {
		writeErrorEnvelope(w, appErr.StatusCode, appErr.CanonicalCode, appErr.Message, r)
		return
	}
	writeErrorEnvelope(w, http.StatusInternalServerError, constants.ErrCodeInternalError, "internal server error", r)
}

func TenantFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(ContextKeyTenantID).(string); ok {
		return val
	}
	return ""
}

func RequestIDFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(ContextKeyRequestID).(string); ok {
		return val
	}
	return ""
}

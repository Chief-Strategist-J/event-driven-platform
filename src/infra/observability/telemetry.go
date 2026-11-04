package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var defaultTracer = otel.Tracer("event-driven-platform")

// StartSpan creates a named child span following the 4-tier hierarchy.
func StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return defaultTracer.Start(ctx, spanName, opts...)
}

// RecordError implements Step 1 of the 3-step failure invariant:
// marks active span as ERROR and attaches error description and stack.
func RecordError(span trace.Span, err error, attrs ...attribute.KeyValue) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}

// StructuredLog represents the canonical log-to-trace JSON schema mandated by 10-tracing-writing.md.
type StructuredLog struct {
	Timestamp  string         `json:"timestamp"`
	Level      string         `json:"level"`
	Service    string         `json:"service"`
	Message    string         `json:"message"`
	TraceID    string         `json:"trace_id,omitempty"`
	SpanID     string         `json:"span_id,omitempty"`
	TenantID   string         `json:"tenant_id,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// Log emits structured JSON logs with active context correlation (Step 2 of failure invariant).
func Log(ctx context.Context, level, service, message string, tenantID string, attrs map[string]any) {
	entry := StructuredLog{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Level:      level,
		Service:    service,
		Message:    message,
		TenantID:   tenantID,
		Attributes: attrs,
	}

	span := trace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		entry.TraceID = span.SpanContext().TraceID().String()
		entry.SpanID = span.SpanContext().SpanID().String()
	}

	data, err := json.Marshal(entry)
	if err == nil {
		fmt.Fprintln(os.Stderr, string(data))
	}
}

func LogWarn(ctx context.Context, service, message, tenantID string, attrs map[string]any) {
	Log(ctx, "WARN", service, message, tenantID, attrs)
}

func LogError(ctx context.Context, service, message, tenantID string, attrs map[string]any) {
	Log(ctx, "ERROR", service, message, tenantID, attrs)
}

func LogInfo(ctx context.Context, service, message, tenantID string, attrs map[string]any) {
	Log(ctx, "INFO", service, message, tenantID, attrs)
}

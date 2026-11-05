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
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

var defaultTracer = otel.Tracer("event-driven-platform")

func InitGlobalTracer(ctx context.Context, serviceName, collectorEndpoint string) (func(context.Context) error, error) {
	if collectorEndpoint == "" {
		collectorEndpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if collectorEndpoint == "" {
			collectorEndpoint = "127.0.0.1:4317"
		}
	}
	if serviceName == "" {
		serviceName = "event-driven-platform"
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(collectorEndpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", "local"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize resource: %w", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(
		exporter,
		sdktrace.WithBatchTimeout(500*time.Millisecond),
		sdktrace.WithExportTimeout(3*time.Second),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defaultTracer = tp.Tracer(serviceName)

	return tp.Shutdown, nil
}

func StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return defaultTracer.Start(ctx, spanName, opts...)
}

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

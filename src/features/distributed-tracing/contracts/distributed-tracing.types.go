package contracts

import (
	"time"

	"github.com/google/uuid"
)

type HopType string

const (
	HopTypeConnectRPC    HopType = "CONNECT_RPC"
	HopTypeOutboxRelay   HopType = "OUTBOX_RELAY"
	HopTypeKafkaConsumer HopType = "KAFKA_CONSUMER"
	HopTypeAsyncReaction HopType = "ASYNC_REACTION"
)

type W3CTraceContext struct {
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`
	TraceFlags string `json:"trace_flags"`
	TraceState string `json:"trace_state"`
}

type TraceSamplingPolicy struct {
	ID                 uuid.UUID `json:"id"`
	PublicID           string    `json:"public_id"`
	TenantID           string    `json:"tenant_id"`
	SourceService      string    `json:"source_service"`
	ExternalID         string    `json:"external_id"`
	CorrelationID      uuid.UUID `json:"correlation_id"`
	ServiceName        string    `json:"service_name"`
	FlowName           string    `json:"flow_name"`
	SampleRate         float64   `json:"sample_rate"`
	Priority           int       `json:"priority"`
	AlwaysSampleErrors bool      `json:"always_sample_errors"`
	MinDurationMs      int       `json:"min_duration_ms"`
	IsActive           bool      `json:"is_active"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type TraceSpanAudit struct {
	ID                     uuid.UUID `json:"id"`
	PublicID               string    `json:"public_id"`
	TenantID               string    `json:"tenant_id"`
	SourceService          string    `json:"source_service"`
	ExternalID             string    `json:"external_id"`
	CorrelationID          uuid.UUID `json:"correlation_id"`
	TraceID                string    `json:"trace_id"`
	SpanID                 string    `json:"span_id"`
	ParentSpanID           string    `json:"parent_span_id"`
	ServiceName            string    `json:"service_name"`
	OperationName          string    `json:"operation_name"`
	DurationMs             int       `json:"duration_ms"`
	HasError               bool      `json:"has_error"`
	SanitizedAttributesJSON string    `json:"sanitized_attributes_json"`
	CreatedAt              time.Time `json:"created_at"`
}

type TracePropagationCoverage struct {
	ID               uuid.UUID `json:"id"`
	PublicID         string    `json:"public_id"`
	TenantID         string    `json:"tenant_id"`
	SourceService    string    `json:"source_service"`
	ExternalID       string    `json:"external_id"`
	CorrelationID    uuid.UUID `json:"correlation_id"`
	HopSource        string    `json:"hop_source"`
	HopTarget        string    `json:"hop_target"`
	HopType          HopType   `json:"hop_type"`
	TotalSpans       int64     `json:"total_spans"`
	PropagatedSpans  int64     `json:"propagated_spans"`
	CoverageRatio    float64   `json:"coverage_ratio"`
	EvaluatedAt      time.Time `json:"evaluated_at"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}


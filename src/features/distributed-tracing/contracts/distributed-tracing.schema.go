package contracts

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func GeneratePublicID(prefix string) string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b))
}

type InjectTraceContextCommand struct {
	TenantID      string            `json:"tenant_id"`
	SourceService string            `json:"source_service"`
	CorrelationID string            `json:"correlation_id"`
	Carrier       map[string]string `json:"carrier"`
}

type InjectTraceContextResult struct {
	Traceparent string            `json:"traceparent"`
	Tracestate  string            `json:"tracestate"`
	Carrier     map[string]string `json:"carrier"`
}

type ExtractTraceContextCommand struct {
	Carrier         map[string]string `json:"carrier"`
	IsBatch         bool              `json:"is_batch"`
	QueueWaitMillis int64             `json:"queue_wait_millis"`
}

type ExtractTraceContextResult struct {
	Context       W3CTraceContext `json:"context"`
	LinkingMode   string          `json:"linking_mode"`
	AttachAsLink  bool            `json:"attach_as_link"`
	AttachAsChild bool            `json:"attach_as_child"`
}

type EvaluateSamplingCommand struct {
	TenantID      string `json:"tenant_id"`
	SourceService string `json:"source_service"`
	FlowName      string `json:"flow_name"`
	CorrelationID string `json:"correlation_id"`
	DurationMs    int    `json:"duration_ms"`
	HasError      bool   `json:"has_error"`
}

type EvaluateSamplingResult struct {
	ShouldSample bool    `json:"should_sample"`
	SampleRate   float64 `json:"sample_rate"`
	DecisionRule string  `json:"decision_rule"`
}

type ReportCoverageCommand struct {
	TenantID        string  `json:"tenant_id"`
	SourceService   string  `json:"source_service"`
	ExternalID      string  `json:"external_id"`
	CorrelationID   string  `json:"correlation_id"`
	HopSource       string  `json:"hop_source"`
	HopTarget       string  `json:"hop_target"`
	HopType         HopType `json:"hop_type"`
	TotalSpans      int64   `json:"total_spans"`
	PropagatedSpans int64   `json:"propagated_spans"`
}

type ReportCoverageResult struct {
	HopSource     string  `json:"hop_source"`
	HopTarget     string  `json:"hop_target"`
	CoverageRatio float64 `json:"coverage_ratio"`
	IsHealthy     bool    `json:"is_healthy"`
}

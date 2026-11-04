package types

type TraceContext struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
}

type RequestContext struct {
	TraceContext   TraceContext `json:"traceContext"`
	RequestID      string       `json:"requestId"`
	CorrelationID  string       `json:"correlationId"`
	CausationID    string       `json:"causationId"`
	IdempotencyKey string       `json:"idempotencyKey"`
	TenantID       string       `json:"tenantId"`
	ClientID       string       `json:"clientId"`
	UserID         string       `json:"userId"`
}

type ResponseMeta struct {
	RequestID       string `json:"requestId"`
	CorrelationID   string `json:"correlationId"`
	CausationID     string `json:"causationId"`
	Timestamp       string `json:"timestamp"`
	ExecutionTimeMs int64  `json:"executionTimeMs"`
	APIVersion      string `json:"apiVersion"`
}

type ResponseErrorDetail struct {
	Field         string `json:"field"`
	Issue         string `json:"issue"`
	Rule          string `json:"rule"`
	RejectedValue any    `json:"rejectedValue,omitempty"`
}

type ResponseError struct {
	Code      string                `json:"code"`
	Message   string                `json:"message"`
	Retryable bool                  `json:"retryable"`
	Details   []ResponseErrorDetail `json:"details,omitempty"`
}

type ApiResponse struct {
	Success    bool           `json:"success"`
	StatusCode int            `json:"statusCode"`
	Data       any            `json:"data"`
	Error      *ResponseError `json:"error"`
	Meta       ResponseMeta   `json:"meta"`
}

type EventEnvelope[T any] struct {
	EventID       string       `json:"event_id"`
	Type          string       `json:"type"`
	SchemaVersion string       `json:"schema_version"`
	AggregateID   string       `json:"aggregate_id"`
	AggregateSeq  int64        `json:"aggregate_seq"`
	OccurredAt    string       `json:"occurred_at"`
	CausationID   string       `json:"causation_id"`
	CorrelationID string       `json:"correlation_id"`
	TenantID      string       `json:"tenant_id"`
	TraceContext  TraceContext `json:"trace_context"`
	Payload       T            `json:"payload"`
}

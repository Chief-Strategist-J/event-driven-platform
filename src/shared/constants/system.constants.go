package constants

const (
	HeaderTraceparent     = "traceparent"
	HeaderXRequestID      = "x-request-id"
	HeaderXCorrelationID  = "x-correlation-id"
	HeaderXCausationID    = "x-causation-id"
	HeaderXIdempotencyKey = "x-idempotency-key"
	HeaderXTenantID       = "x-tenant-id"
	HeaderXClientID       = "x-client-id"
	HeaderXUserID         = "x-user-id"
	HeaderXAPIVersion     = "x-api-version"
	HeaderIfMatch         = "if-match"
	HeaderETag            = "etag"
	HeaderRetryAfter      = "retry-after"
	HeaderContentType     = "Content-Type"
	HeaderXTraceID        = "X-Trace-Id"
	HeaderXSpanID         = "X-Span-Id"

	HeaderEventType     = "event_type"
	HeaderSchemaVersion = "schema_version"
	HeaderSourceService = "source_service"
)

const (
	ContentTypeJSON = "application/json"
	APIVersionV1    = "v1"
	PrefixOutbox    = "otb"
	PrefixCanary    = "canary"
)

const (
	ValidationRuleRequired       = "required"
	ValidationRuleMinValue       = "min_value"
	ValidationRuleSizeLimit      = "size_limit"
	ValidationRuleNoFloats       = "no_floats_allowed"
	ValidationRuleMalformedJSON  = "malformed_json"
)

const (
	FieldTenantID         = "tenant_id"
	FieldSourceService    = "source_service"
	FieldAggregateType    = "aggregate_type"
	FieldAggregateID      = "aggregate_id"
	FieldAggregateVersion = "aggregate_version"
	FieldEventType        = "event_type"
	FieldPayload          = "payload"
	FieldIdempotencyKey   = "idempotency_key"
)

const (
	ErrCodeValidationFailed   = "VALIDATION_FAILED"
	ErrCodeConflict           = "CONFLICT"
	ErrCodeNotFound           = "RESOURCE_NOT_FOUND"
	ErrCodePreconditionFailed = "PRECONDITION_FAILED"
	ErrCodeInternalError      = "INTERNAL_SERVER_ERROR"
)

const (
	StatusPending   = "PENDING"
	StatusInFlight  = "IN_FLIGHT"
	StatusPublished = "PUBLISHED"
	StatusFailed    = "FAILED"
	StatusParked    = "PARKED"
)

const (
	MaxOutboxPayloadBytes = 1048576
	DefaultRelayBatchSize = 50
	MaxWorkerRetries      = 5
	DefaultLeaseSeconds   = 30
	PurgeIntervalHours    = 48
)

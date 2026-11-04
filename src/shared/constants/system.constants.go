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
	PrefixInbox     = "inbox"
)

const (
	ValidationRuleRequired       = "required"
	ValidationRuleMinValue       = "min_value"
	ValidationRuleSizeLimit      = "size_limit"
	ValidationRuleNoFloats       = "no_floats_allowed"
	ValidationRuleMalformedJSON  = "malformed_json"
	ValidationRuleInvalidVersion = "invalid_version"
	ValidationRuleFormat         = "format_invalid"
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
	FieldEventID          = "event_id"
	FieldHandlerID        = "handler_id"
	FieldPayloadHash      = "payload_hash"
)

const (
	ErrCodeValidationFailed   = "VALIDATION_FAILED"
	ErrCodeConflict           = "CONFLICT"
	ErrCodeNotFound           = "RESOURCE_NOT_FOUND"
	ErrCodePreconditionFailed = "PRECONDITION_FAILED"
	ErrCodeInternalError      = "INTERNAL_SERVER_ERROR"
	ErrCodeVersionGap         = "VERSION_GAP_DETECTED"
	ErrCodeDatabaseConstraint = "DATABASE_CONSTRAINT_VIOLATION"
)

const (
	ConstraintConsumerInboxDedupeUK              = "consumer_inbox_dedupe_uk"
	ConstraintConsumerInboxTenantIDCK            = "consumer_inbox_tenant_id_ck"
	ConstraintConsumerInboxHandlerIDCK           = "consumer_inbox_handler_id_ck"
	ConstraintConsumerInboxAggregateTypeCK       = "consumer_inbox_aggregate_type_ck"
	ConstraintConsumerInboxAggregateIDCK         = "consumer_inbox_aggregate_id_ck"
	ConstraintConsumerInboxAggregateVersionCK    = "consumer_inbox_aggregate_version_ck"
	ConstraintConsumerInboxEventTypeCK           = "consumer_inbox_event_type_ck"
	ConstraintConsumerInboxPayloadHashCK         = "consumer_inbox_payload_hash_ck"
	ConstraintConsumerProjectionsTenantIDCK      = "consumer_projections_tenant_id_ck"
	ConstraintConsumerProjectionsHandlerIDCK     = "consumer_projections_handler_id_ck"
	ConstraintConsumerProjectionsAggregateTypeCK = "consumer_projections_aggregate_type_ck"
	ConstraintConsumerProjectionsAggregateIDCK   = "consumer_projections_aggregate_id_ck"
	ConstraintConsumerProjectionsLastVersionCK   = "consumer_projections_last_version_ck"
	ConstraintConsumerProjectionsPayloadHashCK   = "consumer_projections_payload_hash_ck"
)

const (
	StatusPending   = "PENDING"
	StatusInFlight  = "IN_FLIGHT"
	StatusPublished = "PUBLISHED"
	StatusFailed    = "FAILED"
	StatusParked    = "PARKED"

	StatusApplied   = "APPLIED"
	StatusDuplicate = "DUPLICATE_SKIPPED"
	StatusGapHeld   = "GAP_HELD"
)

const (
	SemanticClassState = "STATE"
	SemanticClassDelta = "DELTA"
)

const (
	MaxOutboxPayloadBytes = 1048576
	DefaultRelayBatchSize = 50
	MaxWorkerRetries      = 5
	DefaultLeaseSeconds   = 30
	PurgeIntervalHours    = 48
)

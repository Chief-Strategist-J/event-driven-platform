package contracts

import (
	"time"
)

type StreamOrderingClass string

const (
	OrderingStrict StreamOrderingClass = "O_STRICT"
	OrderingState  StreamOrderingClass = "O_STATE"
	OrderingNone   StreamOrderingClass = "O_NONE"
)

type FailureClassification string

const (
	ClassEnvironmental    FailureClassification = "ENVIRONMENTAL"
	ClassContention       FailureClassification = "CONTENTION"
	ClassRateLimited      FailureClassification = "RATE_LIMITED"
	ClassMessageTransient FailureClassification = "MESSAGE_TRANSIENT"
	ClassPermanentData    FailureClassification = "PERMANENT_DATA"
	ClassBusinessReject   FailureClassification = "BUSINESS_REJECT"
)

type RetryDisposition string

const (
	DispositionPauseCircuit    RetryDisposition = "PAUSE_CIRCUIT"
	DispositionImmediateRetry  RetryDisposition = "IMMEDIATE_RETRY"
	DispositionHeldScheduled   RetryDisposition = "HELD_SCHEDULED"
	DispositionTopicScheduled  RetryDisposition = "TOPIC_SCHEDULED"
	DispositionDeadLetter      RetryDisposition = "DEAD_LETTER"
	DispositionExpired         RetryDisposition = "EXPIRED"
)

type RetryAttemptStatus string

const (
	StatusScheduled RetryAttemptStatus = "SCHEDULED"
	StatusRetrying  RetryAttemptStatus = "RETRYING"
	StatusExhausted RetryAttemptStatus = "EXHAUSTED"
	StatusExpired   RetryAttemptStatus = "EXPIRED"
	StatusResolved  RetryAttemptStatus = "RESOLVED"
)

type RetryAttemptLedgerRecord struct {
	ID                    string                `json:"id"`
	PublicID              string                `json:"publicId"`
	TenantID              string                `json:"tenantId"`
	SourceService         string                `json:"sourceService"`
	ExternalID            string                `json:"externalId"`
	CorrelationID         string                `json:"correlationId"`
	ConsumerGroup         string                `json:"consumerGroup"`
	HandlerID             string                `json:"handlerId"`
	EventID               string                `json:"eventId"`
	AggregateType         string                `json:"aggregateType"`
	AggregateID           string                `json:"aggregateId"`
	AggregateVersion      int64                 `json:"aggregateVersion"`
	AttemptNumber         int                   `json:"attemptNumber"`
	MaxAttempts           int                   `json:"maxAttempts"`
	BackoffMs             int64                 `json:"backoffMs"`
	DueAt                 time.Time             `json:"dueAt"`
	Status                RetryAttemptStatus    `json:"status"`
	FailureClassification FailureClassification `json:"failureClassification"`
	IdempotencyKey        string                `json:"idempotencyKey"`
	ErrorMessage          string                `json:"errorMessage"`
	ErrorCode             string                `json:"errorCode"`
	ValidUntil            *time.Time            `json:"validUntil,omitempty"`
	CreatedAt             time.Time             `json:"createdAt"`
	UpdatedAt             time.Time             `json:"updatedAt"`
}

type EvaluateRetryInput struct {
	StreamOrdering StreamOrderingClass   `json:"streamOrdering"`
	Classification FailureClassification `json:"classification"`
	AttemptNumber  int                   `json:"attemptNumber"`
	MaxAttempts    int                   `json:"maxAttempts"`
	BaseBackoffMs  int64                 `json:"baseBackoffMs"`
	MaxBackoffMs   int64                 `json:"maxBackoffMs"`
	ValidUntil     *time.Time            `json:"validUntil,omitempty"`
	CircuitOpen    bool                  `json:"circuitOpen"`
	RetryAfterMs   int64                 `json:"retryAfterMs,omitempty"`
}

type EvaluateRetryResult struct {
	Disposition RetryDisposition `json:"disposition"`
	BackoffMs   int64            `json:"backoffMs"`
	DueAt       time.Time        `json:"dueAt"`
	Reason      string           `json:"reason"`
}

type ScheduleRetryInput struct {
	TenantID         string                `json:"tenantId"`
	SourceService    string                `json:"sourceService"`
	ExternalID       string                `json:"externalId"`
	CorrelationID    string                `json:"correlationId,omitempty"`
	ConsumerGroup    string                `json:"consumerGroup"`
	HandlerID        string                `json:"handlerId"`
	EventID          string                `json:"eventId"`
	AggregateType    string                `json:"aggregateType"`
	AggregateID      string                `json:"aggregateId"`
	AggregateVersion int64                 `json:"aggregateVersion"`
	AttemptNumber    int                   `json:"attemptNumber"`
	MaxAttempts      int                   `json:"maxAttempts"`
	BackoffMs        int64                 `json:"backoffMs"`
	DueAt            time.Time             `json:"dueAt"`
	Classification   FailureClassification `json:"classification"`
	ErrorMessage     string                `json:"errorMessage"`
	ErrorCode        string                `json:"errorCode"`
	ValidUntil       *time.Time            `json:"validUntil,omitempty"`
}

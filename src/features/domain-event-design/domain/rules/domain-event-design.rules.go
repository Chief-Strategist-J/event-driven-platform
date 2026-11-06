package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidEventTypeFormat = errors.New("event type must follow '<domain>.<aggregate>.<fact>.v<N>' format")
	ErrCrudVerbDisallowed     = errors.New("event type must describe an immutable domain fact and avoid CRUD verbs")
	ErrInvalidUUIDv7          = errors.New("event_id must be a valid UUIDv7 format")
	ErrCausalDepthExceeded    = errors.New("causation depth exceeds maximum threshold of 16")
	ErrCausalDepthNegative    = errors.New("causation depth cannot be negative")
	ErrInvalidAggregateSeq    = errors.New("aggregate version must be strictly positive")
	ErrOccConflict            = errors.New("aggregate version conflict detected")
	ErrAggregateTerminal      = errors.New("aggregate has emitted a terminal event and is closed")
	ErrInvalidEffectiveTime   = errors.New("effective_at timestamp cannot be zero or unset")
	ErrInvalidTransactionGroup= errors.New("transaction group size must be greater than zero")
)

var eventTypeRegex = regexp.MustCompile(`^[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.[a-z][a-z0-9\-]*\.v[0-9]+$`)

var bannedCrudSubstrings = []string{
	".created.",
	".updated.",
	".deleted.",
	".modified.",
	".orderupdated.",
	".customerupdated.",
}

func ValidateEventType(eventType string) error {
	trimmed := strings.TrimSpace(eventType)
	if !eventTypeRegex.MatchString(trimmed) {
		return ErrInvalidEventTypeFormat
	}
	lower := strings.ToLower(trimmed)
	for _, banned := range bannedCrudSubstrings {
		if strings.Contains(lower, banned) {
			return ErrCrudVerbDisallowed
		}
	}
	return nil
}

func ValidateUUIDv7(id string) error {
	parsed, err := uuid.Parse(strings.TrimSpace(id))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidUUIDv7, err)
	}
	if parsed.Version() != 7 && parsed.Version() != 4 {
		return ErrInvalidUUIDv7
	}
	return nil
}

func ValidateCausalDepth(depth int) error {
	if depth < 0 {
		return ErrCausalDepthNegative
	}
	if depth > 16 {
		return ErrCausalDepthExceeded
	}
	return nil
}

func ComputeCausalPathHash(eventID string, causationID string, depth int) string {
	raw := fmt.Sprintf("%s:%s:%d", strings.TrimSpace(eventID), strings.TrimSpace(causationID), depth)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])
}

func ValidateTimestamps(effectiveAt time.Time, recordedAt time.Time) error {
	if effectiveAt.IsZero() {
		return ErrInvalidEffectiveTime
	}
	if recordedAt.IsZero() {
		return errors.New("recorded_at timestamp cannot be zero")
	}
	return nil
}

func ValidateTransactionGroup(groupID string, size int) error {
	trimmed := strings.TrimSpace(groupID)
	if trimmed != "" && size <= 0 {
		return ErrInvalidTransactionGroup
	}
	return nil
}

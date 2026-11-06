package rules

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"event-driven-platform/src/features/distributed-tracing/contracts"
)

var (
	ErrInvalidTraceparentFormat = errors.New("traceparent must conform to W3C format '00-32hex-16hex-02hex'")
	ErrTraceIDZero              = errors.New("trace_id cannot be all zeros")
	ErrSpanIDZero               = errors.New("span_id cannot be all zeros")
)

var traceparentRegex = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$`)

var sensitivePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(bearer\s+[a-z0-9_\-\.]+)`),
	regexp.MustCompile(`(?i)(password["']?\s*[:=]\s*["']?[^"'\s]+)`),
	regexp.MustCompile(`\b(?:\d{4}[-\s]?){3}\d{4}\b`),
}

func ParseW3CTraceparent(header string) (contracts.W3CTraceContext, error) {
	trimmed := strings.ToLower(strings.TrimSpace(header))
	matches := traceparentRegex.FindStringSubmatch(trimmed)
	if len(matches) != 4 {
		return contracts.W3CTraceContext{}, ErrInvalidTraceparentFormat
	}

	traceID := matches[1]
	spanID := matches[2]
	flags := matches[3]

	if traceID == strings.Repeat("0", 32) {
		return contracts.W3CTraceContext{}, ErrTraceIDZero
	}
	if spanID == strings.Repeat("0", 16) {
		return contracts.W3CTraceContext{}, ErrSpanIDZero
	}

	return contracts.W3CTraceContext{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
	}, nil
}

func FormatW3CTraceparent(ctx contracts.W3CTraceContext) string {
	flags := ctx.TraceFlags
	if flags == "" {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", ctx.TraceID, ctx.SpanID, flags)
}

func SanitizeAttributes(attributes map[string]string) string {
	var parts []string
	for k, v := range attributes {
		sanitized := v
		for _, re := range sensitivePatterns {
			sanitized = re.ReplaceAllString(sanitized, "[REDACTED]")
		}
		parts = append(parts, fmt.Sprintf("%q:%q", k, sanitized))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func ClassifyLinkingMode(isBatch bool, queueWaitMillis int64) (string, bool, bool) {
	if isBatch {
		return "SPAN_LINK_BATCH", true, false
	}
	if queueWaitMillis > 300000 {
		return "SPAN_LINK_DELAYED", true, false
	}
	return "CHILD_SPAN_DIRECT", false, true
}

func EvaluateDeterministicHash(correlationID string, sampleRate float64) bool {
	if sampleRate >= 1.0 {
		return true
	}
	if sampleRate <= 0.0 {
		return false
	}
	hash := sha256.Sum256([]byte(strings.TrimSpace(correlationID)))
	val := binary.BigEndian.Uint32(hash[:4])
	normalized := float64(val) / float64(0xFFFFFFFF)
	return normalized <= sampleRate
}


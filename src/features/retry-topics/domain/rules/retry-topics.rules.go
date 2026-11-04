package rules

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
)

func DeriveDeterministicIdempotencyKey(handlerID, eventID, effectName string) string {
	raw := fmt.Sprintf("%s:%s:%s", handlerID, eventID, effectName)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:16])
}

func CalculateJitteredBackoff(attempt int, baseMs, maxMs int64) int64 {
	if attempt < 1 {
		attempt = 1
	}
	if baseMs <= 0 {
		baseMs = 1000
	}
	if maxMs <= 0 {
		maxMs = 60000
	}

	multiplier := math.Pow(2, float64(attempt-1))
	computed := float64(baseMs) * multiplier
	if computed > float64(maxMs) {
		computed = float64(maxMs)
	}

	jitterRange := int64(computed * 0.2)
	if jitterRange < 1 {
		jitterRange = 1
	}

	n, err := rand.Int(rand.Reader, big.NewInt(jitterRange*2))
	var jitter int64 = 0
	if err == nil {
		jitter = n.Int64() - jitterRange
	}

	result := int64(computed) + jitter
	if result < baseMs {
		result = baseMs
	}
	if result > maxMs {
		result = maxMs
	}
	return result
}

func EvaluateRetryDecision(input contracts.EvaluateRetryInput, now time.Time) contracts.EvaluateRetryResult {
	if input.Classification == contracts.ClassEnvironmental || input.CircuitOpen {
		return contracts.EvaluateRetryResult{
			Disposition: contracts.DispositionPauseCircuit,
			BackoffMs:   0,
			DueAt:       now,
			Reason:      "environmental error or open circuit breaker requires partition pause without burning attempt budget",
		}
	}

	if input.Classification == contracts.ClassContention {
		return contracts.EvaluateRetryResult{
			Disposition: contracts.DispositionImmediateRetry,
			BackoffMs:   10,
			DueAt:       now.Add(10 * time.Millisecond),
			Reason:      "database contention or optimistic lock conflict allows immediate in-place retry",
		}
	}

	if input.Classification == contracts.ClassPermanentData || input.Classification == contracts.ClassBusinessReject {
		return contracts.EvaluateRetryResult{
			Disposition: contracts.DispositionDeadLetter,
			BackoffMs:   0,
			DueAt:       now,
			Reason:      "permanent schema invalidity or domain rejection cannot succeed upon retry and routes directly to DLQ",
		}
	}

	if input.AttemptNumber >= input.MaxAttempts {
		return contracts.EvaluateRetryResult{
			Disposition: contracts.DispositionDeadLetter,
			BackoffMs:   0,
			DueAt:       now,
			Reason:      "maximum retry attempts exhausted",
		}
	}

	backoff := CalculateJitteredBackoff(input.AttemptNumber, input.BaseBackoffMs, input.MaxBackoffMs)
	if input.Classification == contracts.ClassRateLimited && input.RetryAfterMs > 0 {
		backoff = input.RetryAfterMs
	}

	dueAt := now.Add(time.Duration(backoff) * time.Millisecond)

	if input.ValidUntil != nil && !input.ValidUntil.IsZero() {
		if dueAt.After(*input.ValidUntil) {
			return contracts.EvaluateRetryResult{
				Disposition: contracts.DispositionExpired,
				BackoffMs:   0,
				DueAt:       now,
				Reason:      "event validity deadline breached before next retry attempt can execute",
			}
		}
	}

	if input.StreamOrdering == contracts.OrderingStrict || input.StreamOrdering == contracts.OrderingState {
		return contracts.EvaluateRetryResult{
			Disposition: contracts.DispositionHeldScheduled,
			BackoffMs:   backoff,
			DueAt:       dueAt,
			Reason:      "ordered streams require held-key scheduled retry to preserve causal delta sequence",
		}
	}

	return contracts.EvaluateRetryResult{
		Disposition: contracts.DispositionTopicScheduled,
		BackoffMs:   backoff,
		DueAt:       dueAt,
		Reason:      "unordered stream allows progressive retry topic routing",
	}
}

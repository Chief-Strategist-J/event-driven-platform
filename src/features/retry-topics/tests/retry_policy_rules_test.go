package tests

import (
	"testing"
	"time"

	"event-driven-platform/src/features/retry-topics/contracts"
	"event-driven-platform/src/features/retry-topics/domain/machines"
	"event-driven-platform/src/features/retry-topics/domain/rules"
)


func TestRetryPolicy_Tier0_EvaluateRetryDecision(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	futureDeadline := now.Add(5 * time.Minute)
	pastDeadline := now.Add(-1 * time.Minute)
	tightDeadline := now.Add(500 * time.Millisecond)

	tests := []struct {
		name            string
		input           contracts.EvaluateRetryInput
		wantDisposition contracts.RetryDisposition
		wantZeroBackoff bool
		wantMinBackoff  int64
	}{
		{
			name: "Environmental error pauses circuit and preserves budget",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingStrict,
				Classification: contracts.ClassEnvironmental,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionPauseCircuit,
			wantZeroBackoff: true,
		},
		{
			name: "Open circuit breaker forces pause circuit",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingNone,
				Classification: contracts.ClassMessageTransient,
				CircuitOpen:    true,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionPauseCircuit,
			wantZeroBackoff: true,
		},
		{
			name: "Contention error allows immediate in-place retry with 10ms backoff",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingStrict,
				Classification: contracts.ClassContention,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionImmediateRetry,
			wantMinBackoff:  10,
		},
		{
			name: "Permanent data error routes directly to Dead Letter Queue",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingStrict,
				Classification: contracts.ClassPermanentData,
				AttemptNumber:  1,
				MaxAttempts:    5,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionDeadLetter,
			wantZeroBackoff: true,
		},
		{
			name: "Business domain rejection routes directly to Dead Letter Queue",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingNone,
				Classification: contracts.ClassBusinessReject,
				AttemptNumber:  1,
				MaxAttempts:    5,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionDeadLetter,
			wantZeroBackoff: true,
		},
		{
			name: "Exhausted attempts equal to MaxAttempts routes to Dead Letter Queue",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingStrict,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  3,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionDeadLetter,
			wantZeroBackoff: true,
		},
		{
			name: "Exhausted attempts greater than MaxAttempts routes to Dead Letter Queue",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingNone,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  4,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionDeadLetter,
			wantZeroBackoff: true,
		},
		{
			name: "Rate-limited error honors upstream RetryAfterMs override",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingNone,
				Classification: contracts.ClassRateLimited,
				RetryAfterMs:   5000,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionTopicScheduled,
			wantMinBackoff:  5000,
		},
		{
			name: "Ordered stream strictly enforces held-key scheduled retry",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingStrict,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
				ValidUntil:     &futureDeadline,
			},
			wantDisposition: contracts.DispositionHeldScheduled,
			wantMinBackoff:  1000,
		},
		{
			name: "State-ordered stream also mandates held-key scheduled retry",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingState,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
				ValidUntil:     &futureDeadline,
			},
			wantDisposition: contracts.DispositionHeldScheduled,
			wantMinBackoff:  1000,
		},
		{
			name: "Unordered stream allows progressive retry topic scheduling",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingNone,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  1000,
				MaxBackoffMs:   10000,
			},
			wantDisposition: contracts.DispositionTopicScheduled,
			wantMinBackoff:  1000,
		},
		{
			name: "Expired valid_until deadline marks disposition expired",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingStrict,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  2000,
				MaxBackoffMs:   10000,
				ValidUntil:     &pastDeadline,
			},
			wantDisposition: contracts.DispositionExpired,
			wantZeroBackoff: true,
		},
		{
			name: "Tight valid_until deadline breached by computed backoff marks expired",
			input: contracts.EvaluateRetryInput{
				StreamOrdering: contracts.OrderingNone,
				Classification: contracts.ClassMessageTransient,
				AttemptNumber:  1,
				MaxAttempts:    3,
				BaseBackoffMs:  2000,
				MaxBackoffMs:   10000,
				ValidUntil:     &tightDeadline,
			},
			wantDisposition: contracts.DispositionExpired,
			wantZeroBackoff: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := rules.EvaluateRetryDecision(tc.input, now)
			if res.Disposition != tc.wantDisposition {
				t.Fatalf("expected disposition %s, got %s (reason: %s)", tc.wantDisposition, res.Disposition, res.Reason)
			}
			if tc.wantZeroBackoff && res.BackoffMs != 0 {
				t.Fatalf("expected zero backoff, got %d", res.BackoffMs)
			}
			if tc.wantMinBackoff > 0 && res.BackoffMs < tc.wantMinBackoff {
				t.Fatalf("expected at least %d backoff, got %d", tc.wantMinBackoff, res.BackoffMs)
			}
		})
	}
}


func TestRetryPolicy_Tier0_StateMachineTransitions(t *testing.T) {
	tests := []struct {
		name      string
		current   contracts.RetryAttemptStatus
		next      contracts.RetryAttemptStatus
		wantState contracts.RetryAttemptStatus
		wantErr   bool
	}{
		{
			name:      "SCHEDULED -> RETRYING valid",
			current:   contracts.StatusScheduled,
			next:      contracts.StatusRetrying,
			wantState: contracts.StatusRetrying,
			wantErr:   false,
		},
		{
			name:      "SCHEDULED -> EXPIRED valid",
			current:   contracts.StatusScheduled,
			next:      contracts.StatusExpired,
			wantState: contracts.StatusExpired,
			wantErr:   false,
		},
		{
			name:      "SCHEDULED -> RESOLVED invalid conflict",
			current:   contracts.StatusScheduled,
			next:      contracts.StatusResolved,
			wantState: contracts.StatusScheduled,
			wantErr:   true,
		},
		{
			name:      "RETRYING -> RESOLVED valid terminal",
			current:   contracts.StatusRetrying,
			next:      contracts.StatusResolved,
			wantState: contracts.StatusResolved,
			wantErr:   false,
		},
		{
			name:      "RETRYING -> SCHEDULED valid next attempt",
			current:   contracts.StatusRetrying,
			next:      contracts.StatusScheduled,
			wantState: contracts.StatusScheduled,
			wantErr:   false,
		},
		{
			name:      "RETRYING -> EXHAUSTED valid exhaustion",
			current:   contracts.StatusRetrying,
			next:      contracts.StatusExhausted,
			wantState: contracts.StatusExhausted,
			wantErr:   false,
		},
		{
			name:      "RESOLVED -> RETRYING invalid modification of terminal state",
			current:   contracts.StatusResolved,
			next:      contracts.StatusRetrying,
			wantState: contracts.StatusResolved,
			wantErr:   true,
		},
		{
			name:      "EXHAUSTED -> SCHEDULED invalid resurrection",
			current:   contracts.StatusExhausted,
			next:      contracts.StatusScheduled,
			wantState: contracts.StatusExhausted,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, err := machines.TransitionRetryStatus(tc.current, tc.next)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error transitioning %s -> %s, got nil", tc.current, tc.next)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if st != tc.wantState {
				t.Fatalf("expected state %s, got %s", tc.wantState, st)
			}
		})
	}
}


func BenchmarkRetryPolicy_EvaluateRetryDecision(b *testing.B) {
	now := time.Now().UTC()
	input := contracts.EvaluateRetryInput{
		StreamOrdering: contracts.OrderingStrict,
		Classification: contracts.ClassMessageTransient,
		AttemptNumber:  2,
		MaxAttempts:    5,
		BaseBackoffMs:  1000,
		MaxBackoffMs:   30000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res := rules.EvaluateRetryDecision(input, now)
		if res.Disposition != contracts.DispositionHeldScheduled {
			b.Fatalf("benchmark failed unexpected disposition: %s", res.Disposition)
		}
	}
}

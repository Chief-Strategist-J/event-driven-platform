package machines

import (
	"event-driven-platform/src/features/distributed-tracing/contracts"
	"event-driven-platform/src/features/distributed-tracing/domain/rules"
)

type SamplingStateMachine struct{}

func NewSamplingStateMachine() *SamplingStateMachine {
	return &SamplingStateMachine{}
}

func (m *SamplingStateMachine) Evaluate(
	policy contracts.TraceSamplingPolicy,
	cmd contracts.EvaluateSamplingCommand,
) contracts.EvaluateSamplingResult {
	if cmd.HasError && policy.AlwaysSampleErrors {
		return contracts.EvaluateSamplingResult{
			ShouldSample: true,
			SampleRate:   1.0,
			DecisionRule: "ERROR_TAIL_SAMPLING_OVERRIDE",
		}
	}

	if policy.MinDurationMs > 0 && cmd.DurationMs >= policy.MinDurationMs {
		return contracts.EvaluateSamplingResult{
			ShouldSample: true,
			SampleRate:   1.0,
			DecisionRule: "LATENCY_THRESHOLD_OVERRIDE",
		}
	}

	shouldSample := rules.EvaluateDeterministicHash(cmd.CorrelationID, policy.SampleRate)
	return contracts.EvaluateSamplingResult{
		ShouldSample: shouldSample,
		SampleRate:   policy.SampleRate,
		DecisionRule: "DETERMINISTIC_CORRELATION_HASH",
	}
}


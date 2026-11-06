package ports

import (
	"context"

	"event-driven-platform/src/features/distributed-tracing/contracts"
)

type DistributedTracingRepositoryPort interface {
	SaveSamplingPolicy(ctx context.Context, policy contracts.TraceSamplingPolicy) error
	GetSamplingPolicy(ctx context.Context, tenantID string, serviceName string) (contracts.TraceSamplingPolicy, error)
	RecordSpanAudit(ctx context.Context, audit contracts.TraceSpanAudit) error
	UpsertPropagationCoverage(ctx context.Context, cov contracts.TracePropagationCoverage) error
}

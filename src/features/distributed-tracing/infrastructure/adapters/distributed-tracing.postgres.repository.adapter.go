package adapters

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"event-driven-platform/src/features/distributed-tracing/contracts"
	"event-driven-platform/src/features/distributed-tracing/infrastructure/ports"
	"event-driven-platform/src/features/distributed-tracing/infrastructure/queries"
)

var (
	ErrPolicyNotFound = errors.New("trace sampling policy not found")
)

type PostgresDistributedTracingAdapter struct {
	db *sql.DB
}

func NewPostgresDistributedTracingAdapter(db *sql.DB) ports.DistributedTracingRepositoryPort {
	return &PostgresDistributedTracingAdapter{db: db}
}

func (a *PostgresDistributedTracingAdapter) SaveSamplingPolicy(
	ctx context.Context,
	policy contracts.TraceSamplingPolicy,
) error {
	tr := otel.Tracer("distributed-tracing")
	ctx, span := tr.Start(ctx, "PostgresDistributedTracingAdapter.SaveSamplingPolicy")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.InsertSamplingPolicy,
		policy.ID,
		policy.PublicID,
		policy.TenantID,
		policy.SourceService,
		policy.ExternalID,
		policy.CorrelationID,
		policy.ServiceName,
		policy.FlowName,
		policy.SampleRate,
		policy.Priority,
		policy.AlwaysSampleErrors,
		policy.MinDurationMs,
		policy.IsActive,
		now,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (a *PostgresDistributedTracingAdapter) GetSamplingPolicy(
	ctx context.Context,
	tenantID string,
	serviceName string,
) (contracts.TraceSamplingPolicy, error) {
	tr := otel.Tracer("distributed-tracing")
	ctx, span := tr.Start(ctx, "PostgresDistributedTracingAdapter.GetSamplingPolicy")
	defer span.End()

	if a.db == nil {
		return contracts.TraceSamplingPolicy{
			TenantID:           tenantID,
			ServiceName:        serviceName,
			SampleRate:         1.0,
			AlwaysSampleErrors: true,
			IsActive:           true,
		}, nil
	}

	var policy contracts.TraceSamplingPolicy
	err := a.db.QueryRowContext(ctx, queries.SelectSamplingPolicy, tenantID, serviceName).Scan(
		&policy.ID,
		&policy.PublicID,
		&policy.TenantID,
		&policy.SourceService,
		&policy.ExternalID,
		&policy.CorrelationID,
		&policy.ServiceName,
		&policy.FlowName,
		&policy.SampleRate,
		&policy.Priority,
		&policy.AlwaysSampleErrors,
		&policy.MinDurationMs,
		&policy.IsActive,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.TraceSamplingPolicy{
				TenantID:           tenantID,
				ServiceName:        serviceName,
				SampleRate:         1.0,
				AlwaysSampleErrors: true,
				IsActive:           true,
			}, nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.TraceSamplingPolicy{}, err
	}
	return policy, nil
}

func (a *PostgresDistributedTracingAdapter) RecordSpanAudit(
	ctx context.Context,
	audit contracts.TraceSpanAudit,
) error {
	tr := otel.Tracer("distributed-tracing")
	ctx, span := tr.Start(ctx, "PostgresDistributedTracingAdapter.RecordSpanAudit")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.InsertTraceSpanAudit,
		audit.ID,
		audit.PublicID,
		audit.TenantID,
		audit.SourceService,
		audit.ExternalID,
		audit.CorrelationID,
		audit.TraceID,
		audit.SpanID,
		audit.ParentSpanID,
		audit.ServiceName,
		audit.OperationName,
		audit.DurationMs,
		audit.HasError,
		audit.SanitizedAttributesJSON,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (a *PostgresDistributedTracingAdapter) UpsertPropagationCoverage(
	ctx context.Context,
	cov contracts.TracePropagationCoverage,
) error {
	tr := otel.Tracer("distributed-tracing")
	ctx, span := tr.Start(ctx, "PostgresDistributedTracingAdapter.UpsertPropagationCoverage")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.UpsertPropagationCoverage,
		cov.ID,
		cov.PublicID,
		cov.TenantID,
		cov.SourceService,
		cov.ExternalID,
		cov.CorrelationID,
		cov.HopSource,
		cov.HopTarget,
		string(cov.HopType),
		cov.TotalSpans,
		cov.PropagatedSpans,
		cov.CoverageRatio,
		now,
		now,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

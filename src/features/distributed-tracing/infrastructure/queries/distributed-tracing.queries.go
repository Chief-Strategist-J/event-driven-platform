package queries

const InsertSamplingPolicy = `
INSERT INTO trace_sampling_policy_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    service_name, flow_name, sample_rate, priority, always_sample_errors, min_duration_ms,
    is_active, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, service_name, flow_name) DO UPDATE SET
    sample_rate = EXCLUDED.sample_rate,
    priority = EXCLUDED.priority,
    always_sample_errors = EXCLUDED.always_sample_errors,
    min_duration_ms = EXCLUDED.min_duration_ms,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at;
`

const SelectSamplingPolicy = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       service_name, flow_name, sample_rate, priority, always_sample_errors, min_duration_ms,
       is_active, created_at, updated_at
FROM trace_sampling_policy_ledger
WHERE tenant_id = $1 AND service_name = $2 AND is_active = TRUE
ORDER BY priority ASC
LIMIT 1;
`

const InsertTraceSpanAudit = `
INSERT INTO trace_span_audit_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    trace_id, span_id, parent_span_id, service_name, operation_name, duration_ms,
    has_error, sanitized_attributes_json, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, trace_id, span_id) DO NOTHING;
`

const UpsertPropagationCoverage = `
INSERT INTO trace_propagation_coverage_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    hop_source, hop_target, hop_type, total_spans, propagated_spans, coverage_ratio,
    evaluated_at, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, hop_source, hop_target, hop_type) DO UPDATE SET
    total_spans = trace_propagation_coverage_ledger.total_spans + EXCLUDED.total_spans,
    propagated_spans = trace_propagation_coverage_ledger.propagated_spans + EXCLUDED.propagated_spans,
    coverage_ratio = CASE 
        WHEN (trace_propagation_coverage_ledger.total_spans + EXCLUDED.total_spans) > 0 
        THEN ROUND(((trace_propagation_coverage_ledger.propagated_spans + EXCLUDED.propagated_spans)::numeric / 
                    (trace_propagation_coverage_ledger.total_spans + EXCLUDED.total_spans)::numeric), 4)
        ELSE 1.0000 
    END,
    evaluated_at = EXCLUDED.evaluated_at,
    updated_at = EXCLUDED.updated_at;
`

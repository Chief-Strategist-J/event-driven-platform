# Architectural Decision Record (ADR): Event Authorization Engine

## Status
Accepted

## Context
In a distributed event-driven microservices architecture, services exchange sensitive data across message brokers. Without a strict, declarative authorization engine, unauthorized producers can forge events or publish to topics outside their domain boundary, and rogue consumers can read sensitive events or join unauthorized consumer groups. Furthermore, even with topic-level ACLs, consumers may lack data-governance clearances (e.g. `view_pii`) requiring fine-grained policy-based enforcement.

## Decision
1. **Policy-Based Access Control (PBAC/RBAC)**: Model authorization policies using declarative rules that bind principals (workloads/users) to resources (`TOPIC`, `GROUP`, `FIELD`) and operations (`PUBLISH`, `SUBSCRIBE`, `VIEW_PII`, `MANAGE`).
2. **PostgreSQL Policy Ledgers**:
   - `auth_policy_ledger`: Stores tenant-scoped authorization policies with status `ACTIVE`, `DISABLED`, or `REVOKED`.
   - `auth_audit_log`: Append-only, tamper-evident log capturing every authorization decision (`ALLOWED` or `DENIED`), including request attributes, matched policy public ID, and evaluation latency.
3. **Fail-Closed Principle**: Any request without an explicit, active `ALLOW` policy is rejected with `DENIED` (`ErrAccessDenied`).
4. **OpenTelemetry Tracing Compliance**: Every evaluation generates dedicated spans (`EventAuthorizationService.AuthorizePublish`, `EventAuthorizationService.AuthorizeSubscribe`, `EventAuthorizationService.EvaluateAccess`) annotated with principal, resource, operation, and outcome attributes.
5. **Zero-Null Schema Invariant**: All policy and audit tables strictly enforce `NOT NULL` constraints, valid regex identifiers (`pol_[0-9a-z]{16,26}`), and check constraints per `05-db-writing-rules.md`.

## Consequences
- **Positive**: Strict least-privilege boundary enforcement across event production and consumption; audit logs satisfy SOC2 and ISO27001 audit requirements.
- **Trade-off**: Evaluation adds sub-millisecond overhead to event dispatch; mitigated by efficient indexing and in-memory policy caching where appropriate.

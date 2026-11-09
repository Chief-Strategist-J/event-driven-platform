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

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

In multi-tenant event brokers, unauthorized consumers can subscribe to wildcard topics or unauthorized producers can inject forged events into payment topics. Static network firewalls and coarse-grained IAM roles cannot inspect dynamic message headers, tenant boundaries, or payload classifications, leading to catastrophic cross-tenant data leaks and privilege escalation.

### 2. Internal Algorithmic Mechanics & State Invariants

Every incoming event publication and consumption request passes through an Attribute-Based Access Control (ABAC) evaluation gate. The gate inspects the caller's cryptographically signed JWT claims, extracts the tenant context, and matches it against topic security policies and payload classifications. If a principal attempts to emit or consume an event outside their authorized tenant scope, the gate rejects the request immediately.

### 3. Core Architectural Principles

* **Attribute-Based Access Control (ABAC)**: Authorization decisions evaluate dynamic context—subject claims, tenant boundaries, topic classifications—rather than static roles.
* **Zero-Trust Ingress Interception**: All event entrypoints enforce cryptographically verified authentication before routing messages to application logic.
* **Rigid Tenant Scope Fencing**: Cross-tenant event interaction is strictly forbidden; mismatches between caller identity and tenant payload fail immediately.
* **Auditable Security Telemetry**: Security denials generate tamper-evident audit records capturing caller identity, target topic, and timestamp without payload leakage.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is Role-Based Access Control (RBAC) insufficient for multi-tenant streaming? (RBAC states 'User is Admin', but cannot evaluate dynamic contextual rules like 'User can only read Orders where tenant_id == caller.tenant_id').**
2. **How do we prevent identity spoofing in asynchronous event chains? (The ingress gateway validates the cryptographic signature of the JWT, binds the tenant_id into an immutable context header, and signs the internal envelope).**
3. **What is the performance overhead of authorizing every message? (Access policies and public keys are cached locally with fast TTL invalidation; token signature verification executes in under 20 microseconds without database roundtrips).**

---

## Authoritative References & Production Citations

* **[NIST Special Publication 800-162: Guide to Attribute Based Access Control (ABAC)](https://csrc.nist.gov/publications/detail/sp/800-162/final)**
  - *Architectural Rationale & Implementation Grounding*: Federal standard defining architectural components (PEP, PDP, PAP) and policy evaluation models for enterprise ABAC.
* **[RFC 7519: JSON Web Token (JWT) Specification](https://www.rfc-editor.org/rfc/rfc7519)**
  - *Architectural Rationale & Implementation Grounding*: Industry standard for compact, URL-safe means of representing claims to be transferred between two parties in distributed systems.
* **[OWASP API Security Top 10: Broken Object Level Authorization (BOLA)](https://owasp.org/www-project-api-security/)**
  - *Architectural Rationale & Implementation Grounding*: Comprehensive guidance on preventing unauthorized multi-tenant data access and parameter tampering vulnerabilities.
* **[Open Policy Agent (OPA) Documentation](https://www.openpolicyagent.org/docs/latest/)**
  - *Architectural Rationale & Implementation Grounding*: Industry standard for declarative, unified policy enforcement across microservices and streaming platforms.


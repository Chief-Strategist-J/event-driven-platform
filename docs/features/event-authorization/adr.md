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

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Decentralized Policy Enforcement at Consumer Ingress
* **Design Philosophy & Epistemology**:
  In an asynchronous pub/sub architecture, the producer cannot know or authorize downstream consumers. Relying on a centralized authorization server during message consumption creates an availability bottleneck that breaks during network partitions. The philosophy is Cryptographic Self-Contained Authorization: authorization claims are embedded cryptographically in signed tokens, evaluated locally by consumer middleware.
* **Threat Vector Defeated**: Cascading platform outages caused by centralized identity provider (IdP) timeouts during high-throughput event processing.
* **Storage & State Invariant**:
```text
VerifySignature(Token, PublicKey) == TRUE
EvaluatePolicy(Token.Claims, RequiredScope) == PERMIT
CentralIdP_NetworkCalls == 0 during active message processing
```
* **Implementation Mechanism**:
  Producers inject signed JWT / PASETO security tokens into message headers. Consumer middleware verifies the token signature against cached public keys and evaluates local Open Policy Agent (OPA) / Rego policies in RAM without making external network calls.
* **Non-Negotiable Trade-Off**: Token expiration latency. If a user's permissions are revoked in the IdP, existing messages with valid unexpired tokens will be processed until the token expires.

---

#### Principle 2: Broker-Level Access Control (mTLS + SASL SCRAM)
* **Design Philosophy & Epistemology**:
  Application-level authorization is useless if an unauthorized service can connect directly to the message broker and subscribe to sensitive topics. The philosophy is Defense in Depth at the Transport Layer: the broker itself must authenticate every connecting client and enforce strict topic-level Read/Write Access Control Lists (ACLs).
* **Threat Vector Defeated**: Rogue microservices or compromised developer machines connecting to production brokers and exfiltrating financial or customer data streams.
* **Storage & State Invariant**:
```text
Broker.Authenticate(ClientCertificate) == TRUE
Broker.CheckACL(Principal, Topic, Operation) == ALLOW
```
* **Implementation Mechanism**:
  All Kafka connections require mutual TLS (mTLS) with client certificates issued by an internal PKI. Kafka ACLs enforce that only specific producer service principals can write to a topic, and only specific consumer service principals can read from it.
* **Non-Negotiable Trade-Off**: PKI certificate lifecycle management and automated certificate rotation complexity across Kubernetes pods.

---

#### Principle 3: Tenant Boundary Isolation & Context Guarding
* **Design Philosophy & Epistemology**:
  In a multi-tenant event platform, the greatest catastrophic failure is cross-tenant data leakage. If Tenant A's event is processed by Tenant B's context, regulatory compliance and customer trust are destroyed. The philosophy is Inflexible Tenant Fencing: the tenant identifier is an immutable security boundary stamped into the message envelope and enforced at every layer.
* **Threat Vector Defeated**: Cross-tenant data leakage where bugs in consumer processing logic apply Tenant A's event mutations to Tenant B's database records.
* **Storage & State Invariant**:
```text
Event.TenantID == ExecutionContext.TenantID == DatabaseRow.TenantID
CrossTenantMutationAttempt ==> Immediate Panic / Security Alert
```
* **Implementation Mechanism**:
  Platform consumer middleware extracts `tenant_id` from message headers, binds it to the thread-local / Go context, and sets PostgreSQL session variables (`SET LOCAL app.current_tenant = $1`) to activate database Row-Level Security (RLS) automatically.
* **Non-Negotiable Trade-Off**: Added latency of configuring tenant-specific database session variables on each consumed message batch.

---

#### Principle 4: Auditability of Authorization Decisions
* **Design Philosophy & Epistemology**:
  A security system that silently permits or rejects actions without leaving an immutable audit trail cannot pass security compliance audits. The philosophy is Forensic Non-Repudiation: every authorization decision—especially rejections—must be permanently recorded with full context.
* **Threat Vector Defeated**: Undetected privilege escalation attacks and unauthorized access attempts slipping past security operations teams.
* **Storage & State Invariant**:
```text
AuthDecision(Reject) ==> EmitSecurityAuditAlert(Principal, Topic, Action, Reason, Timestamp)
```
* **Implementation Mechanism**:
  When consumer middleware or broker ACLs reject an unauthorized message, an immutable audit event is immediately published to a dedicated `security-alerts` topic, triggering alerts in the Security Operations Center (SOC).
* **Non-Negotiable Trade-Off**: Potential alert noise during service deployment rollouts if access policies are updated out of sync with application deployments.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is it dangerous to rely solely on the producer service to enforce authorization?**
   * *Answer*: Producers can be compromised, misconfigured, or bypassed. The consumer is the sovereign owner of its database and business domain; it must independently verify that incoming events possess the legitimate authority to trigger local state changes.
2. **How does the system revoke authorization for a compromised service in an asynchronous event pipeline?**
   * *Answer*: Revocation is enforced at two levels: the broker coordinator revokes the service's mTLS certificate and SASL credentials to cut connection access immediately; consumers update their local OPA policy cache to immediately reject any messages bearing the compromised principal ID.
3. **What happens if a message arrives with a valid cryptographic token that has expired during a 4-hour broker backlog delay?**
   * *Answer*: Consumers distinguish between 'token issuance time' and 'event creation time'. If the token was valid when the producer committed the event, the consumer verifies historical validity against the event timestamp while ensuring the message was not replayed.

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


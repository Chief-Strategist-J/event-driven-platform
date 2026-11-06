# Multi-Tenant Event Isolation — Failure Catalog & Mitigation Strategy

This catalog documents failure modes across the multi-tenant event lifecycle, derived from the business logic decision tree.

---

## 1. Severity Taxonomy

- **LOSS**: Irreversible loss of tenant event data or premature destruction of active encryption keys.
- **CORRUPT**: Silent state divergence, cross-tenant record leakage, or mutated historical payloads.
- **OUTAGE**: Platform-wide unavailability caused by noisy neighbor saturation or metadata explosion.
- **DEGRADE**: Elevated consumer lag, broker page-cache thrashing, or depleted KMS rate limits.
- **COMPLIANCE**: Breach of GDPR/CCPA erasure SLAs, cross-border data residency violations, or unredacted PII in logs.
- **SECURITY**: Cross-tenant data exfiltration, spoofed identity claims, or broken row-level security.

---

## 2. Failure Nodes Catalog

### Category 2: Tenancy Model Selection & Topic Topology

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F2.1** | Per-event-type topics per tenant | **OUTAGE** | Creating dedicated topics per event type explodes broker metadata and partition counts. | Enforce aggregate-stream topics only (`<domain>.<aggregate>.v<major>`). | Kafka cluster partition count monitor. |
| **F2.2** | Tenant tier changes treated as config flip | **CORRUPT** | Flipping a tenant from pooled to dedicated topics mid-stream causes out-of-order execution and duplicates. | Treat tier changes as stream migrations (`MT-13`): dual-publish from outbox and cut over per aggregate version. | Event sequence ordering validator. |
| **F2.3** | Regex subscriptions pick up other tenant topics | **SECURITY** | Consumers subscribing to `t.*` ingest topics of unintended tenants. | Explicit subscription sets driven strictly by tenant registry configuration and Kafka ACLs. | Consumer group subscription audit alerts. |

---

### Category 3: Tenant Identity & Async Propagation

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F3.1** | Client-supplied tenant header trusted | **SECURITY** | Edge proxy trusts unverified `x-tenant-id` header from client request. | Enforce `MT-01`: server-side token validation extracts tenant from verified claims. Reject client headers. | Security gateway audit filter. |
| **F3.2** | Ambient context bleed across worker lanes | **SECURITY** | Reusing worker goroutine context causes events to be processed under the previous tenant's identity. | Enforce `MT-05`: instantiate immutable per-record scope; write-time guard validates DB write tenant against record scope. | Interleaved lane concurrency tests. |
| **F3.3** | Support/admin tools act "as tenant" without audit | **SECURITY** | Internal support tools bypass tenant authentication to query customer records. | Impersonation is modeled as an audited mode requiring explicit reason, ticket reference, and short-lived expiry. | SIEM privilege escalation alert. |
| **F3.4** | Aggregate moved between tenants in place | **CORRUPT** | Updating tenant column on an existing aggregate row breaks referential integrity and audit trails. | In-place tenant mutation is forbidden. Model as `CloseInTenantA` + `CreateInTenantB` with explicit lineage. | PostgreSQL immutability update trigger. |
| **F3.5** | Legacy events missing tenant identifier | **CORRUPT** | Upstream system emits events without tenant metadata. | Fail closed: reject at outbox staging. Never default missing tenants to a fallback tenant. | Outbox rejection metric counter. |

---

### Category 4: Partitioning & Ordering Per Tenant

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F4.1** | Inconsistent partition key format | **CORRUPT** | Different producer microservices use divergent key separators (e.g. `:` vs `/`), splitting partition routing. | Centralized key builder library enforces canonical format: `tenant_id + ":" + aggregate_id`. | Key format validation assertion. |
| **F4.2** | Hot tenant saturates shared partition | **DEGRADE** | A high-volume tenant floods a shared partition, starving other tenants assigned to the same partition. | Enforce `MT-11`: token-bucket rate limits throttle rogue tenant; migrate sustained high-volume tenants to `M-DEDICATED`. | Per-partition tenant skew metrics. |
| **F4.3** | PII placed in partition key or headers | **COMPLIANCE** | Storing email addresses or customer names in keys survives cryptographic key shredding. | Enforce `MT-09`: partition keys and headers must contain only opaque, non-personal identifiers (UUIDs). | Static lint rule on event schemas. |

---

### Category 5: Access Control & Storage Isolation

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F5.1** | Relying on Kafka ACLs for pooled topic isolation | **SECURITY** | Assuming Kafka ACLs can filter records inside shared multi-tenant topics. | Kafka ACLs only protect topic boundaries. Services processing pooled topics must be reviewed as trusted kernels. | Architecture review compliance gate. |
| **F5.2** | RLS bypassed by superuser database role | **SECURITY** | Application connects to PostgreSQL using the database owner or superuser account, bypassing Row-Level Security. | Application roles strictly revoke superuser privileges; `FORCE ROW LEVEL SECURITY` enabled on all multi-tenant tables. | DB connection role audit probe. |
| **F5.3** | Search aggregations leak cross-tenant terms | **SECURITY** | Autocomplete or faceted search aggregations execute without mandatory tenant filter clauses. | Query rewrite layer mandates tenant filter for all aggregation and suggestion queries. | Synthetic search leak canary probes. |
| **F5.4** | Cache key collision across tenants | **SECURITY** | Unprefixed Redis/Memcached keys allow Tenant A to overwrite or read Tenant B's cached projections. | Cache wrapper library enforces `tnt:<tenant_id>:<key>` prefixing; rejects unprefixed keys. | Cache key format validator. |
| **F5.5** | Connection pool reuses previous session tenant | **SECURITY** | Pooled database connections retain session-level `SET app.current_tenant` from previous requests. | Enforce transaction-scoped settings (`SET LOCAL app.current_tenant = ...`); reset context on connection release. | Multi-tenant pool leak integration test. |

---

### Category 6: Consumer-Side Boundary Enforcement

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F6.1** | Silent drop on envelope mismatch | **LOSS** | Consumer encounters header/payload mismatch and drops the message to avoid panicking. | Enforce `MT-04`: silent drops are strictly banned. Route event to `INTEGRITY-TENANT` quarantine and raise SIEM page. | Dead-letter queue counter alert. |
| **F6.2** | Unknown tenant onboarding race | **DEGRADE** | Producer publishes events immediately after tenant signup before registry cache replicates to consumers. | Replicate registry metadata before activating tenant; apply short causal wait (500ms) before quarantining. | Tenant lookup retry metrics. |
| **F6.3** | Late event arrives for offboarded tenant | **COMPLIANCE** | Late webhook or historical replay re-inserts data for a contractually terminated tenant account. | Tenant lifecycle machine flags tenant `OFFBOARDED`; consumers skip and record tombstoned counts without mutating state. | Offboarded tenant event drop log. |

---

### Category 7: Noisy Neighbor & Quota Management

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F7.1** | Bulk import saturates consumer worker pool | **DEGRADE** | Tenant performs batch CSV ingest, exhausting all available worker threads. | Apply ingress throttling at source and route bulk workloads to dedicated low-priority consumer lanes. | Lane concurrency share metrics. |
| **F7.2** | Tenant halt implemented as partition pause | **OUTAGE** | Halting a rogue tenant pauses the Kafka partition, accidentally halting all innocent tenants sharing that partition. | Tenant halt parks the rogue tenant's keys in a held-key store; Kafka partitions continue streaming normally. | Partition pause detection monitor. |
| **F7.3** | High-cardinality tenant metrics crash Prometheus | **OUTAGE** | Emitting per-tenant Prometheus metrics for 100,000 tenants exhausts monitoring memory. | Maintain per-tenant metrics only for Top-N and VIP tenants; aggregate long-tail tenants into shared platform metrics. | Prometheus memory usage alerts. |

---

### Category 8: Data Residency & Sovereignty

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F8.1** | Pooled topic mirrored cross-region for DR | **COMPLIANCE** | Active-active replication copies EU/Swiss sovereign tenant events into a US data center. | Residency controller verifies regional allowlists; residency-bound tenants must use `M-SILO` or regional topics. | Cross-region replication audit probe. |
| **F8.2** | Diagnostic logs with PII shipped to central region | **COMPLIANCE** | Centralized Datadog/CloudWatch pipeline aggregates logs containing raw payloads across jurisdictions. | Strict ban on raw payloads in logs; error trackers mask all tenant payload data and sanitize stack traces. | Automated log scanner for PII patterns. |

---

### Category 9 & 10: Cryptographic Shredding & Erasure

| Node | Scenario | Severity | Root Cause | Handling & Mitigation | Detection |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F9.1** | Encrypting whole payload with tenant master key | **COMPLIANCE** | Entire JSON blob encrypted with tenant KEK, making individual user GDPR erasure impossible without erasing entire tenant. | Field-level encryption hierarchy: tenant KEK wraps subject/period DEKs; only personal fields are encrypted. | Payload encryption schema inspection. |
| **F9.2** | Consumer retains master KEK in memory | **SECURITY** | Worker pods cache master KMS keys locally; pod compromise exposes all historical tenant data. | Consumers never hold KEKs. Decryption calls use KMS unwrap permissions, and plaintext DEKs are cached with 10-min TTL. | Key memory dump audit. |
| **F9.3** | Accidental master KEK deletion | **LOSS** | Administrator deletes KMS key, permanently destroying historical decryptability for all tenant data. | Enable KMS deletion protection with a 30-day waiting period; deletion requires dual-approval workflow. | KMS key deletion attempt alarms. |
| **F9.4** | Per-event KMS API calls trigger rate throttling | **DEGRADE** | Every consumed message calls AWS/GCP KMS directly, exhausting regional API quotas. | Cache unwrapped DEKs in consumer memory with bounded TTL (10 minutes max); DEKs are per subject/period, not per event. | KMS API call rate vs cache hit ratio. |
| **F10.1** | "Deleting the key equals complete erasure" | **COMPLIANCE** | DEK is shredded, but plaintext copies persist in search indices, Redis caches, and database projections. | Enforce `MT-10`: erasure orchestrator executes active checklist across projections, search indices, and caches. | Post-erasure synthetic search scan. |
| **F10.5** | Historical replay resurrects shredded data | **COMPLIANCE** | Replaying event streams attempts to decrypt shredded records and throws errors, or restores historical plaintext. | Crypto adapter catches shredded status and emits `[ERASED]` marker; replay pipelines recognize and preserve marker. | Replay audit ledger verification. |


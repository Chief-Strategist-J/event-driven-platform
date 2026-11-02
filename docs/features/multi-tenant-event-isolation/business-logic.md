# Multi-Tenant Event Isolation — Business Logic Decision Tree

```
MULTI-TENANT EVENT ISOLATION (BUSINESS DECISION TREE)
│
├── 1. TENANCY MODEL SELECTION & TOPOLOGY TIER
│   └── Trigger: Configuring Event Topics for Multi-Tenant Platform
│       ├── Check: Tenant Classification & Throughput Tier
│       │   ├── Branch A: High-Tier VIP / Enterprise Tenant (High Traffic / Dedicated SLA)
│       │   │   ├── Model: Dedicated Topic per Tenant
│       │   │   ├── Topic Naming: `tenant.<tenant_id>.<domain>.<event>.v1`
│       │   │   ├── Advantages: Dedicated partition capacity, zero noisy-neighbor interference, custom retention
│       │   │   └── Resource Quotas: Dedicated Kafka broker client quotas (e.g. 50MB/s)
│       │   │
│       │   └── Branch B: Standard Multi-Tenant Pooled Tier (Small to Mid-Size Tenants)
│       │       ├── Model: Shared Topics with Partition Keying & Tenant Headers
│       │       ├── Topic Naming: `shared.<domain>.<event>.v1`
│       │       ├── Key Rule: Key MUST incorporate `tenant_id`: `key = tenant_id + ":" + aggregate_id`
│       │       └── Header Invariant: Mandatory `X-Tenant-ID: <uuid>` on every Kafka message header
│       │
│       └── Validation Guard: Event emitted without `tenant_id` metadata?
│           └── Rejection: Producer validation layer drops event with `ErrMissingTenantContext`
│
├── 2. LOGICAL ISOLATION & ACCESS CONTROL ENFORCEMENT
│   └── Trigger: Service Consumes or Publishes Events Across Shared Topics
│       ├── Ingestion Filter (Consumer Side):
│       │   ├── Worker context initialized with authenticated tenant scope: `ctx.TenantID`
│       │   ├── Inspect Record Header: `record.Headers["X-Tenant-ID"]`
│       │   ├── Verification:
│       │   │   ├── Matches authorized tenant context ──> Pass to domain projection handler
│       │   │   └── Mismatched Tenant ID:
│       │   │       ├── Action: Silently drop or quarantine (Cross-tenant leak prevention)
│       │   │       └── Security Audit: Log security violation to SIEM / audit trail
│       │
│       └── Kafka Broker ACL Enforcement:
│           ├── SASL/SCRAM or mTLS principal assigned per tenant service
│           └── ACL Rules: Principal can ONLY write/read to topics matching `tenant.<tenant_id>.*`
│
└── 3. STORAGE & CRYPTO-SHREDDING COMPLIANCE TIER
    └── Trigger: Tenant Requests Account Deletion / GDPR Right to be Forgotten
        ├── Problem: Kafka event streams are immutable; cannot delete single tenant rows from historical log
        │
        ├── Solution: Per-Tenant Cryptographic Shredding
        │   ├── Architecture:
        │   │   ├── Every tenant has a unique Master Encryption Key (MEK) stored in KMS / Vault
        │   │   ├── All sensitive event payloads for Tenant X are encrypted with `MEK_X` before publishing
        │   │   └── Downstream consumers fetch `MEK_X` to decrypt during live consumption or replay
        │   │
        │   ├── Right-to-be-Forgotten Execution:
        │   │   ├── Action: Delete `MEK_X` from KMS / Vault permanently
        │   │   ├── Immediate Effect: All historical events for Tenant X across all Kafka topics and S3 cold archives
        │   │   │   become cryptographically unrecoverable ciphertext (Instant Compliant Erasure)
        │   │   └── Write Model / Read Store: Issue standard hard DELETE for active PostgreSQL / OpenSearch tables
        │   │
        │   └── Verification: Replaying from offset 0 results in undecryptable ciphertext; events safely discarded
```

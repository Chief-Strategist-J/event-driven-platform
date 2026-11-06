# Multi-Tenant Event Isolation — Architecture Diagrams & Workflows

This document visualizes the core architecture, runtime lifecycles, cryptographic key management, and noisy-neighbor quota enforcement of the Multi-Tenant Event Isolation subsystem.

---

## 1. High-Level Component Interaction Map

The Multi-Tenant Event Isolation engine enforces strict boundary isolation at every stage of ingestion, transport, consumption, and storage.

```mermaid
flowchart TD
    subgraph ClientLayer ["Client & Ingress Layer"]
        CLI["Authenticated Client Request"]
        GW["API Gateway / Token Verifier (C-TCX)"]
    end

    subgraph SecurityKernel ["Tenant Security Kernel"]
        TRG["Tenant Registry (C-TRG)"]
        QOS["Quota & Fairness Engine (C-QOS)"]
        CRY["Field Crypto Service (C-CRY)"]
        KMS["KMS / HSM Key Hierarchy (C-KMS)"]
        ERA["Erasure Orchestrator (C-ERA)"]
    end

    subgraph TransportLayer ["Event Transport (Kafka)"]
        OBX["Producer Outbox Stamping (C-OBX)"]
        TOP["Partitioning Engine: tenant_id + aggregate_id"]
    end

    subgraph ConsumerLayer ["Consumer Execution Runtime (C-RT)"]
        RT["Per-Record Scope Builder"]
        CHK["Triple-Check Validator (MT-02)"]
        PROJ["PostgreSQL Projections with RLS"]
        DLQ["Quarantine DLQ (INTEGRITY-TENANT)"]
    end

    CLI -->|Bearer Token| GW
    GW -->|Extracts claims| TRG
    GW -->|Checks lane budget| QOS
    GW -->|Encrypts PII fields| CRY
    CRY <-->|Wraps / Unwraps DEK| KMS
    GW -->|Dispatches event| OBX
    OBX -->|Key = tenant + aggregate| TOP
    TOP -->|Consumes stream| RT
    RT -->|Constructs scope| CHK
    CHK -->|Valid envelope| PROJ
    CHK -->|Mismatch or residency breach| DLQ
    ERA -->|Shreds subject / tenant DEK| CRY
```

---

## 2. Tenant Lifecycle State Machine

The tenant lifecycle is governed by a deterministic finite state machine (`MT-12`). Once a tenant reaches `OFFBOARDED`, its ID is permanently tombstoned to prevent data resurrection.

```mermaid
stateDiagram-v2
    [*] --> ONBOARDING: Registry record created & keys provisioned
    ONBOARDING --> ACTIVE: Placement validated & initial canary passes
    ONBOARDING --> OFFBOARDED: Onboarding aborted

    ACTIVE --> SUSPENDED: Billing dispute or abuse alert
    SUSPENDED --> ACTIVE: Dispute resolved & reactivated

    ACTIVE --> OFFBOARDING: Contract terminated or deletion requested
    SUSPENDED --> OFFBOARDING: Contract terminated

    OFFBOARDING --> OFFBOARDED: Plaintext purged, topics pruned & keys shredded
    OFFBOARDED --> [*]: ID tombstoned forever (Reactivation forbidden)
```

---

## 3. Ingress Envelope Triple-Check Verification Flow

Every incoming event must satisfy the invariant: `Header Tenant == Key Prefix Tenant == Payload Tenant`. Mismatches trigger immediate fail-closed quarantine.

```mermaid
sequenceDiagram
    autonumber
    actor Producer as Upstream Producer / API
    participant GW as Gateway / Scope Validator
    participant Reg as Tenant Registry (C-TRG)
    participant QOS as Quota Controller (C-QOS)
    participant Outbox as Transactional Outbox
    participant DLQ as Quarantine Dead-Letter Queue

    Producer->>GW: Publish Event (Header, PartitionKey, Payload)
    GW->>Reg: GetTenant(HeaderTenant)
    
    alt Tenant Not Found or Inactive
        Reg-->>GW: Tenant Not Found / Suspended
        GW->>DLQ: Route to DLQ (INTEGRITY-TENANT-UNKNOWN)
        GW-->>Producer: Return HTTP 403 Forbidden
    else Tenant Active
        Reg-->>GW: Tenant Record (AllowedRegions, Tier, Quotas)
    end

    GW->>GW: Assert (Header == KeyPrefix == Payload)
    alt Tenant Mismatch Detected
        GW->>DLQ: Route to DLQ (INTEGRITY-TENANT-HEADER-PAYLOAD)
        GW-->>Producer: Return HTTP 403 Forbidden
    end

    GW->>GW: Assert (CurrentRegion in AllowedRegions)
    alt Residency Disallowed
        GW->>DLQ: Route to DLQ (INTEGRITY-TENANT-RESIDENCY)
        GW-->>Producer: Return HTTP 403 Forbidden
    end

    GW->>QOS: CheckQuotaBudget(TenantID)
    alt Quota Budget Exceeded (Held Keys / Quarantine)
        QOS-->>GW: Throttled = true
        GW-->>Producer: Return HTTP 429 Too Many Requests
    else Within Budget
        QOS-->>GW: Allowed = true
        GW->>Outbox: Stage Event with Immutable Tenant Context
        GW-->>Producer: Return HTTP 201 Created
    end
```

---

## 4. Cryptographic Key Shredding & Right-to-be-Forgotten Flow

This sequence visualizes GDPR Right-to-be-Forgotten execution on an immutable append-only event stream using field-level encryption.

```mermaid
sequenceDiagram
    autonumber
    actor User as Data Subject (End-User)
    participant Orchestrator as Erasure Orchestrator (C-ERA)
    participant Keystore as Tenant DEK Keystore (C-KMS)
    participant Cache as Consumer Memory DEK Cache
    participant Stream as Kafka Historical Replay / Consumer

    User->>Orchestrator: Submit GDPR Erasure Request (subject_id)
    Orchestrator->>Orchestrator: Verify identity, retention rules & legal holds
    
    Orchestrator->>Keystore: ShredSubjectDEK(tenant_id, subject_id, period_key)
    Keystore->>Keystore: Set status = "SHREDDED", clear wrapped_dek
    Keystore-->>Orchestrator: Key Shredded Confirmation
    
    Orchestrator->>Cache: Broadcast InvalidateCache(tenant_id, subject_id)
    Cache->>Cache: Flush local memory DEK entry
    
    Note over Stream,Keystore: Future Historical Replay of Encrypted Event
    Stream->>Cache: FetchPlaintextDEK(tenant_id, subject_id)
    Cache-->>Stream: Cache Miss
    Stream->>Keystore: GetDEK(tenant_id, subject_id)
    Keystore-->>Stream: Status = "SHREDDED"
    Stream->>Stream: Return [ERASED] byte marker
    Stream->>Stream: Deserialization succeeds; PII safely omitted
```

---

## 5. Noisy-Neighbor Fair Queueing & Dynamic Throttling Flow

Shared partitions serve multiple tenants simultaneously. When a rogue tenant saturates capacity, only that tenant is throttled while others continue processing without interruption.

```mermaid
flowchart TD
    ING["Incoming Event Batch on Shared Partition"] --> PARSE["Parse Record Tenant Scope"]
    PARSE --> BUDGET{"Evaluate Tenant Quota Budget"}
    
    BUDGET -->|Held Keys < Max AND Quarantine < Max| ALLOW["Dispatch to Worker Lane"]
    BUDGET -->|Held Keys >= Max OR Quarantine >= Max| THROTTLE["Activate Tenant-Level Throttle"]
    
    ALLOW --> EXEC["Execute Domain Handler"]
    EXEC --> COMMIT["Commit Offset & Update Metrics"]
    
    THROTTLE --> HOLD["Park Key in Tenant Held Store"]
    HOLD --> LOG["Emit Alert to SIEM & Update Quota Budget"]
    LOG --> CONT["Shared Partition Continues Streaming Other Tenants"]
```

---

## 6. Tenancy Model Topology & Routing

The platform dynamically routes event streams across three distinct topology models based on customer tier, contractual isolation, and data sovereignty requirements.

```mermaid
flowchart TD
    INGRESS["Ingress Event Envelope"] --> TOPOLOGY{"Topology Resolver (C-TOP)"}

    subgraph PoolTopology ["M-POOL: Shared Topic Topology"]
        P_TOPIC["Pooled Topic: domain.aggregate.v1"]
        P_GRP["Shared Consumer Group: cg.service.domain"]
        P_DB["PostgreSQL: Shared DB + RLS Scoping"]
        P_TOPIC --> P_GRP --> P_DB
    end

    subgraph DedicatedTopology ["M-DEDICATED: Per-Tenant Topics"]
        D_TOPIC["Dedicated Topic: t.tenant_id.domain.aggregate.v1"]
        D_GRP["Dedicated or Filtered Consumer Group"]
        D_DB["PostgreSQL: Shared DB + Dedicated Schema"]
        D_TOPIC --> D_GRP --> D_DB
    end

    subgraph SiloTopology ["M-SILO: Isolated Sovereign Enclave"]
        S_KAFKA["Air-Gapped Regional Kafka Cluster"]
        S_GRP["Isolated Regional Consumer Group"]
        S_DB["Dedicated Sovereign Database"]
        S_KAFKA --> S_GRP --> S_DB
    end

    TOPOLOGY -->|Standard SaaS Tier| P_TOPIC
    TOPOLOGY -->|High Throughput / Custom Retention| D_TOPIC
    TOPOLOGY -->|Regulatory Residency / Swiss Banking| S_KAFKA
```

---

## 7. Stream Tier Migration Cutover Workflow (MT-13)

Migrating a tenant from `M-POOL` to `M-DEDICATED` without stopping live traffic, dropping events, or breaking partition ordering requires a version-based cutover runbook.

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Platform Migration Operator
    participant Outbox as Producer Outbox
    participant Pooled as M-POOL Topic
    participant Dedicated as M-DEDICATED Topic
    participant LegacyConsumer as Legacy Consumer Group
    participant ShadowConsumer as Shadow Consumer Group
    participant Projection as Read-Model Projection

    Admin->>Outbox: Phase 1: Enable Dual-Publish for Tenant
    Note over Outbox: Outbox simultaneously stages events to both Pooled & Dedicated topics
    Outbox->>Pooled: Publish Event (Tenant A, Version V)
    Outbox->>Dedicated: Publish Event (Tenant A, Version V)

    Admin->>ShadowConsumer: Phase 2: Start Shadow Consumer on Dedicated Topic
    ShadowConsumer->>Dedicated: Ingest stream & update shadow projection
    LegacyConsumer->>Pooled: Continue serving live production reads

    Admin->>Admin: Phase 3: Validate Stream Parity at Aggregate Version V_Cut
    Admin->>Projection: Phase 4: Atomic Pointer Swap (Live reads route to Dedicated projection)
    
    Admin->>LegacyConsumer: Phase 5: Drain & Unsubscribe Tenant A from Pooled Consumer
    Admin->>Outbox: Phase 6: Disable Dual-Publish (Publish only to Dedicated Topic)
```

---

## 8. Defense-in-Depth Read Path & PostgreSQL RLS Enforcement

Every database read transaction enforces defense in depth (`MT-07`): application-level query scoping combined with PostgreSQL session-scoped Row-Level Security (`RLS`).

```mermaid
sequenceDiagram
    autonumber
    actor Client as API Client / Consumer
    participant API as Read Service Handler
    participant Pool as Database Connection Pool
    participant PG as PostgreSQL Engine (RLS)

    Client->>API: Read Orders (Bearer Token)
    API->>API: Extract server-derived TenantContext (Tenant A)
    API->>Pool: Acquire Connection
    Pool-->>API: Active Connection
    
    API->>PG: BEGIN TRANSACTION
    API->>PG: SET LOCAL app.current_tenant = "tenant_A"
    
    API->>PG: SELECT * FROM orders WHERE tenant_id = "tenant_A" AND id = "ord_01"
    PG->>PG: Evaluate RLS Policy: orders_tenant_isolation_policy
    PG->>PG: Check (tenant_id = current_setting('app.current_tenant'))
    
    alt Policy Passed
        PG-->>API: Return Order Data
    else Cross-Tenant Access Attempt (e.g. Malicious SQL Injection)
        PG-->>API: 0 Rows Returned / Permission Denied
    end

    API->>PG: COMMIT / RESET LOCAL app.current_tenant
    API->>Pool: Return Connection to Pool (Clean Context)
    API-->>Client: HTTP 200 OK
```

---

## 9. Binary Encrypted Event Envelope Architecture

To balance event streaming observability with strict GDPR right-to-be-forgotten shredding, envelopes separate non-personal routing headers from field-level encrypted payloads.

```
┌────────────────────────────────────────────────────────────────────────┐
│ PUBLIC CLEAR ENVELOPE (Stream Routing & Filtering)                     │
├────────────────────────────────────────────────────────────────────────┤
│ • event_id:       "019302bf-9c76-78e2-b13e-108781628172" (UUIDv7)     │
│ • tenant_id:      "tenant_enterprise_01"                              │
│ • aggregate_id:   "ord_9876543210" (Opaque external ID)                │
│ • aggregate_type: "order"                                              │
│ • aggregate_seq:  4                                                    │
│ • event_type:     "OrderCompleted"                                     │
│ • schema_version: "1.2.0"                                              │
│ • correlation_id: "019302bf-9c76-78e2-b13e-108781628173"              │
│ • occurred_at:    "2026-11-06T10:15:00Z"                               │
├────────────────────────────────────────────────────────────────────────┤
│ CRYPTOGRAPHIC KEY METADATA                                             │
├────────────────────────────────────────────────────────────────────────┤
│ • subject_id:     "usr_sub_109283" (End-user ID)                       │
│ • period_key:     "2026-Q4"                                            │
│ • dek_key_id:     "dek_019302bf9c7678e2b13e"                           │
│ • algorithm:      "AES_256_GCM"                                        │
├────────────────────────────────────────────────────────────────────────┤
│ ENCRYPTED FIELD PAYLOAD                                                │
├────────────────────────────────────────────────────────────────────────┤
│ {                                                                      │
│   "order_total_cents": 15000,                                          │
│   "currency": "USD",                                                   │
│   "customer_pii": "CIPHERTEXT(nonce=12B, tag=16B, data=AES_GCM(...))" │
│ }                                                                      │
│                                                                        │
│ ──▶ Upon Subject DEK Shredding:                                        │
│     "customer_pii" ──▶ "[ERASED]"                                      │
└────────────────────────────────────────────────────────────────────────┘
```



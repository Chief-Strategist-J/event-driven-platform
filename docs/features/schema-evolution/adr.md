# Architectural Decision Record: Transitive Schema Evolution, Consumer Readiness Gating & Tolerant Reading (WP-16)

## 1. Title & Metadata
* **Status**: APPROVED
* **Date**: 2026-11-06
* **Decision Makers**: Platform Architecture Guild, Schema Governance Board, Chief Distributed Systems Architect
* **Governing Specification**: [Schema Evolution — Production System Design Tree (v2)](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/schema-evolution/business-logic.md)

---

## 2. Context & Problem Statement
In distributed event-driven systems, events stored in message brokers (Kafka) and persistent event stores are immutable historical facts. Software components evolve independently across services, organizations, and languages. Without formal architectural governance over schema mutations:
1. **Pairwise vs Transitive Incompatibility (F2.1, OUTAGE)**: Pairwise compatibility ($v_3 \sim v_2$ and $v_2 \sim v_1$) does not guarantee transitive compatibility ($v_3 \sim v_1$). When a historical replay or disaster-recovery rebuild occurs from offset 0, modern consumers encounter legacy $v_1$ events and crash.
2. **Fabricated Facts via Domain Defaults (F9.3, CORRUPT)**: Standard deserializers often substitute arbitrary default values (e.g. defaulting currency to `"USD"` or status to `"ACTIVE"`) when fields are missing from older events, corrupting historical analytics and legal audit trails.
3. **Producer Deployments Starving Consumers (F2.2, OUTAGE)**: Shipping a new producer schema before all consumer groups declare readiness in the Consumer Registry (`C-CRG`) causes mass deserialization failures across downstream services.
4. **Silent Semantic Scale Shifts (F3.4, CORRUPT)**: Altering the precision or scale of a numeric field (e.g., decimal scale $2 \to 4$, or seconds $\to$ milliseconds) without changing the syntactic schema type bypasses schema registries while distorting financial values by factors of 100 or 1000.
5. **Systemic vs Single-Message Panic Storms (F9.2, OUTAGE)**: Routing thousands of deserialization errors to a Dead-Letter Queue individually during a bad schema deployment causes alert storms and hides systemic root causes.

To eliminate data corruption and consumer panics while supporting autonomous continuous deployment, the platform mandates strict transitive schema evolution governance.

---

## 3. Decision Drivers & Architecture Invariants (SE-01 to SE-11)

* **SE-01: Transitive Compatibility against Production Lineage [Certain]**:
  Every schema artifact enforces `FULL_TRANSITIVE` compatibility against the production registry lineage. A proposed schema version must maintain bidirectional compatibility with *every* historical version still within the data retention horizon.
* **SE-02: Registry Rules Cannot Be Loosened Silently [Certain]**:
  Registry compatibility rules (`FULL_TRANSITIVE`, `BACKWARD_TRANSITIVE`) can only be modified via the governance pipeline (`C-GOV`) with explicit architectural review; unauthorized rule loosening triggers immediate P0 audit alerts.
* **SE-03: Zero Auto-Registration in Production [Certain]**:
  Applications and producer runtimes have read-only credentials to the production schema registry. Only the automated CI/CD governance pipeline identity (`C-GOV`) can register new schema versions.
* **SE-04: Defaults Never Invent Business Facts [Certain]**:
  Optional fields default strictly to `null` or an explicit `UNKNOWN` marker. Non-null domain defaults are forbidden unless mathematically justified as universally true across all historical events.
* **SE-05: Meaning Never Changes Under an Existing Name [Certain]**:
  Field semantics, measurement units, precision scales, and currency conventions are immutable. Altering the meaning of a property mandates an explicit field rename or major stream version fork.
* **SE-06: Deploy Order Follows Change Class via Consumer Registry [Certain]**:
  Under `BACKWARD` compatibility, consumer groups must deploy first. Producer deployments are blocked by the release pipeline until the Consumer Registry (`C-CRG`) verifies that 100% of registered consumers declare support for the new schema version.
* **SE-07: Decommissioning Gated on Consumer Registry & Usage Telemetry [Certain]**:
  Retiring a legacy stream or schema version requires:
  - Zero active consumer registrations in `C-CRG`.
  - Zero broker subscriptions over 30 days.
  - Zero read operations recorded in field-usage telemetry (`C-TEL`).
  - Decommissioning is **never** decided based solely on zero consumer lag (F8.1).
* **SE-08: Retained Bytes Stay Readable Forever [Certain]**:
  Schemas are never hard-deleted from registries. When an event stream is archived to cold S3 storage, the full schema definition lineage and golden samples are archived alongside the raw envelopes.
* **SE-09: Headers and Keys are Explicit Contracts [Certain]**:
  Message key partition routing, header names, and tracing formats are versioned contracts governed by `C-GOV`. Renaming headers or altering key casing is treated as a breaking change.
* **SE-10: Cross-Language Semantic Equivalence [Certain]**:
  Code-generated serializers across all supported runtimes (Go, Kotlin, TypeScript, Python) must produce byte-compatible representations verified by golden sample round-trip tests in CI.
* **SE-11: Single vs Systemic Deserialization Classification [Certain]**:
  Deserialization errors are classified at runtime:
  - *Single message failure*: Quarantines raw bytes to DLQ and holds partition key to preserve ordering.
  - *Systemic failure* (divergence across $>5$ keys in window): Halts consumer partition processing and emits a single consolidated P1 alert.

---

## 4. Compatibility Regimes & Governance Rules

| Regime | Compatibility Mode | Producer Deploy First? | Consumer Deploy First? | Enforced By |
| :--- | :--- | :--- | :--- | :--- |
| **Standard Event Stream** | `FULL_TRANSITIVE` | Either | Either | Apicurio Registry Gate |
| **Strict Consumer Lead** | `BACKWARD_TRANSITIVE` | Blocked until C-CRG Ready | **Mandatory First** | Pipeline Gate G9 + C-CRG |
| **Breaking Stream Redesign** | `STREAM_FORK` (`.v2`) | Dual-Publish in Same Tx | Shadow Parity Cutover | Transactional Outbox + Topic Fork |

---

## 5. System Architecture & Component Mapping

```
+-----------------------------------------------------------------------------------+
|                        SCHEMA GOVERNANCE PLATFORM (C-GOV)                         |
|                                                                                   |
|  +--------------------+    +----------------------+    +-----------------------+  |
|  |  Schema Registry   |    |  Consumer Registry   |    | Field Usage Telemetry |  |
|  |   (FULL_TRANSITIVE |    | (Supported Versions  |    | (Active Field Reads   |  |
|  |    Apicurio PROD)  |    |  & Deploy Gating)    |    |  per Consumer Group)  |  |
|  +---------+----------+    +----------+-----------+    +-----------+-----------+  |
|            |                          |                            |              |
+------------|--------------------------|----------------------------|--------------+
             v                          v                            v
    +-------------------------------------------------------------------+
    | Release Gating Engine (G1 to G9): Verifies Compatibility & Ready |
    +---------------------------------+---------------------------------+
                                      |
         +----------------------------+----------------------------+
         |                                                         |
         v                                                         v
+-------------------------------+                         +-------------------------------+
|     Producer Serialization    |                         |    Consumer Deserialization   |
| - Embeds Schema ID in Header  |                         | - Resolves Writer Schema      |
| - Dual-Publishes v1+v2 in Tx  |                         | - Tolerant Reader Projection  |
| - Zero Auto-Registration      |                         | - Null/Unknown (No Defaults)  |
| - Validates Semantic Metadata |                         | - Single vs Systemic DLQ Gate |
+-------------------------------+                         +-------------------------------+
```

---

## 6. PostgreSQL Persistence Model (Migration `0020`)
The database schema introduces four dedicated tables supporting the schema evolution domain:

1. **`schema_evolution_version_registry`**:
   Tracks subject schemas, version numbers, canonical SHA-256 fingerprints, compatibility modes (`FULL_TRANSITIVE`, `BACKWARD_TRANSITIVE`), and schema definitions.
2. **`schema_evolution_consumer_registry`**:
   Maintains consumer group identities, stream subscriptions, declared supported schema version ranges, and release clearance status (`C-CRG`).
3. **`schema_evolution_dual_publish_ledger`**:
   Manages multi-stream transitions (`v1` $\to$ `v2`), dual-publish start offsets, sunset deadlines, and the 5-point decommission gate checklist (§8.5).
4. **`schema_evolution_incident_log`**:
   Audits deserialization failures, single vs systemic classification, quarantined DLQ envelopes, and schema rule violation attempts.

---

## 7. Consequences & Trade-offs

### Positive
- **Zero Replay Failures**: `FULL_TRANSITIVE` ensures historical replays from offset 0 never encounter unreadable legacy versions.
- **Audit & Regulatory Integrity**: Strict prohibition of synthetic domain defaults (`SE-04`) prevents silent fabrication of business facts in financial reports.
- **Safe Continuous Delivery**: Automated release gating (`SE-06`) ensures producers cannot deploy schemas that break active consumers.

### Negative & Mitigations
- **Rigorous CI Pipeline**: G1–G9 validation gates add 30–60 seconds to pull request builds.
  - *Mitigation*: Cached golden samples and parallelized compatibility linters.
- **Extended Migration Windows**: Dual-publishing and consumer attestations require disciplined 30-day tracking.
  - *Mitigation*: Automated telemetry tracking (`C-TEL`) alerts teams with lagging migrations.

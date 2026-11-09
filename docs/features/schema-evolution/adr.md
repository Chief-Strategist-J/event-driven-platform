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

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

In distributed microservices, services deploy on independent cadences. If producer teams deploy a new schema that breaks backward compatibility, downstream consumer services deployed days or weeks earlier immediately crash with unmarshaling exceptions. Reverting releases across dozens of autonomous microservices is practically impossible without zero-downtime schema evolution.

### 2. Internal Algorithmic Mechanics & State Invariants

The platform enforces strict Backward Compatibility as the default evolution rule. Under backward compatibility: (1) New fields added to a schema must declare default values. (2) Existing fields cannot be removed or have their data types altered. The Schema Registry validates these rules via CI/CD before any schema can be registered. Downstream consumers built with schema v1.0 safely read events emitted by producers using schema v1.1.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Strict Backward Compatibility Invariant
* **Design Philosophy & Epistemology**:
  In an asynchronous ecosystem, consumers cannot be upgraded instantaneously alongside producers. Rolling deployments mean old and new versions run concurrently for hours or days. The philosophy is Asynchronous Independence: any new schema version must be fully readable by existing consumers running older software versions.
* **Threat Vector Defeated**: Production-wide consumer crashes during rolling deployments when new producers emit events that old consumers cannot parse.
* **Storage & State Invariant**:
```text
Deserialize(NewEventBytes, OldConsumerSchema) == SUCCESS
CompatibilityMode == BACKWARD (or BACKWARD_TRANSITIVE)
```
* **Implementation Mechanism**:
  The Schema Registry enforces BACKWARD compatibility rules on all schema updates. Adding fields without default values, deleting required fields, or altering existing field types is mathematically rejected by the registry API.
* **Non-Negotiable Trade-Off**: Developers cannot make abrupt, breaking changes to contracts; all breaking evolution requires multi-stage migration lifecycles.

---

#### Principle 2: Mandatory Defaults for Additive Evolution
* **Design Philosophy & Epistemology**:
  An additive field is only safe if old software versions and historical logs can handle its absence deterministically. The philosophy is Deterministic Absence: every newly introduced field must define a default value.
* **Threat Vector Defeated**: Deserialization failures when replaying historical event logs that were published prior to the introduction of a new required field.
* **Storage & State Invariant**:
```text
For all newly added fields: Field.Default != NULL
Historical events deserialize with default values applied automatically
```
* **Implementation Mechanism**:
  Schema linters in CI reject any PR introducing a schema modification where new fields lack a default value (e.g., `"default": ""` or `"default": 0`).
* **Non-Negotiable Trade-Off**: Application domain logic must be prepared to handle default values meaningfully without misinterpreting zero/false as active domain choices.

---

#### Principle 3: Prohibition of In-Place Field Renaming
* **Design Philosophy & Epistemology**:
  To a computer, 'renaming' a field is identical to deleting the old field and creating a new one. In an asynchronous event stream, this instantly breaks older consumers that still look for the original field name. The philosophy is Expand, Alias, Contract: never rename in-place; introduce the new field alongside the old one, maintain both, and deprecate the old field over time.
* **Threat Vector Defeated**: Silent data loss where consumers fail to populate internal models because the expected field name was renamed by the producer.
* **Storage & State Invariant**:
```text
Field renames must be executed as: Add(NewField) -> DualWrite -> Deprecate(OldField) -> Remove(OldField)
```
* **Implementation Mechanism**:
  Protobuf / Avro schemas utilize field aliases or maintain both fields during migration windows. Code reviews block any PR that changes a field name in-place.
* **Non-Negotiable Trade-Off**: Temporary payload size overhead and maintenance of dual-field mapping logic in domain services during migration windows.

---

#### Principle 4: Automated Pre-Merge CI Compatibility Gates
* **Design Philosophy & Epistemology**:
  Relying on developers to remember schema evolution rules during manual code reviews is insufficient. The philosophy is Automated Continuous Verification: schema compatibility must be verified automatically by CI/CD pipelines before any code or schema change can be merged to main.
* **Threat Vector Defeated**: Accidental breaking schema changes slipping into production through rushed pull requests or oversight.
* **Storage & State Invariant**:
```text
CI_Check: SchemaRegistryClient.TestCompatibility(ProposedSchema, LatestProductionSchema) == PASS
PR Merge blocked if status != PASS
```
* **Implementation Mechanism**:
  GitHub Actions / GitLab CI runs automated schema validation jobs using the Schema Registry CLI / Maven / Gradle plugins. If the proposed schema violates compatibility rules, the build fails with an actionable diff.
* **Non-Negotiable Trade-Off**: CI pipeline dependency on a staging Schema Registry instance or local registry container to run compatibility tests.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **What is the difference between BACKWARD and FORWARD schema compatibility?**
   * *Answer*: BACKWARD compatibility means consumers running old code can read events written by new producers. FORWARD compatibility means consumers running new code can read events written by old producers. FULL compatibility satisfies both, enabling upgrades in any order.
2. **How does Protobuf handle schema evolution differently than JSON Schema?**
   * *Answer*: Protobuf identifies fields strictly by integer field numbers (tags), not field names. You can rename a field in Protobuf without breaking compatibility, as long as the integer field tag and wire type remain identical.
3. **What should you do if a fundamental business requirement demands a breaking, non-backward-compatible schema change?**
   * *Answer*: You must create a brand-new topic (e.g., `orders.v2`) with a new major schema version. Producers publish to `orders.v2`, and a dual-write or bridge adapter mirrors events until consumers have fully migrated to the new topic.

---
## Authoritative References & Production Citations

* **[Martin Kleppmann: Designing Data-Intensive Applications (Chapter 4: Encoding and Evolution)](https://dataintensive.net/)**
  - *Architectural Rationale & Implementation Grounding*: The definitive chapter analyzing binary encoding formats (Avro, Protocol Buffers, Thrift) and compatibility rules.
* **[Confluent Schema Registry: Rules of Schema Evolution](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)**
  - *Architectural Rationale & Implementation Grounding*: Exhaustive documentation on schema compatibility modes (BACKWARD, FORWARD, FULL) and evolution rules.
* **[Apache Avro Specification: Schema Resolution Rules](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)**
  - *Architectural Rationale & Implementation Grounding*: Formal specification detailing how a reader schema resolves differences against an incoming writer schema.
* **[Thoughtworks Technology Radar: Expand and Contract Pattern](https://www.thoughtworks.com/radar/techniques/expand-and-contract)**
  - *Architectural Rationale & Implementation Grounding*: Architectural pattern for making breaking changes to interfaces and databases in an evolutionary, zero-downtime manner.

## Amendment 2026-11-07: Compatibility enforcement at registration, and migration 0028 context

### Problem observed
`POST /api/v1/schemas/register` never checked compatibility. A new version that added a required field (breaking under BACKWARD) was accepted with `201`, which defeats invariant SE-04.

### Root cause
`SchemaEvolutionService.RegisterSchema` validated fingerprint uniqueness and version monotonicity only; the stored `compatibility_mode` was recorded but never evaluated against the lineage (scaffold left the check unimplemented, RC4).

### Decision / fix
New pure function `rules.CheckSchemaCompatibility(mode, candidateDefinition, lineage)` in `domain/rules/schema-compatibility.rules.go`, called from `RegisterSchema` after lineage load. A violation returns `ErrIncompatibleSchema` (409) with human-readable reasons per past version.

Field model (`parseSchemaFields`): Avro record (`fields[]`; a field is required iff it has no `default`) or JSON Schema (`properties` + `required[]`; required iff listed in `required` and no `default`). Types are compared by canonical JSON. A definition that is not parseable JSON, or lineage entries that are not, are skipped (no semantic comparison possible, so they pass).

Compatibility matrix per mode (what is rejected):

| Mode | Compared against | BACKWARD check (new schema reads old data) | FORWARD check (old schema reads new data) |
|---|---|---|---|
| BACKWARD | latest version only | new required field without default; changed type | not checked |
| BACKWARD_TRANSITIVE | every past version | same as above | not checked |
| FORWARD | latest version only | not checked | removed required field; changed type |
| FORWARD_TRANSITIVE | every past version | not checked | same as above |
| FULL | latest version only | yes | yes |
| FULL_TRANSITIVE (default when mode is empty, strictest fallback) | every past version | yes | yes |
| NONE | n/a | no check | no check |

Notes: adding an optional (defaulted) field passes BACKWARD and FULL; removing an optional field passes FORWARD; removing a required field fails FORWARD/FULL. `NONE` is not accepted at the API: the DB check `ck_schema_evolution_version_compatibility` and `ErrInvalidCompatibilityMode` list only the six real modes (the rule function still short-circuits for NONE). Duplicate fingerprint, non-monotonic version and incompatible schema all return `409`; an unknown mode returns `400`.

We implemented structural checks in-service rather than delegating to Apicurio so the rule is deterministic, unit-testable and runs even when the registry is unavailable. Apicurio remains a separate store written only by `POST /api/v1/schemas` (unverified relationship to this route beyond that).

### Migration 0028 (context; also relevant to materialized-views and partitioning)
`database/migrations/sql/0028_scope_partitioning_and_rebuild_uniqueness_per_tenant.sql` (+ `.rollback.sql`) is not about schema registry tables. It replaces three global uniqueness constraints with per-tenant ones: `partition_topic_topology` `UNIQUE(topic_name)` -> `UNIQUE(tenant_id, topic_name)`; `partition_migration_runbook_log` `UNIQUE(migration_id)` -> `UNIQUE(tenant_id, migration_id)`; `materialized_view_rebuild_log` `UNIQUE(rebuild_id)` -> `UNIQUE(tenant_id, rebuild_id)`. Why: one tenant could block another from using the same topic/migration/rebuild id. Reversible, lock risk LOW, schema only. **Not applied to the live database**: `serve` does not run migrations and the live DB has no `schema_migrations` table; apply via the migration tooling. Rollback restores global uniqueness and fails if tenants now share values.

### API-visible behavior change
- Incompatible schema registration: `201` -> `409` (**breaking** for producers that relied on unchecked registration).
- Unknown mode: `400`.

### Verification
Live: v2 adding a required field -> 409; v2 adding a defaulted field -> 201. Unit tests in `src/features/schema-evolution/tests/schema_evolution_unit_test.go`.

### Remaining gaps
- Only structural field required/type checks: no enum-symbol, nested record, union-widening or logical-type analysis.
- Non-JSON schema formats (e.g. Protobuf) pass unchecked.
- Type comparison is strict string equality of canonical JSON (a widening such as int to long is flagged as a change).

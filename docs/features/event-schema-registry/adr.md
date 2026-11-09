

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Without centralized schema governance, producers independently evolve event payloads, adding or renaming fields without coordination. Downstream consumer microservices crash in production when unmarshaling unexpected types or missing required fields. Embedding full schema definitions in every message payload inflates network bandwidth and broker storage by 800%.

### 2. Internal Algorithmic Mechanics & State Invariants

The platform integrates with an Apicurio / Confluent-compatible Schema Registry. Producers register schemas before publishing; the registry validates compatibility rules and assigns a unique 4-byte Schema ID. The producer prepends this Schema ID (using a 5-byte magic framing format: 0x00 + 4-byte big-endian ID) to the binary payload. Consumers extract the ID, fetch the cached schema from the local registry cache, and deserialize with zero schema transmission overhead.

### 3. Core Architectural Principles

* **Centralized Contract Authority**: Schemas must be validated centrally prior to emission; producers cannot publish payloads that violate active compatibility policies.
* **Wire Overhead Elimination**: Messages serialize lightweight 4-byte schema IDs in the framing header rather than transmitting redundant schema text across every message.
* **Fail-Fast Producer Verification**: Producers validate payloads against cached contracts in-memory, throwing validation errors before publishing to the network.
* **Multi-Tenant Subject Namespacing**: Schema subjects enforce strict tenant and bounded context segregation to prevent accidental cross-tenant contract collisions.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **What happens if the Schema Registry crashes during peak traffic? (Clients maintain local in-memory LRU schema caches; once a schema ID is cached, producers and consumers operate completely uninterrupted during registry downtime).**
2. **Why is schema evolution governance enforced at the registry rather than in consumer code? (Registry-side enforcement prevents invalid contracts from ever entering the broker log; fixing corrupted historical messages in Kafka requires painful topic migration).**
3. **How do we handle multi-tenant subject naming? (Subjects follow strict domain namespaces: ${tenant_id}.${domain}.${entity}.${event_type}-value, preventing cross-tenant schema contamination).**

---

## Authoritative References & Production Citations

* **[Apicurio Registry REST API Specification v2](https://www.apicur.io/registry/)**
  - *Architectural Rationale & Implementation Grounding*: Open-source, production-grade schema registry supporting Avro, JSON Schema, and Protobuf with multi-tier compatibility validation.
* **[Confluent Schema Registry Architecture & Wire Protocol](https://docs.confluent.io/platform/current/schema-registry/index.html)**
  - *Architectural Rationale & Implementation Grounding*: Industry standard establishing the 5-byte magic byte schema identifier framing format for Kafka message payloads.
* **[Apache Avro Specification v1.11.1](https://avro.apache.org/docs/current/spec.html)**
  - *Architectural Rationale & Implementation Grounding*: Binary serialization framework providing rich data structures, compact binary encoding, and schema resolution rules.
* **[Martin Kleppmann: Schema Evolution in Avro, Protocol Buffers and Thrift](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html)**
  - *Architectural Rationale & Implementation Grounding*: Comprehensive comparative analysis of schema resolution mechanisms across modern serialization formats.

## Amendment 2026-11-07: Apicurio write path and GET semantics

- `POST /api/v1/schemas` is the only route that writes to Apicurio.
- schema-evolution (`POST /api/v1/schemas/register`) and event-versioning (`POST /api/v1/schemas/upcasters`, upcast) write to Postgres only; they do not publish to Apicurio.
- `GET /api/v1/schemas` is intentionally unsupported and returns 405.

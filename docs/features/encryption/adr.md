# ADR: Envelope Encryption, Per-Subject DEK Isolation, and Authenticated Additional Data Binding

## Status
Accepted

## Context
Event-driven distributed architectures stream and store personally identifiable information (PII) and sensitive financial attributes across durable storage topics, outbox tables, CDC relays, and read-side projections. In this architecture:
1. "Encryption at rest" (e.g. disk or volume-level encryption) fails to protect against insider database administrators, snapshot leakage, or broad Kafka consumer access (Threat model §2).
2. GDPR "Right to be Forgotten" (RTBF) and CCPA erasure mandates require deleting a subject's personal data without destroying the append-only event stream or breaking cryptographic hash chains (AU-09, EN-01).
3. If AES-GCM encryption does not strictly bind contextual metadata via Authenticated Additional Data (AAD), an attacker can transplant valid ciphertext from one subject or field into another without detection (EN-02, Failure E5.4a).
4. Direct KMS calls for every individual message would exhaust rate limits and cause systemic outages during high-throughput replays (EN-07, Failure E7.3).

## Decision
1. **Envelope Encryption Hierarchy (EN-01, EN-06)**:
   - Root Key Encryption Keys (KEKs) are provisioned per tenant and stored in KMS/Vault.
   - Fine-grained Data Encryption Keys (DEKs) are generated per data subject (`subject_id`) and stored wrapped under the tenant KEK in `encryption_dek_ledger`.
   - Plaintext DEKs are cached locally with bounded TTL (5-15 min) to satisfy high-throughput requirements without repeated KMS roundtrips.

2. **Mandatory Authenticated Additional Data (AAD) Construction (EN-02)**:
   - Every payload encryption strictly binds:
     $$\text{AAD} = \text{tenant\_id} \,\|\, \text{subject\_id} \,\|\, \text{aggregate\_id} \,\|\, \text{field\_path} \,\|\, \text{schema\_version} \,\|\, \text{key\_ref}$$
   - Any attempt to copy or replay ciphertext to a different subject, field, or tenant results in immediate tag verification failure (`TAMPERED`).

3. **Cryptographic Erasure / Crypto-Shredding (EN-01, EN-05)**:
   - To execute an erasure request, the subject's wrapped DEK in `encryption_dek_ledger` is zeroized/shredded and an immutable audit entry is logged in `crypto_shredding_audit_ledger`.
   - Historical ciphertexts in immutable topics remain intact; upon read attempt, the key is resolved as `SHREDDED`, cleanly producing an `ERASED` status rather than corruption or `NULL` overwrite.

4. **Strict 6-Outcome Decryption Evaluation (EN-05)**:
   - Decryption evaluates to one of six explicit states:
     - `DECRYPTED`: Valid tag and successful decryption.
     - `ERASED`: Subject DEK was intentionally shredded.
     - `UNAUTHORIZED`: Consumer lacks entitlement to the field class.
     - `UNAVAILABLE`: Transient KMS or network failure.
     - `TAMPERED`: AEAD authentication tag or AAD mismatch.
     - `MALFORMED`: Corrupted or invalid payload envelope.

5. **Keyed Blind Indexes for Equality Searches (C-BIX)**:
   - Equality lookups on encrypted identifiers (e.g. searching customer by email) use keyed HMAC digests ($H_{\text{bix\_key}}(\text{normalized\_val})$). Raw PII is never stored in index tables.

## Consequences
- **Positive**: Strict GDPR/CCPA crypto-shredding compliance on append-only streams, zero cross-subject ciphertext transplantation, bounded KMS API overhead, explicit error classification.
- **Negative**: Added CPU overhead for AES-256-GCM AEAD operations, reduced compression ratios for encrypted message fields, blind index management overhead.

---

## Deep Failure-First Architectural Principles

### 1. Existential Purpose & The Catastrophe Prevented

Event brokers persist data to unencrypted local disk logs and replicate across network boundaries. If an attacker gains unauthorized access to broker disks, network captures, or backup archives, sensitive customer data and credentials are fully compromised. Encrypting whole disks (at-rest) fails to protect against insider threats with broker read access; field-level envelope encryption is required.

### 2. Internal Algorithmic Mechanics & State Invariants

The producer generates an ephemeral Data Encryption Key (DEK) per event or tenant. The payload is encrypted using AES-256-GCM authenticated encryption. The DEK is encrypted using a Master Key Encryption Key (KEK) managed in KMS/HSM and prepended to the message envelope. Consumers retrieve the wrapped DEK, request KMS to unwrap it, and decrypt the payload in-memory. Destroying the DEK enables instant, GDPR-compliant crypto-shredding.

### 3. Core Architectural Principles & Systems Philosophy

#### Principle 1: Zero-Trust Payload Encryption at Rest and in Motion
* **Design Philosophy & Epistemology**:
  Infrastructure security is insufficient. Encrypting Kafka disks and TLS wire protocols does not protect data from compromised broker administrators, memory dumps, or misconfigured topic access controls. The philosophy is End-to-End Application-Layer Encryption: sensitive payloads must be encrypted before leaving the producer's memory and decrypted only after entering the authorized consumer's memory.
* **Threat Vector Defeated**: Massive data exposure resulting from compromised broker infrastructure, unauthorized log scraping, or malicious cloud infrastructure administrators.
* **Storage & State Invariant**:
```text
Payload on Kafka Broker Disk == Ciphertext
Decryption possible ONLY with authorized KMS Key access
```
* **Implementation Mechanism**:
  The producer utilizes envelope encryption via AWS KMS or HashiCorp Vault. A local Data Encryption Key (DEK) encrypts the payload using AES-256-GCM. The encrypted DEK is included in message headers; the broker sees only opaque ciphertext bytes.
* **Non-Negotiable Trade-Off**: Broker-side search and filtering limitations. The broker cannot filter messages based on encrypted payload contents without decrypting them.

---

#### Principle 2: Cryptographic Envelope Separation (DEK vs KEK)
* **Design Philosophy & Epistemology**:
  Directly calling a cloud KMS to encrypt every individual event introduces massive API latency, exhausts rate limits, and incurs unsustainable costs. The philosophy is Two-Tier Key Hierarchy: protect the data with ephemeral local Data Encryption Keys (DEKs) and protect the DEKs with a centralized Key Encryption Key (KEK).
* **Threat Vector Defeated**: Ingress latency spikes, cloud KMS throttling outages, and high API costs during high-throughput event bursts.
* **Storage & State Invariant**:
```text
EncryptedPayload = AES_GCM(Data, DEK)
EncryptedDEK = KMS.Encrypt(KEK, DEK)
KMS Calls <= Ceiling(TotalEvents / CacheBatchSize)
```
* **Implementation Mechanism**:
  Producers generate a cryptographic DEK locally in RAM. The DEK is encrypted via KMS using the master KEK and cached securely with a bounded Time-To-Live (e.g., 5 minutes). Message payloads are encrypted locally with the active DEK, reducing KMS API calls by 99.9%.
* **Non-Negotiable Trade-Off**: Key caching window. If a master KEK is revoked in KMS, cached local DEKs can continue encrypting/decrypting until their local TTL expires.

---

#### Principle 3: Cryptographic Shredding for Privacy Compliance (GDPR)
* **Design Philosophy & Epistemology**:
  In an immutable event log (Kafka, WORM storage), physically deleting a single event or customer record is architecturally prohibited. Attempting to delete individual events rewrites history and corrupts offset indexing. The philosophy is Erasure via Key Annihilation: encrypt personal data with a tenant- or user-specific key. When privacy regulations require deletion, destroy the key, rendering the historical data mathematically unrecoverable entropy.
* **Threat Vector Defeated**: Regulatory non-compliance fines from GDPR / CCPA privacy mandates due to the inability to delete user data from immutable event streams.
* **Storage & State Invariant**:
```text
Erasure(User_X) == DestroyKey(UserKey_X) ==> Decrypt(Data_X) is Mathematically Impossible
```
* **Implementation Mechanism**:
  Personal Identifiable Information (PII) is encrypted with a dedicated per-user DEK stored in a secure key vault. Upon receiving a verified GDPR Article 17 erasure request, the platform deletes the user's DEK from the vault. The immutable event log remains intact, but the user's data is permanently unreadable.
* **Non-Negotiable Trade-Off**: Key vault scale. Requires managing millions of discrete cryptographic keys across high-cardinality user bases.

---

#### Principle 4: Authenticated Encryption with Associated Data (AEAD)
* **Design Philosophy & Epistemology**:
  Confidentiality without authenticity invites tampering. An attacker who cannot read an encrypted payload might still tamper with ciphertext bits or swap headers to cause catastrophic application logic failures upon decryption. The philosophy is Authenticated Cryptography: the cryptographic signature must bind the ciphertext to its metadata headers.
* **Threat Vector Defeated**: Ciphertext bit-flipping attacks and header spoofing where an attacker modifies message metadata (e.g., tenant ID) while ciphertext remains unchanged.
* **Storage & State Invariant**:
```text
AEAD_Verify(Ciphertext, AssociatedData, Tag) == TRUE
where AssociatedData == TenantID || EventID || SchemaVersion
```
* **Implementation Mechanism**:
  The encryption engine uses AES-256-GCM. Critical message headers (`tenant_id`, `event_id`, `schema_version`) are passed as Additional Authenticated Data (AAD) into the GCM cipher. If an attacker tampers with either the payload or the headers, decryption immediately fails with an authentication error.
* **Non-Negotiable Trade-Off**: Slight CPU overhead for computing and verifying GCM authentication tags on every message hop.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why is disk encryption on Kafka brokers (LUKS / EBS encryption) insufficient for high-security environments?**
   * *Answer*: Disk encryption protects data only when the physical drives are stolen or decommissioned. Once the operating system boots and mounts the volume, any process, container, or administrator with broker access can read plaintext payloads. Application-layer encryption protects data everywhere.
2. **How does key rotation work for historical events stored in long-term Kafka topics?**
   * *Answer*: Historical events retain the encrypted DEK that was active at the time of creation. Rotating the master KEK in KMS re-encrypts the stored DEKs without requiring re-encryption of terabytes of historical payload data on disk.
3. **What happens if a consumer cannot reach the KMS to fetch the decryption key?**
   * *Answer*: The consumer pauses partition consumption and retries with exponential backoff. It must never commit the offset or route the message to DLQ unless the KMS explicitly responds that the key has been permanently destroyed.

---
## Authoritative References & Production Citations

* **[NIST Special Publication 800-57 Part 1: Recommendation for Key Management](https://csrc.nist.gov/publications/detail/sp/800-57-part-1/rev-5/final)**
  - *Architectural Rationale & Implementation Grounding*: Authoritative guidelines on cryptographic key lifecycles, key establishment, and envelope protection standards.
* **[RFC 5116: An Interface and Algorithms for Authenticated Encryption](https://www.rfc-editor.org/rfc/rfc5116)**
  - *Architectural Rationale & Implementation Grounding*: Defines Authenticated Encryption with Associated Data (AEAD) requirements and AES-GCM cipher standards.
* **[AWS Key Management Service (KMS) Cryptographic Details Whitepaper](https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#enveloping)**
  - *Architectural Rationale & Implementation Grounding*: Definitive production architecture for envelope encryption, key wrapping, and high-throughput data protection.
* **[PCI-DSS v4.0 Requirement 3: Protect Stored Account Data](https://www.pcisecuritystandards.org/)**
  - *Architectural Rationale & Implementation Grounding*: Mandates strong cryptography and key-management processes for stored cardholder and sensitive authentication data.

## Amendment 2026-11-07: Envelope success flag on decrypt failures

### Problem observed
`POST` decrypt returned HTTP 403 (outcome `UNAUTHORIZED`) or HTTP 400 (outcomes `TAMPERED`, `MALFORMED`) while the JSON envelope said `"success": true`. Clients and gateways that key off the envelope flag treated a denied or tampered decryption as success.

### Root cause
`EncryptionHandler.writeResponse` hard-coded `Success: true` for every call routed through it. `DecryptField` sets the HTTP status from `out.Outcome` after the service returns without error, then reuses `writeResponse`, so the status and the envelope disagreed (RC5: handler-level contract never tested).

### Decision / fix
Envelope success rule: `success = (HTTP status < 400)`. `writeResponse` now sets `Success: status < http.StatusBadRequest`, so any response the handler emits with a 4xx/5xx status carries `success:false`. The decrypt `data` payload (including the outcome) is still returned alongside it. The HTTP status mapping itself is unchanged.

### API-visible behavior change
Decrypt with UNAUTHORIZED/TAMPERED/MALFORMED: envelope `success` changes from `true` to `false` (status codes unchanged at 403/400). **Breaking** for any client that ignored the status code and branched on `success`.

### Verification
Re-tested live: unauthorized decrypt returns 403 with `success:false`. No new unit test was added for the envelope flag.

### Remaining gaps
No automated handler-level test for the flag. Other features' envelopes were not audited in this change for the same pattern.

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

### 3. Core Architectural Principles

* **Zero-Trust Envelope Encryption**: Sensitive payloads must be encrypted before transmission using ephemeral DEKs wrapped by master KMS keys.
* **Authenticated Symmetric Cryptography**: All encryption must enforce AES-256-GCM AEAD, verifying that ciphertext has not been tampered with in transit.
* **Cryptographic Shredding by Design**: Key lifecycles must be partitioned by tenant or subject, enabling instant regulatory data erasure through key deletion.
* **Decoupled Master Key Rotation**: Rotating master KEKs in KMS must only require re-wrapping stored DEKs, never re-encrypting petabytes of historical message data.

### 4. The Architect's Meta-Questions (Failure-First Evaluation)

1. **Why use Envelope Encryption instead of calling KMS directly for every payload? (KMS APIs are rate-limited and expensive; envelope encryption performs heavy encryption locally with ephemeral DEKs, calling KMS only for small key wrapping).**
2. **How does envelope encryption enable instant GDPR 'Right to be Forgotten'? (By deleting the tenant or subject DEK from the key registry, historical messages on Kafka logs and backups become permanent, unrecoverable ciphertext).**
3. **Why must we use Authenticated Encryption (AES-GCM) rather than basic AES-CBC? (AEAD ciphers guarantee both secrecy and cryptographic authenticity, preventing chosen-ciphertext bit-flipping attacks on encrypted message payloads).**

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

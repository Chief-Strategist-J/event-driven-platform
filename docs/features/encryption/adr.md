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

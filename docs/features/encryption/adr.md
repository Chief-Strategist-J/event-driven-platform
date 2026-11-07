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

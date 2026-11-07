# Encryption Failure Catalog

| Failure ID | Scenario | Severity | Impact | Prevention / Mitigation |
|------------|----------|----------|--------|-------------------------|
| E2.1 | "Encrypted at rest" assumed to protect against insider DB users | SECURITY | Database admins or backup leakers access plaintext PII | Field-level envelope encryption (EN-01, EN-08) |
| E3.1 | Hostname verification disabled in TLS client | SECURITY | Eavesdropper or MITM proxy intercepts plaintext in transit | Strict TLS 1.2+ configuration with SAN/hostname verification |
| E3.2 | Replication / CDC connection left unencrypted | SECURITY | Unencrypted network traffic between database nodes | Require SSL/TLS for all internal CDC and replication channels |
| E3.3a | Ingress certificate expires | OUTAGE | External traffic rejected; total API disruption | Automated cert-manager renewal + expiry alerts at 30/14/7/1 days |
| E3.3b | SPIRE workload agent down longer than SVID TTL | OUTAGE | Inter-service mTLS calls fail across entire mesh | SVID TTL configured $\ge 3\times$ recovery SLA; HA SPIRE servers |
| E3.3c | CA rotated without cross-trust overlap | OUTAGE | Nodes with old CA reject nodes with new CA; split-brain | Dual-root trust store period during CA migration |
| E4.1 | Database snapshot restored into lower environment | COMPLIANCE | Production PII exposed to developers in QA/dev environments | Field-level encrypted ciphertexts remain unreadable without prod KMS keys |
| E4.3 | Core or heap dump written to unencrypted storage | SECURITY | Plaintext DEK keys leaked into crash diagnostics | Disable core dumps in prod; access-controlled ephemeral heap dump storage |
| E4.4 | Customer-managed KMS key disabled accidentally | OUTAGE | Database or field-level data immediately unreadable | Key deletion protection (EN-11) + key state alerts |
| E5.2a | Single shared DEK used across multiple data subjects | COMPLIANCE | Shredding the key erases multiple customers or prevents individual RTBF | Per-subject DEK isolation (EN-01) in `encryption_dek_ledger` |
| E5.4a | Ciphertext copied from Subject A into Subject B's record | CORRUPT / SECURITY | Ciphertext decrypts under wrong subject without error | Mandatory AAD binding (EN-02) including tenant, subject, and field path |
| E5.4b | AAD includes transient event ID | CORRUPT | Field cannot be copied into read projections or derived events | Scope AAD to stable entity attributes (tenant, subject, aggregate, field) |
| E5.4c | Field renamed during schema evolution | CORRUPT | AAD mismatch on historical data causes decryption failure | AAD maps canonical field identity/tag, not transient field names |
| E5.5 | Nonce reuse under AES-256-GCM | SECURITY | Reusing 96-bit nonce under same key leaks plaintext XOR and enables forgery | Cryptographically secure random 96-bit nonces (CSPRNG) + periodic DEK rotation |
| E6.1 | Encryption deferred to async relay or CDC poller | SECURITY | Outbox table and database WAL hold plaintext PII | Application encrypts in-memory before writing outbox row (EN-08) |
| E6.2 | Request payload logged before field-level encryption | COMPLIANCE | PII leaked into log aggregators (ELK, Datadog) | Structured logging with PII-tag masking; leak scanners on log streams |
| E6.3 | Concurrent writers create two DEKs for same new subject | CORRUPT | Data split across two DEKs; erasure shreds only one | PostgreSQL UNIQUE `(tenant_id, subject_id)` constraint |
| E6.4 | KMS API rate-limiting during high-volume ingest or bulk jobs | OUTAGE | Ingestion halts due to KMS throttling | Local plaintext DEK caching with bounded TTL (5-15 min) |
| E6.5 | Producer encrypts with incorrect subject ID | COMPLIANCE | Data cannot be erased under true subject's RTBF request | Derive subject ID strictly from authenticated aggregate ownership |
| E7.1 | Decryption error silently mapped to `NULL` | CORRUPT / LOSS | Transient KMS outage permanently wipes customer profiles in views | Strict 6-outcome machine (EN-05); `NULL` mapping strictly forbidden |
| E7.2 | Plaintext arrives for PII-tagged field | SECURITY | Unencrypted data ingested into downstream systems | Fail-closed policy: quarantine and alert on unexpected plaintext (EN-04) |
| E7.3 | Replay of historical events overwhelms KMS quota | OUTAGE | Live services starved of KMS quota; system-wide degradation | In-memory DEK caching + replay rate-limiting |
| E7.4 | Erasure registry lags behind DEK destruction | DEGRADE | Missing key falsely flagged as accidental corruption | Key destruction and erasure registry update in single atomic workflow |
| E7.5 | Erased marker written into numeric aggregation view | CORRUPT | Math/financial calculations fail on string marker | PII fields strictly forbidden in financial calculation aggregates |
| E8.1 | Unkeyed SHA-256 hash used as searchable index for email | SECURITY | Dictionary/rainbow-table attack re-identifies customer emails | Keyed HMAC blind indexes (C-BIX) with tenant/field-specific secrets |
| E8.2 | Search engine stores decrypted PII without erasure pipeline | COMPLIANCE | Subject RTBF erasure leaves plaintext in search cluster | Search cluster indexes blind index hashes or listens to erasure stream |
| E9.1 | Database backup restore resurrects shredded DEKs | COMPLIANCE | Previously forgotten subject becomes readable again | Re-apply crypto-shredding audit log upon database restore |
| E9.2 | DR region lacks KMS key grant | OUTAGE | Region failover succeeds but services cannot decrypt data | Cross-region KMS multi-region key replication |
| E10.1 | Single shared KEK used across all service workloads | SECURITY | Compromised peripheral service decrypts core customer data | Role-gated KEK policies and field-class decryption entitlements |
| E12.1 | Crypto library upgrade alters AAD serialization silently | OUTAGE | Historical ciphertexts fail authentication tag check | Cross-version golden ciphertext test suite in CI |

| E13.1 | Decrypt denied or failed but envelope reports `success:true` (Amendment 2026-11-07) | SECURITY | Callers branching on the envelope flag treat unauthorized/tampered/malformed decrypt as success | Envelope `success` is `status < 400`; decrypt returns 403 `UNAUTHORIZED` / 400 `TAMPERED` / 400 `MALFORMED` with `success:false`. Operator: alert on 403/400 decrypt rates (possible probing or corruption). |

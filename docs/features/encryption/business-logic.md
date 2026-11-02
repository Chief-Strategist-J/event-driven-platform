# Encryption — Business Logic Decision Tree

```
ENCRYPTION (BUSINESS DECISION TREE)
│
├── 1. ENCRYPTION IN TRANSIT & AT REST ARCHITECTURE
│   ├── Layer A: Encryption in Transit (Wire Security)
│   │   ├── External Ingress (Client -> Envoy Gateway):
│   │   │   └── Standard: TLS 1.3 with strong cipher suites; HTTPS (:8080) and mTLS (:8443)
│   │   ├── Inter-Service Communication (Envoy -> Microservices):
│   │   │   └── Standard: mTLS with SPIFFE/SPIRE workload identities
│   │   └── Event Bus Transport (Producers / Consumers -> Kafka):
│   │       └── Standard: TLS on port 9093 / 9092 with mutual certificate validation
│   │
│   └── Layer B: Encryption at Rest (Storage Infrastructure)
│       ├── PostgreSQL: LUKS block-level encryption + Cloud SQL Google-managed or CMEK encryption
│       ├── Apache Kafka: EBS / PersistentVolume encrypted at rest via KMS customer managed keys
│       ├── OpenSearch: Native index encryption at rest (`index.opendistro.security.encryption.at_rest`)
│       └── MinIO / S3: Server-side encryption (SSE-S3 / SSE-KMS) with automated key rotation
│
├── 2. APPLICATION-LEVEL FIELD ENCRYPTION (PII / SENSITIVE FIELDS)
│   └── Trigger: Producer Prepares Event Payload Containing PII / Sensitive Data
│       ├── Check: Does event payload contain PII (e.g. SSN, credit card, phone, address)?
│       │   ├── NO ──> Serialize payload as standard Protobuf / Avro binary
│       │   └── YES ──> Enforce Application Field-Level Encryption (Envelope Encryption)
│       │
│       ├── Envelope Encryption Protocol:
│       │   ├── Step 1: Request Data Encryption Key (DEK)
│       │   │   └── KMS / Vault returns: Plaintext DEK + KMS-Encrypted DEK (`CipherDEK`)
│       │   │
│       │   ├── Step 2: Encrypt Sensitive Fields Locally in Producer Memory
│       │   │   ├── Algorithm: AES-256-GCM (Authenticated Encryption with Associated Data - AEAD)
│       │   │   ├── Ciphertext Structure: `Base64(Nonce || Ciphertext || AuthTag)`
│       │   │   └── Clear Plaintext: Immediately zero memory buffer of plaintext DEK
│       │   │
│       │   └── Step 3: Inject Key Metadata into Record Headers
│       │       ├── Header `X-Encrypted-Fields`: `["email", "billing_address", "phone"]`
│       │       ├── Header `X-KMS-Key-ID`: `arn:aws:kms:...:key/12345`
│       │       └── Header `X-Ciphertext-DEK`: Base64 encoded encrypted DEK
│       │
│       └── Invariant: Structural routing metadata (`event_id`, `aggregate_id`, `version`) remains unencrypted
│
└── 3. CONSUMER DECRYPTION & CRYPTO-SHREDDING COMPLIANCE
    └── Trigger: Downstream Consumer Deserializes Encrypted Record
        ├── Step 1: Header Check for `X-Encrypted-Fields`
        │   ├── Not Present ──> Standard deserialization
        │   └── Present ──> Enter Decryption Subsystem
        │
        ├── Step 2: Key Resolution & Decryption
        │   ├── Consumer calls KMS / Vault `Decrypt(CipherDEK)`
        │   ├── KMS Access Control Evaluation:
        │   │   ├── Authorized Consumer Role ──> KMS decrypts and returns plaintext DEK
        │   │   ├── Unauthorized Role ──> Access Denied 403 (Consumer cannot inspect PII fields)
        │   │   └── Key Destroyed (Crypto-Shredded Subject):
        │   │       ├── KMS returns: `KeyNotFoundException`
        │   │       └── Resolution: Subject PII permanently erased; consumer treats PII fields as NULL
        │   │
        │   └── Step 3: AES-256-GCM In-Memory Decryption
        │       ├── Decrypt ciphertext fields using resolved DEK
        │       ├── Authenticate AuthTag to detect data tampering
        │       └── Yield decrypted domain event to consumer handler
```

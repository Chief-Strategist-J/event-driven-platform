# Encryption Architecture Diagrams

## 1. Envelope Encryption Hierarchy & Outbox Flow

```mermaid
flowchart TD
    subgraph KMS["External Key Management Service (C-KMS)"]
        KEK["Tenant Root Key (KEK)\nKMS / HSM (Never Leaves HSM)"]
    end

    subgraph KeyLedger["Key Ledgers (PostgreSQL)"]
        KEK_REG[("encryption_kek_ledger\nKMS Key ARNs & Versions")]
        DEK_REG[("encryption_dek_ledger\nPer-Subject Wrapped DEKs")]
        SHRED_REG[("crypto_shredding_audit_ledger\nErasure Audit History")]
    end

    subgraph Application["Producer Command Pipeline"]
        PLAIN["Raw Business Event (PII Fields)"]
        AAD_GEN["Build Contextual AAD\n(Tenant + Subject + Aggregate + Field)"]
        CRYPTO["AES-256-GCM Encrypt"]
        ENC_VAL["EncryptedValue Object\n(scheme, key_ref, nonce, ciphertext, tag)"]
        OUTBOX["Transactional Outbox Table\n(Stored strictly as EncryptedValue)"]
    end

    KEK -->|Wrap / Unwrap| DEK_REG
    DEK_REG -->|Cached Plaintext DEK| CRYPTO
    PLAIN --> CRYPTO
    AAD_GEN --> CRYPTO
    CRYPTO --> ENC_VAL
    ENC_VAL --> OUTBOX
```

## 2. Authenticated Additional Data (AAD) Construction

```mermaid
flowchart LR
    subgraph AAD_Components["Context Elements"]
        T["Tenant ID"]
        S["Subject ID"]
        A["Aggregate ID"]
        F["Field Path (e.g. user.contact.phone)"]
        V["Schema Version"]
        K["Key Reference"]
    end

    subgraph BoundCiphertext["Cryptographic Binding"]
        AAD_STR["Canonical Byte String\n(Deterministic Delimiter)"]
        AES["AES-256-GCM AEAD Engine"]
        CIPHER["Encrypted Payload + Auth Tag"]
    end

    T --> AAD_STR
    S --> AAD_STR
    A --> AAD_STR
    F --> AAD_STR
    V --> AAD_STR
    K --> AAD_STR

    AAD_STR -->|Associated Data (AAD)| AES
    AES --> CIPHER
```

## 3. Decryption Outcome Decision Machine (EN-05)

```mermaid
flowchart TD
    START(["Incoming EncryptedValue"]) --> C1{"Field Tagged as PII?"}
    C1 -- "No (Plaintext)" --> PLAINTEXT["Quarantine: PLAINTEXT-UNEXPECTED\nRaise Security Incident"]
    C1 -- "Yes" --> C2{"Consumer Entitled?"}

    C2 -- "No" --> UNAUTH["Return UNAUTHORIZED\n(Field omitted/masked, not NULL)"]
    C2 -- "Yes" --> C3{"Subject State in DEK Ledger?"}

    C3 -- "SHREDDED" --> ERASED["Return ERASED\n(Subject Crypto-Shredded)"]
    C3 -- "ACTIVE / ROTATED" --> C4{"KMS / Cache Available?"}

    C4 -- "No / Timeout" --> UNAVAIL["Return UNAVAILABLE\n(Pause / Retry Later, Never Wipe)"]
    C4 -- "Yes" --> C5{"AEAD Tag & AAD Match?"}

    C5 -- "Mismatch" --> TAMPERED["Return TAMPERED\n(Security Alert, Quarantine)"]
    C5 -- "Match" --> DECRYPTED(["Return DECRYPTED\n(Plaintext Handled In-Memory)"])
```

## 4. Crypto-Shredding Subject Erasure Flow

```mermaid
sequenceDiagram
    autonumber
    actor Compliance as DPO / Privacy Workflow
    participant SVC as Encryption Service
    participant DEK_DB as encryption_dek_ledger
    participant AUDIT as crypto_shredding_audit_ledger
    participant CACHE as In-Memory DEK Cache
    participant CONSUMER as Downstream Consumer

    Compliance->>SVC: Request Erasure (TenantID, SubjectID, Reason)
    SVC->>AUDIT: Log Immutable Shredding Event (csa_*)
    SVC->>DEK_DB: Update DEK state = SHREDDED, zeroize encrypted_dek_blob
    DEK_DB-->>SVC: Acknowledged
    SVC->>CACHE: Invalidate Cached Plaintext DEK
    SVC-->>Compliance: Subject Successfully Crypto-Shredded

    Note over CONSUMER, DEK_DB: Later: Replay or Query meets Historical Event
    CONSUMER->>SVC: Decrypt Subject Event
    SVC->>DEK_DB: Lookup DEK
    DEK_DB-->>SVC: Status: SHREDDED
    SVC-->>CONSUMER: Return Status: ERASED (Zero Plaintext Leaked)
```

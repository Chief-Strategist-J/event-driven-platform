# Audit Trail Architecture Diagrams

## 1. System Topology & Capture Pipeline

```mermaid
flowchart TD
    subgraph Client["Execution Sources"]
        CMD["Command Handlers (Class A)"]
        AUTH["Auth & Sessions (Class B)"]
        ADM["Admin Tooling (Class C)"]
    end

    subgraph ServiceTx["Primary Database Transaction"]
        BIZ["Business State Mutation"]
        AOB["Audit Outbox (C-AOB)"]
    end

    subgraph Storage["Storage & Anchoring Tiers"]
        LED[("Audit Ledger (C-LED)\nPostgreSQL (Append-Only BYTEA)")]
        CHK[("Checkpoint Registry\nSigned Shard Intervals")]
        WORM[("Immutable WORM Store (C-ANC)\nS3 Object Lock")]
        SIEM["SIEM & Log Analytics (C-BUS / Kafka)"]
    end

    CMD --> BIZ
    CMD --> AOB
    BIZ -.->|Atomic Commit| AOB
    AOB -->|Ingestion Relay| LED
    AUTH -->|Durable Spool| LED
    ADM -->|Direct Fail-Closed| LED

    LED -->|Periodic Sealing| CHK
    CHK -->|Anchored Export| WORM
    LED -.->|Asynchronous Fanout| SIEM
```

## 2. Sharded Cryptographic Hash Chain Structure

```mermaid
flowchart LR
    subgraph Shard01["Shard 01 (Tenant Alpha)"]
        direction LR
        R1["Seq 1\nHash: H(Genesis || P1)"] --> R2["Seq 2\nHash: H(R1_Hash || P2)"]
        R2 --> R3["Seq 3\nHash: H(R2_Hash || P3)"]
    end

    subgraph Shard02["Shard 02 (Tenant Beta)"]
        direction LR
        S1["Seq 1\nHash: H(Genesis || Q1)"] --> S2["Seq 2\nHash: H(S1_Hash || Q2)"]
        S2 --> S3["Seq 3\nHash: H(S2_Hash || Q3)"]
    end

    R3 --> CP1["Checkpoint 1\nRoot: Merkle(R1..R3)\nSigned by KMS"]
    S3 --> CP2["Checkpoint 2\nRoot: Merkle(S1..S3)\nSigned by KMS"]

    CP1 --> WORM_ANC["WORM Anchor Store"]
    CP2 --> WORM_ANC
```

## 3. Cryptographic Verification State Machine

```mermaid
stateDiagram-v2
    [*] --> PENDING_VERIFICATION

    PENDING_VERIFICATION --> COMPUTING_HASH_CHAIN : Load Shard Records
    COMPUTING_HASH_CHAIN --> COMPARING_ANCHOR : Chain Computed Locally

    COMPARING_ANCHOR --> INTACT : Head & Merkle Match Anchored Checkpoint
    COMPARING_ANCHOR --> MISMATCH : Hash Divergence Detected (Tampering)
    COMPARING_ANCHOR --> TRUNCATION_DETECTED : Current Sequence < Anchored Sequence
    COMPARING_ANCHOR --> UNKNOWN : Anchor Store Unreachable / Clock Skew

    MISMATCH --> SECURITY_INCIDENT_RAISED : Raise Alert RB-01
    TRUNCATION_DETECTED --> SECURITY_INCIDENT_RAISED : Raise Alert RB-01
    UNKNOWN --> RETRY_VERIFICATION : Backoff & Re-probe
    RETRY_VERIFICATION --> PENDING_VERIFICATION

    INTACT --> [*]
    SECURITY_INCIDENT_RAISED --> [*]
```

## 4. Query-Audit & Forensics Flow (AU-11)

```mermaid
sequenceDiagram
    autonumber
    actor Auditor as Compliance Auditor
    participant C_QRY as Forensic Query Service
    participant ACL as Access Control & Case Gate
    participant LOG as Audit Access Log (AU-11)
    participant LED as Audit Ledger (C-LED)

    Auditor->>C_QRY: Request Records (Tenant, Shard, Timerange)
    C_QRY->>ACL: Validate Role & Active Case ID
    ACL-->>C_QRY: Authorized (Masking Rules Applied)
    C_QRY->>LOG: Write T-ACCESS Log (Auditor, Query, Timestamp, Reason)
    LOG-->>C_QRY: Confirmed Logged
    C_QRY->>LED: Fetch Immutable Bytes & Re-verify Hash
    LED-->>C_QRY: Return Raw Verbatim Bytes
    C_QRY-->>Auditor: Return Masked Audit Records + Integrity Proof
```

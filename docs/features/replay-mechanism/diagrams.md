# Replay Mechanism — Architecture Diagrams & Workflows

This document visualizes the core architecture, runtime lifecycles, side-effect governance, and zero-downtime cutover mechanics of the Replay Mechanism.

---

## 1. High-Level Component Interaction Map

The Replay Orchestrator provisions an isolated consumer group (`RP-01`), streams historical and live event logs, and guarantees that external mutations strictly pass through the Effect Gateway (`RP-02`, `RP-03`).

```mermaid
flowchart TD
    subgraph StorageSources ["Replay Event Sources"]
        S1["Kafka Live Retention"]
        S2["Kafka Tiered Storage"]
        S3["Cold Storage Archive (Raw Envelopes)"]
    end

    subgraph PlatformCore ["Platform Infrastructure"]
        ORC["Replay Orchestrator (C-ORC)"]
        EFG["Effect Gateway (C-EFG)"]
        PAR["Parity Validator (C-PAR)"]
        CUT["Cutover Controller (C-CUT)"]
    end

    subgraph ConsumerSidecar ["Replay Consumer Runtime"]
        RUN["Replay Runner (C-RUN)"]
        GRP["Sidecar Group: replay_run_id"]
    end

    subgraph TargetStores ["Target Projections"]
        SHADOW["Shadow Projection (Target Table / Index)"]
        EFFECTLOG["Replay Effect Log"]
    end

    StorageSources --> RUN
    ORC -->|Deploys manifest & config| RUN
    RUN -->|State mutations| SHADOW
    RUN -->|Side-effect dispatch requests| EFG
    EFG -->|SUPPRESS / MOCK / RECORD| EFFECTLOG
    PAR -->|Compares at version V| SHADOW
    CUT -->|Atomic pointer switch| SHADOW
```

---

## 2. Replay Run Lifecycle State Machine

The replay lifecycle progresses through six sequential operational states, terminating in `COMPLETED` or aborting to `ABORTED`.

```mermaid
stateDiagram-v2
    [*] --> INITIALIZING: Manifest submitted & approved
    INITIALIZING --> STREAMING: Sidecar consumer group joined & sources mounted
    STREAMING --> FOLLOWING: Historical log caught up to live retention window
    FOLLOWING --> CAUGHT_UP: Lag below live threshold (liveLagThreshold)
    CAUGHT_UP --> PROMOTED: Approved for cutover (reads dual-routing)
    PROMOTED --> COMPLETED: Pointer swapped & old projection decommissioned

    INITIALIZING --> ABORTED: Validation failure or manual abort
    STREAMING --> ABORTED: SLO breach or data integrity failure
    FOLLOWING --> ABORTED: Lag runaway or parity divergence
    CAUGHT_UP --> ABORTED: Parity mismatch or cutover timeout
    PROMOTED --> ABORTED: Post-cutover error spike (instant rollback)
    ABORTED --> [*]
    COMPLETED --> [*]
```

---

## 3. Side-Effect Gateway Decision Engine

External effects (SMS, email, payments, outbox publication, cache invalidation) are routed according to the active declared run mode.

```mermaid
flowchart TD
    A["Event Handler Invokes Effect"] --> B{"Current Run Mode?"}
    
    B -->|LIVE| C["Execute Directly Against External System"]
    B -->|REBUILD or HYDRATE| D["Effect Gateway Intercepts"]
    B -->|FORENSIC| E["Enforce Strict Read-Only Suppression"]
    B -->|RECOVER| F["Reconcile Mode: Check Remote State"]

    D --> G{"Effect Type?"}
    G -->|Outbox Event| H["Suppress or Write to Shadow Outbox"]
    G -->|Third-Party API| I["Mock with Recorded Response or Null Object"]
    G -->|Notification| J["Suppress and Log to Replay Effect Log"]

    F --> K{"Remote Mutation Happened?"}
    K -->|Yes| L["Skip Dispatch and Record in Effect Log"]
    K -->|No| M["Execute Real External Dispatch"]
```

---

## 4. Dual-Run Atomic Cutover and Rollback Topology

During the promotion window, writes are dual-directed or handed off while maintaining immediate zero-downtime rollback capability.

```mermaid
sequenceDiagram
    participant API as Ingress API Router
    participant LIVE as Live Projection (V1)
    participant SHADOW as Shadow Projection (V2)
    participant CUT as Cutover Controller
    participant ORC as Replay Orchestrator

    Note over API,LIVE: Normal Operations: API routes queries to V1
    ORC->>CUT: Signal Run CAUGHT_UP (Lag <= Threshold)
    CUT->>SHADOW: Validate Parity Verification (C-PAR)
    CUT->>API: Atomic Pointer Switch (V1 -> V2)
    Note over API,SHADOW: Post-Cutover: API routes queries to V2
    alt Observation Window Normal
        CUT->>ORC: Complete Cutover (Transition to COMPLETED)
        ORC->>LIVE: Decommission V1 after retention window
    else Anomaly Detected during Observation
        CUT->>API: Immediate Rollback Pointer Switch (V2 -> V1)
        CUT->>ORC: Abort Replay Run (Transition to ABORTED)
    end
```


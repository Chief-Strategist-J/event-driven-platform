# Architectural Decision Record: Cooperative Consumer Groups & Static Membership (WP-6)

## 1. Title & Metadata
* **Status**: APPROVED
* **Date**: 2026-11-04
* **Decision Makers**: Platform Architecture Guild, Chief Distributed Systems Architect
* **Governing Specification**: [Consumer Groups Decision Tree](file:///home/btpl-lap-22/live/System-Architect/event-driven-platform/docs/features/consumer-groups/business-logic.md)

---

## 2. Context & Problem Statement
In high-throughput event-driven microservices running on Kubernetes, consumer instances frequently restart during rolling updates, node drains, and autoscaling.

Legacy Kafka consumer group configurations create severe outages:
1. **Stop-the-World Eager Rebalances**: Banned assignors (`RangeAssignor`, `RoundRobinAssignor`) revoke every partition across all live consumers when a single pod restarts, halting stream processing globally.
2. **Rebalance Storms from Ephemeral Identity**: Dynamic membership causes pods to leave and rejoin under randomized member IDs, triggering cascading rebalances.
3. **Split-Brain Concurrent Writes**: When a terminating pod experiences GC or drain pauses, it continues writing events after the Kafka coordinator has assigned its partitions to another pod.
4. **Poll Loop Deadlock**: Handlers executing slow processing exceed `max.poll.interval.ms` (300s), causing the coordinator to evict the consumer and trigger repeated rebalance loops.

---

## 3. Decision Drivers & Invariants
* **Mandatory Cooperative Rebalancing**: Only `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` is permitted. Eager assignors are strictly banned. Unaffected partitions continue processing uninterrupted during member joins/leaves.
* **Static Membership (`group.instance.id`)**: Pod names (e.g. `orders-worker-0`) act as static member identifiers. Restarts within `session.timeout.ms` (45s) do not trigger rebalances.
* **Partition Generation Epoch Fencing**: Partition assignments carry a monotonic generation ID stored in PostgreSQL table `consumer_group_member`. Stale workers cannot commit writes.
* **Watchdog Execution Guard**: If batch processing duration reaches 80% of `max.poll.interval.ms` (240s), partition consumption pauses immediately to prevent broker eviction.
* **Batch Ingestion Limits**: `max.poll.records = 500` and `fetch.max.bytes = 50MB`.

---

## 4. Entity-Relationship Data Model & Database Writing Rules
Complies with [05-db-writing-rules.md](file:///home/btpl-lap-22/live/System-Architect/llm-obs-policies/rules/folder-structure/05-db-writing-rules.md):

```mermaid
erDiagram
    consumer_group_member {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (cgm_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        varchar group_id "Kafka consumer group name"
        varchar member_id "Kafka member identifier"
        varchar instance_id "Static membership pod identifier"
        varchar client_host "Consumer host IP or hostname"
        varchar assignment_strategy "Must be CooperativeStickyAssignor"
        jsonb assigned_partitions "List of assigned topic partitions"
        bigint generation_id "Rebalance generation epoch"
        timestamptz heartbeat_at "Latest heartbeat timestamp"
        varchar status "JOINING, STABLE, REBALANCING, DEAD"
        timestamptz created_at "Record creation timestamp"
        timestamptz updated_at "Record update timestamp"
    }

    consumer_group_rebalance_log {
        uuid id PK "Role 1: UUIDv7 surrogate identifier"
        varchar public_id UK "Role 2: External opaque ID (cgr_...)"
        varchar tenant_id PK "Role 3: Tenant scope & primary partition key"
        varchar source_service UK "Role 4A: Upstream service producer"
        varchar external_id UK "Role 4B: Upstream natural key"
        uuid correlation_id "Role 5: OpenTelemetry trace correlation"
        varchar group_id "Kafka consumer group name"
        bigint generation_id "Generation epoch after rebalance"
        varchar rebalance_reason "Reason for rebalance trigger"
        int member_count "Number of active members"
        jsonb revoked_partitions "Partitions revoked during rebalance"
        jsonb assigned_partitions "Partitions assigned during rebalance"
        bigint duration_ms "Rebalance protocol latency milliseconds"
        timestamptz created_at "Record creation timestamp"
    }
```

---

## 5. Execution Sequence Diagrams

### 5.1 Cooperative Rebalance Protocol
```mermaid
sequenceDiagram
    autonumber
    actor Coord as Kafka Group Coordinator
    participant Pod1 as Consumer Pod 0 (Existing)
    participant Pod2 as Consumer Pod 1 (Joining)
    participant DB as consumer_group_member

    Note over Pod1: Assigned P0, P1, P2 (Generation 1)
    Pod2->>Coord: JoinGroup (CooperativeStickyAssignor)
    Coord->>Pod1: Rebalance Notification (Revoke only P2)
    
    rect rgb(235, 255, 235)
        Note over Pod1: Invariant: P0 and P1 continue processing!
        Pod1->>Pod1: Flush P2 in-flight tx & CommitSync(P2)
        Pod1->>DB: Update generation 2 (retaining P0, P1)
    end

    Coord->>Pod2: Assign P2 (Generation 2)
    Pod2->>DB: RegisterMember (assigned P2, Generation 2)
    Note over Pod2: Pod 1 and Pod 2 now stream in parallel without global stall
```

### 5.2 Static Membership Pod Restart (Zero Rebalances)
```mermaid
sequenceDiagram
    autonumber
    actor Kubelet as Kubernetes Kubelet
    actor Coord as Kafka Group Coordinator
    participant Pod as Consumer Pod 0 (Static ID: pod-0)
    
    Kubelet->>Pod: SIGTERM (Rolling deployment)
    Note over Pod: Static membership: Pod does NOT send LeaveGroupRequest!
    Pod->>Coord: CommitSync latest offsets & Disconnect
    
    rect rgb(240, 240, 255)
        Note over Coord: Pod-0 is missing heartbeats but within session.timeout (45s)
        Note over Coord: Zero rebalance triggered! Group remains STABLE.
    end

    Kubelet->>Pod: Launch Replacement Container (Same instance_id: pod-0)
    Pod->>Coord: JoinGroup (group.instance.id = pod-0)
    Coord-->>Pod: Rejoin Accepted without rebalance! Retains P0, P1.
```

# Capacity Model & Back-of-the-Envelope Sizing — Event-Driven Platform

## 1. High-Level Workload Assumptions

| Parameter | Baseline Value | Peak Value | Notes |
|---|---|---|---|
| **Event Ingestion Rate** | $15,000$ events/sec | $50,000$ events/sec | $3.33\times$ peak-to-average ratio |
| **Average Payload Size** | $1.5$ KB | $4.0$ KB | JSON business domain payload |
| **Envelope & Headers** | $0.5$ KB | $0.8$ KB | W3C headers, traceparent, UUIDs |
| **Total Uncompressed Event Size** | **$2.0$ KB** | **$4.8$ KB** | Average across all topic classes |
| **Compression Ratio (Snappy/Zstd)**| **$2.5:1$** ($60\%$ reduction) | $2.0:1$ | Realistic for structured JSON |
| **Compressed On-Wire/Disk Size** | **$0.8$ KB** | **$2.4$ KB** | Stored on broker disk and network transit |
| **Replication Factor ($RF$)** | $3$ | $3$ | `min.insync.replicas = 2` |
| **Hot Retention Window** | $7$ days | $7$ days | High-speed local NVMe SSD |
| **Cold / Tiered Retention** | $30$ days | $90$ days | Object store (S3 / GCS) via Tiered Storage |

---

## 2. Ingestion Throughput & Bandwidth Math

### Network Ingress (Brokers)

At peak ingestion ($50,000$ events/sec):

$$\text{Raw Ingress Bandwidth} = 50,000 \text{ events/sec} \times 2.0 \text{ KB} = 100,000 \text{ KB/sec} = 100 \text{ MB/sec} = 800 \text{ Mbps}$$

With client-side compression (Snappy / Zstd):

$$\text{Compressed Ingress Bandwidth} = 50,000 \text{ events/sec} \times 0.8 \text{ KB} = 40,000 \text{ KB/sec} = 40 \text{ MB/sec} = 320 \text{ Mbps}$$

### Inter-Broker Replication Bandwidth

With $RF = 3$, every leader partition replicates data to 2 follower brokers:

$$\text{Replication Bandwidth} = \text{Compressed Ingress} \times (RF - 1) = 40 \text{ MB/sec} \times 2 = 80 \text{ MB/sec} = 640 \text{ Mbps}$$

### Total Broker Network Traffic

$$\text{Total Broker Network Ingress} = \text{Client Ingress} + \text{Replication Ingress} = 40 + 80 = 120 \text{ MB/sec} = 960 \text{ Mbps}$$

$$\text{Consumer Egress (assuming 4 consumer groups average fan-out)} = 4 \times 40 \text{ MB/sec} = 160 \text{ MB/sec} = 1.28 \text{ Gbps}$$

$$\text{Total Cluster Network IO (Peak)} = 120 \text{ MB/sec (in)} + 160 \text{ MB/sec (out)} = 280 \text{ MB/sec} = 2.24 \text{ Gbps}$$

---

## 3. Storage & Disk Retention Sizing

### Daily Event Volume

$$\text{Events per Day (Average)} = 15,000 \text{ events/sec} \times 86,400 \text{ sec/day} = 1,296,000,000 \approx 1.30 \text{ Billion events/day}$$

$$\text{Events per Day (Sustained Peak)} = 50,000 \text{ events/sec} \times 86,400 \text{ sec/day} = 4,320,000,000 = 4.32 \text{ Billion events/day}$$

### Daily Disk Storage Consumption

$$\text{Compressed Daily Ingest} = 4.32 \times 10^9 \text{ events} \times 0.8 \text{ KB} = 3,456,000 \text{ MB} = 3.456 \text{ TB/day (single replica)}$$

Factoring in Replication Factor $RF = 3$:

$$\text{Total Daily Storage Write} = 3.456 \text{ TB} \times 3 = 10.368 \text{ TB/day}$$

### 7-Day Hot NVMe SSD Retention

$$\text{7-Day Hot Storage} = 10.368 \text{ TB/day} \times 7 \text{ days} = 72.576 \text{ TB}$$

Adding a **$30\%$ safety headroom buffer** for rebalances, index segments, and traffic spikes:

$$\text{Required Cluster NVMe Capacity} = 72.576 \text{ TB} \times 1.30 = 94.35 \text{ TB} \approx 95 \text{ TB}$$

### 30-Day Cold Tiered Storage (S3 / GCS)

$$\text{Single Replica Cold Storage} = 3.456 \text{ TB/day} \times 30 \text{ days} = 103.68 \text{ TB}$$

*(Note: Tiered storage uses single object copy with cloud provider durability $99.999999999\%$, eliminating $RF=3$ local storage amplification).*

---

## 4. Partition Sizing & Consumer Scaling

### Single Partition Limits
- A single Kafka partition on modern NVMe can safely sustain:
  - **Write throughput:** $\approx 5 \text{ MB/sec}$ or $\approx 2,500 \text{ compressed events/sec}$.
  - **Sequential read throughput:** $\approx 20 \text{ MB/sec}$.

### Required Partition Count

For peak cluster throughput of $50,000$ events/sec on core Tier-0 topics:

$$\text{Minimum Theoretical Partitions} = \frac{50,000 \text{ events/sec}}{2,500 \text{ events/sec/partition}} = 20 \text{ partitions}$$

To accommodate a **$2.5\times$ partition skew / hot-key allowance** and parallel consumer autoscaling:

$$\text{Allocated Partitions for Tier-0 Core Topics} = 20 \times 2.5 = 50 \text{ partitions (rounded to 64 for binary balance)}$$

---

## 5. Transactional Outbox Write Amplification (PostgreSQL)

Every event published via the Transactional Outbox pattern introduces write amplification on the upstream relational database:

1. **Insert:** `INSERT INTO outbox_events ...` (WAL write + table heap write + B-tree index update on `(status, created_at)`).
2. **Relay Read:** `SELECT ... FROM outbox_events WHERE status = 'PENDING' LIMIT 1000 FOR UPDATE SKIP LOCKED`.
3. **Status Update / Purge:** `DELETE FROM outbox_events WHERE id = ANY(...)` or mark `PUBLISHED`.

### Relational Database IOPS Math

At peak $50,000$ events/sec:

$$\text{Outbox Inserts} = 50,000 \text{ writes/sec}$$
$$\text{Outbox Deletes / Purges} = 50,000 \text{ writes/sec}$$
$$\text{Total Outbox Operations on DB} = 100,000 \text{ write ops/sec}$$

**Mitigation & Architectural Invariant:**
- No single monolithic PostgreSQL instance handles 100,000 outbox writes/sec.
- The platform enforces **Distributed Database Sharding**: the outbox tables are distributed across application database shards ($N \ge 20$ microservice databases), reducing per-database outbox write load to:

$$\text{Per-DB Outbox Load} = \frac{50,000}{20} = 2,500 \text{ events/sec}$$

A load of 2,500 outbox writes/sec per PostgreSQL primary node is well within standard enterprise AWS RDS `io2` / Google Cloud SQL SSD capacity ($5,000\text{--}10,000$ IOPS).

---

## 6. Broker Cluster Sizing & Hardware Bill of Materials

To service $50,000$ peak events/sec ($120 \text{ MB/sec}$ ingress, $160 \text{ MB/sec}$ egress, $95 \text{ TB}$ NVMe hot storage) with $N+2$ fault tolerance:

| Node Role | Count | Specification per Node | Total Cluster Provisioning |
|---|---|---|---|
| **Kafka Brokers (KRaft)** | **6** | 16 vCPU, 64 GB RAM, $2 \times 10\text{ Gbps}$ NIC, $20\text{ TB}$ NVMe SSD | 96 vCPU, 384 GB RAM, 120 TB NVMe |
| **KRaft Controllers** | **3** | 4 vCPU, 16 GB RAM, 500 GB SSD (Isolated Quorum) | 12 vCPU, 48 GB RAM, 1.5 TB SSD |
| **Outbox Relay Workers** | **12** | 4 vCPU, 8 GB RAM (Go stateless daemons) | 48 vCPU, 96 GB RAM |
| **Consumer Workers** | **24** | 8 vCPU, 16 GB RAM (Go auto-scaling pool) | 192 vCPU, 384 GB RAM |

### Linux OS Page Cache Validation

Kafka relies on the OS page cache for read zero-copy (`sendfile` syscall):
- Active streaming consumer working set: 30 seconds of hot traffic.
- $30 \text{ seconds} \times 40 \text{ MB/sec} = 1.2 \text{ GB}$ of active cache needed.
- With 6 brokers providing $6 \times 48 \text{ GB (free RAM for cache)} = 288 \text{ GB}$, over $99.5\%$ of consumer reads hit RAM directly without touching NVMe disk reads.


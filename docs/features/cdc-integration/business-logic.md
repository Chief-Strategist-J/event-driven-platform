# CDC Integration — Business Logic Decision Tree

```CDC INTEGRATION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it / verify per version)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── 0.1 Components
│   │   ├── C-PG   Source PostgreSQL primary (+ physical standbys)
│   │   ├── C-PUB  Publication — explicit table list (outbox only, or approved raw tables)
│   │   ├── C-SLT  Logical replication slot — one per connector; named, inventoried
│   │   ├── C-DBZ  Debezium PostgreSQL connector (single task per connector)
│   │   ├── C-KC   Kafka Connect cluster — workers, internal topics (configs, offsets, status), REST control plane
│   │   ├── C-CNV  Converters — key/value serialization, schema registry integration
│   │   ├── C-SIG  Signal table — incremental snapshot and control signals (in publication)
│   │   ├── C-HB   Heartbeat table/action — keeps slot advancing on idle databases
│   │   ├── C-AUD  Outbox audit table — durable per-event record (event_id, aggregate, version) for re-emit
│   │   ├── C-TGT  Target Kafka topics — pre-created with durability config
│   │   ├── C-CTL  CDC control plane — authenticated wrapper over Connect REST (config-as-code, approvals, audit)
│   │   └── C-MON  Monitoring — slot health, time-to-full, connector task state, end-to-end canary
│   │
│   ├── 0.2 CDC modes
│   │   ├── M-OUTBOX    Domain events from outbox table via event router (contract-owned, versioned)
│   │   └── M-RAWTABLE  Row-change streams of internal tables (data movement, search sync, warehouse)
│   │
│   └── 0.3 Ownership
│       ├── Platform: Connect cluster, control plane, connector templates, slot inventory, monitoring
│       ├── DB team: source parameters, roles, publications, failover-slot configuration, upgrades
│       ├── Producer team (M-OUTBOX): outbox schema, audit table, re-emit tooling
│       └── Data owners (M-RAWTABLE): which tables may be captured, PII classification, consumers allowed
│
├── 1. INVARIANTS  (with enforcement)
│   ├── CDC-01 Snapshot ≠ history [C]
│   │   └── Enforced by: recovery runbooks never use snapshots to replace lost events; history comes from C-AUD / event store
│   ├── CDC-02 Every slot is owned, monitored, and accounted for [C]
│   │   └── Enforced by: slot inventory (slot ↔ connector ↔ owner); orphan/inactive slot alerts
│   ├── CDC-03 No implicit snapshots [C]
│   │   └── Enforced by: snapshot mode explicit per connector; outbox = no data; raw bootstrap via signals only
│   ├── CDC-04 Publication scope is explicit [C]
│   │   └── Enforced by: manual publications with named tables; auto-create for all tables forbidden
│   ├── CDC-05 Zero error tolerance in the pipeline [C]
│   │   └── Enforced by: error tolerance none; DB constraints prevent bad outbox rows (outbox tree F1.14)
│   ├── CDC-06 Raw CDC carries complete, correct row images [C]
│   │   └── Enforced by: replica identity policy per table; TOAST handling; exact numeric handling; deploy validation
│   ├── CDC-07 Slot survival across failover is designed, not hoped for [C]
│   │   └── Enforced by: failover slots (v17+) or slot-sync mechanism + reconciliation runbook (≤16)
│   ├── CDC-08 No ghost events [C]
│   │   └── Enforced by: logical decoding gated on physical standby confirmation where supported (outbox tree F7.1)
│   ├── CDC-09 Control plane is authenticated and audited [C]
│   │   └── Enforced by: REST behind auth + network isolation; changes only via C-CTL with approvals
│   ├── CDC-10 Configuration is code [C]
│   │   └── Enforced by: connector configs in version control; drift detection vs running config
│   ├── CDC-11 WAL alerting is time-based [C]
│   │   └── Enforced by: time-to-disk-full projections; slot status; inactive duration
│   └── CDC-12 End-to-end is proven continuously [C]
│       └── Enforced by: canary events through the real path; reconciliation against C-AUD
│
├── 2. USE-CASE SELECTION
│   ├── 2.1 Decision
│   │   ├── Other services/teams consume it as an integration contract? → M-OUTBOX only
│   │   ├── Same team moves its own data to its own derived store (search, cache, warehouse)? → M-RAWTABLE allowed
│   │   ├── Data warehouse / lake ingestion? → M-RAWTABLE with data-owner approval + PII policy
│   │   └── Cross-team consumption of raw tables → forbidden (schema coupling + semantic leakage)
│   │
│   ├── 2.2 Separation
│   │   ├── M-OUTBOX and M-RAWTABLE use separate connectors, slots, publications, and topics
│   │   └── Reason: different snapshot policies, failure handling, and recovery semantics
│   │
│   └── F2.1 Raw table topics adopted as integration contracts — CORRUPT on every schema change [C]
│       ├── Impact: column renames, type changes, table splits break consumers silently
│       └── Handling: 2.1; consumer ACLs on raw topics restricted to the owning team
│
├── 3. SOURCE DATABASE PREREQUISITES
│   ├── 3.1 Parameters (sized, not minimums)
│   │   ├── Logical WAL level (restart required — plan maintenance window)
│   │   ├── Replication slots: connectors × 2 + physical standbys + failover headroom [L]
│   │   ├── WAL senders: slots + standbys + base backups + headroom [L]
│   │   ├── WAL sender timeout and TCP keepalives tuned for network path (detect dead connections)
│   │   ├── Logical decoding memory sized for large transactions (spill avoidance)
│   │   ├── Slot WAL keep-size cap: set only with slot-loss runbook ready (§9.3)
│   │   ├── Idle-in-transaction and statement timeouts for app and human roles (§9.4)
│   │   └── Failover slot settings per version (§10)
│   │
│   ├── 3.2 Roles & permissions
│   │   ├── Dedicated replication role (least privilege); not superuser
│   │   ├── Publication owned by DB admin role; connector cannot alter publications
│   │   ├── SELECT only on published tables + signal/heartbeat tables (write on those two only)
│   │   └── Credentials from secret store; rotation procedure tested (§12.6)
│   │
│   ├── 3.3 Table requirements
│   │   ├── Primary key on every captured table (incremental snapshots require it) [C]
│   │   ├── Replica identity: default for outbox (insert/delete only); FULL or suitable index for raw tables
│   │   │   needing old values on update/delete (§7.1)
│   │   └── Signal and heartbeat tables in the publication
│   │
│   ├── 3.4 Managed services
│   │   ├── Provider-specific logical replication enablement and parameter names [L]
│   │   ├── Provider failover behavior for logical slots verified against current docs [G]
│   │   └── Storage autoscaling does NOT replace slot alerts (cost explosion, then caps)
│   │
│   ├── F3.1 Slot/sender limits too low — connector cannot connect after failover or during maintenance — OUTAGE [C]
│   ├── F3.2 Replication role with superuser — blast radius of credential leak — SECURITY [C]
│   └── F3.3 Table without primary key — incremental snapshot impossible; updates hard to key — CORRUPT [C]
│
├── 4. CONNECTOR CONFIGURATION  (decisions, all pinned in version control)
│   ├── 4.1 Identity
│   │   ├── Connector name stable forever (offsets keyed by name)
│   │   ├── Slot name and publication name explicit (never auto-generated)
│   │   └── Topic prefix unique per source database
│   │
│   ├── 4.2 Snapshot policy
│   │   ├── M-OUTBOX: no data (never snapshot outbox rows) [L: mode name per version]
│   │   └── M-RAWTABLE: no data at start + incremental snapshot via signal for bootstrap (§5)
│   │
│   ├── 4.3 Error & lifecycle
│   │   ├── Error tolerance: none
│   │   ├── Slot never dropped on stop
│   │   ├── Retriable error retries bounded; task failure → alert (not silent)
│   │   └── Heartbeat interval + heartbeat action query (writes to heartbeat table)
│   │
│   ├── 4.4 Converters
│   │   ├── Key: plain string of aggregate id (M-OUTBOX) — partition affinity (ordering tree F5.3)
│   │   ├── Value: explicit schema format via registry (versioning tree); no inferred JSON schemas
│   │   └── Converter settings identical across environments (drift detection)
│   │
│   ├── 4.5 Data type handling (M-RAWTABLE)
│   │   ├── Decimal handling: exact representation (string or precise) — never double for money [C]
│   │   ├── Time precision and timezone handling explicit
│   │   ├── Unchanged TOAST placeholder handling decided (§7.2)
│   │   └── Binary handling mode consistent with consumers
│   │
│   ├── 4.6 Operations captured
│   │   ├── Deletes: tombstone emission decided per topic (compacted vs not) [C: tombstones on delete by default]
│   │   ├── Truncate: captured or explicitly skipped — documented, consumers informed [L: default may skip]
│   │   └── Transaction metadata: enabled when consumers need transaction boundaries (§7.5)
│   │
│   ├── 4.7 Topics
│   │   ├── Pre-created with RF 3, min ISR 2, no unclean election, correct partitions, delete policy (event topics)
│   │   └── Connect topic auto-creation disabled or constrained to the same settings
│   │
│   ├── F4.1 Snapshot mode left at default for outbox — mass republish on first start/rename — duplicates/storm [C]
│   ├── F4.2 Connector renamed — offsets lost — snapshot or stream from slot position unexpectedly [C]
│   │   └── Handling: rename = migration runbook (§11.3)
│   ├── F4.3 Decimal handled as double — money precision loss — CORRUPT financial [C]
│   ├── F4.4 Config drift between running connector and repo — unknown behavior — CORRUPT [C]
│   │   └── Detection: periodic diff of running config vs repo → alert
│   └── F4.5 Topic auto-created with broker defaults — durability gaps — LOSS [C]
│
├── 5. SNAPSHOT SEMANTICS
│   ├── 5.1 Initial (blocking at start)
│   │   ├── Consistent snapshot via long repeatable-read transaction [C]
│   │   ├── Holds xmin horizon for duration → bloat on busy tables [C]
│   │   ├── Slot retains WAL from snapshot start until streaming catches up [C]
│   │   └── Use only for small tables or maintenance windows
│   │
│   ├── 5.2 No data
│   │   └── Streams changes from slot position; correct for outbox; requires separate bootstrap for raw tables
│   │
│   ├── 5.3 Incremental (signal-triggered, chunked, concurrent with streaming)
│   │   ├── Requires primary key; reads in key-ordered chunks [C]
│   │   ├── Watermarking deduplicates chunk rows against concurrent streamed changes [C]
│   │   ├── Resumable across restarts (state in offsets) [L]
│   │   ├── Rate-limited by chunk size and pacing; scheduled off-peak for large tables
│   │   └── Emits snapshot-read records — consumers treat as upserts, not as business events
│   │
│   ├── 5.4 Ordering of snapshot reads vs streamed changes
│   │   ├── Consumers must not let an older snapshot read overwrite a newer streamed change
│   │   └── Version guard: source position (LSN/commit order) or row version column (§8)
│   │
│   ├── F5.1 Snapshot used to "recover" lost events — history and deletes lost permanently — LOSS [C]
│   │   └── Handling: CDC-01; §13 recovery tree
│   ├── F5.2 Long initial snapshot on large table — bloat + WAL retention + disk risk — OUTAGE [C]
│   │   └── Handling: incremental snapshot instead
│   ├── F5.3 Incremental snapshot on outbox — emits stale/current rows as events — CORRUPT [C]
│   │   └── Handling: outbox excluded from snapshot signals (policy check in C-CTL)
│   ├── F5.4 Signal table not in publication / not writable — signals ignored silently [C]
│   │   └── Detection: snapshot progress metrics remain idle after signal
│   └── F5.5 Snapshot reads treated as creates by consumers — duplicate business effects [C]
│       └── Handling: consumers branch on operation type; snapshot reads never trigger external effects
│
├── 6. OUTBOX ROUTING  (M-OUTBOX)
│   ├── 6.1 Router decisions
│   │   ├── Route by aggregate type only; topic = aggregate stream (ordering tree OR-03)
│   │   ├── Key = aggregate id; event id → header; event type → header; schema id carried by serializer
│   │   ├── Payload bytes passed through unchanged (versioning tree §7.1)
│   │   └── Deletes of outbox rows ignored (insert+delete pattern)
│   │
│   ├── 6.2 Interaction with snapshots
│   │   └── No snapshot of outbox ever (F5.3); router behavior on snapshot reads must not be relied upon [L]
│   │
│   ├── F6.1 Routing by event type — ordering broken (outbox tree §4) — CORRUPT [C]
│   ├── F6.2 Router misconfiguration emits nothing (wrong field names) — LOSS with healthy connector [C]
│   │   └── Detection: canary event (§15) never arrives; publish rate = 0 while outbox insert rate > 0
│   └── F6.3 Outbox row updated (not insert/delete) — unexpected update events — CORRUPT [L]
│       └── Handling: outbox is insert-only (+delete); DB trigger or constraint prevents updates
│
├── 7. RAW TABLE CDC CORRECTNESS  (M-RAWTABLE)
│   ├── 7.1 Before images
│   │   ├── Default replica identity: old values contain only key columns [C]
│   │   ├── Consumers needing old values (diffs, audit) → FULL replica identity on that table (WAL volume cost)
│   │   └── Deletes: at least key available; FULL needed for full deleted-row content
│   │
│   ├── 7.2 Unchanged TOAST columns
│   │   ├── Updates that don't modify a large column emit a placeholder instead of the value [C]
│   │   ├── Consumers must treat placeholder as "unchanged" (merge with existing), never write it as data
│   │   └── Alternative: FULL replica identity (value available) — cost tradeoff
│   │
│   ├── 7.3 Data types
│   │   ├── Exact numerics (F4.3), timestamps with explicit timezone semantics, enums, arrays, json types
│   │   └── Type changes in source = breaking for consumers (DDL coordination §7.6)
│   │
│   ├── 7.4 Deletes & tombstones
│   │   ├── Delete event + tombstone (for compaction) [C]
│   │   ├── Compacted target topics lose history by design; non-compacted keep delete events
│   │   └── Soft deletes (flag column) appear as updates — consumers must interpret consistently
│   │
│   ├── 7.5 Transaction boundaries
│   │   ├── Without transaction metadata, a multi-row commit arrives as unrelated records [C]
│   │   ├── With metadata: boundary events + per-record transaction id/order → consumers can apply atomically
│   │   └── Cross-table transactions span topics → consumers needing atomicity must buffer by transaction (bounded)
│   │
│   ├── 7.6 Schema evolution (DDL)
│   │   ├── pgoutput sends relation metadata with changes; connector adapts structure [L]
│   │   ├── Column add/drop/rename/type change → downstream schema change through registry compatibility rules
│   │   ├── DDL runbook: expand/contract on source (versioning tree §8) for captured tables
│   │   └── No separate schema-history topic needed for PostgreSQL connector [C]
│   │
│   ├── 7.7 Truncate
│   │   └── Captured → consumers must clear target; skipped → targets diverge silently (document + forbid truncate on captured tables)
│   │
│   ├── F7.1 Placeholder written into target as real value — CORRUPT [C]
│   ├── F7.2 Deletes ignored by consumer — ghost rows downstream — CORRUPT [C]
│   ├── F7.3 Partial transaction applied downstream (consumer reads mid-transaction) — inconsistent views [C]
│   │   └── Handling: 7.5 when consistency matters; otherwise documented eventual consistency
│   ├── F7.4 Column rename on source breaks consumers — CORRUPT/OUTAGE [C]
│   │   └── Handling: DDL review gate for captured tables; registry compatibility check before migration
│   ├── F7.5 Truncate skipped — target retains all rows — CORRUPT [C]
│   └── F7.6 PII columns captured to broadly readable topics — COMPLIANCE [C]
│       └── Handling: column exclusion lists; field encryption (multi-tenant tree §9); topic ACLs
│
├── 8. ORDERING & VERSION SEMANTICS OF CDC EVENTS
│   ├── 8.1 Per-row order preserved within a partition when keyed by primary key [C]
│   ├── 8.2 Version for consumer guards
│   │   ├── M-OUTBOX: aggregate version from producer (ordering tree OR-01)
│   │   └── M-RAWTABLE: row version column (preferred) or source commit position (LSN) from event metadata
│   ├── 8.3 Restart duplicates: connector replays from last flushed position → duplicates in order [C]
│   ├── F8.1 Primary key change (update of key column) — delete + create under different keys — reorder risk [C]
│   │   └── Handling: forbid PK updates on captured tables; consumers handle delete/create pair
│   └── F8.2 Snapshot read older than streamed update overwrites it — CORRUPT [C]
│       └── Handling: 5.4 version guard
│
├── 9. WAL & SLOT MANAGEMENT
│   ├── 9.1 Metrics per slot
│   │   ├── Retained WAL: current WAL position − restart position (disk impact)
│   │   ├── Consumer progress: current WAL position − confirmed flush position (lag)
│   │   ├── Slot status (reserved / extended / unreserved / lost), active flag, inactive duration
│   │   ├── WAL generation rate (bytes/s) and disk free
│   │   └── Catalog horizon held by slot (catalog bloat)
│   │
│   ├── 9.2 Alerts (CDC-11) [L]
│   │   ├── Time-to-disk-full < 6 h → warn; < 2 h → page
│   │   ├── Slot inactive > 5 min → warn; > 15 min → page
│   │   ├── Status unreserved → page; lost → incident (LOSS)
│   │   └── Orphan slot (no connector in inventory) → page
│   │
│   ├── 9.3 Keep-size cap tradeoff
│   │   ├── With cap: disk protected; slot invalidated → LOSS event → §13.1
│   │   ├── Without cap: no loss; disk exhaustion → primary write OUTAGE
│   │   └── Decision recorded per database (T0 often chooses cap + aggressive alerting + audit-based recovery)
│   │
│   ├── 9.4 Long transactions
│   │   ├── Slot restart position cannot advance past oldest running transaction [C]
│   │   └── Timeouts on roles; alert on oldest transaction age (outbox tree F2.3)
│   │
│   ├── 9.5 Idle source database
│   │   └── Heartbeat writes keep slot advancing when captured tables are quiet [C]
│   │
│   ├── F9.1 Fixed-size alert threshold — fires too late or constantly — OUTAGE risk [C]
│   ├── F9.2 Orphan slot from deleted connector — WAL retained indefinitely — OUTAGE [C]
│   │   └── Handling: decommission runbook drops slot; inventory reconciliation
│   ├── F9.3 Multiple slots on one DB — each decodes full WAL — CPU/IO cost multiplied — DEGRADE [C]
│   │   └── Handling: minimize connectors per database; capacity test
│   └── F9.4 Large transaction spill to disk — decoding slowdown, disk usage on source — DEGRADE [C]
│       └── Handling: logical decoding memory sizing; chunked bulk jobs
│
├── 10. HIGH AVAILABILITY & FAILOVER
│   ├── 10.1 PostgreSQL 17+
│   │   ├── Logical slots created as failover-enabled; standby slot synchronization enabled [C]
│   │   ├── Logical decoding waits for designated physical standbys (no ghost events) [C]
│   │   └── After failover: connector reconnects to new primary; verify slot present at expected position
│   │
│   ├── 10.2 PostgreSQL ≤ 16
│   │   ├── Logical slots not carried over on failover [C]
│   │   ├── Options: slot-sync extension or HA manager slot handling [L]; otherwise slot recreated → gap
│   │   └── Every failover triggers §13.1 reconciliation
│   │
│   ├── 10.3 Connector endpoint
│   │   ├── Connects to writer endpoint directly (no pooler) [L]
│   │   ├── Startup verifies connected node is primary
│   │   └── Reconnect with backoff; alert if not streaming within SLO after failover
│   │
│   ├── F10.1 Failover on ≤16 without reconciliation — silent gap — LOSS [C]
│   ├── F10.2 Async standby promoted — events already published for lost transactions — ghost events — CORRUPT [C]
│   │   └── Handling: 10.1 gating; ≤16: synchronous replication for T0 sources; reconciliation
│   └── F10.3 Connector connects to old primary (split brain / stale DNS) — divergent streams — CORRUPT [L]
│       └── Handling: fencing at HA layer; primary check on connect
│
├── 11. UPGRADES & MIGRATIONS
│   ├── 11.1 PostgreSQL major upgrade
│   │   └── Drain to zero lag → record positions + last audit ids → upgrade → recreate slot/publication
│   │       (unless slot migration supported for your path) → start with no data → reconcile (outbox tree L4)
│   │
│   ├── 11.2 Debezium / Connect upgrade
│   │   ├── Pin versions; read release notes for default changes (snapshot mode names, converters, headers) [L]
│   │   ├── Staging run with contract tests on output (key bytes, headers, payload format)
│   │   └── Rolling worker upgrade; connector paused during incompatible changes
│   │
│   ├── 11.3 Connector rename / move to another Connect cluster
│   │   ├── Stop old → record slot confirmed position → create new connector reusing SAME slot and offsets
│   │   │   (or migrate offsets explicitly) → start → verify no gap/no snapshot
│   │   └── Never two connectors on one slot simultaneously
│   │
│   ├── 11.4 Moving from poller relay to CDC (outbox tree L3)
│   │   └── Drain poller → cutover point → CDC starts after point → reconcile
│   │
│   └── F11.1 Upgrade changes header/key format — consumers break or partition affinity changes — CORRUPT [L]
│       └── Detection: contract tests; partition-affinity canary
│
├── 12. CONNECT CLUSTER OPERATIONS
│   ├── 12.1 Workers: ≥ 3, spread across zones; connector task placement monitored
│   ├── 12.2 Internal topics: compacted, RF 3, min ISR 2, pre-created; unique group per cluster
│   ├── 12.3 Task state monitoring: alert on FAILED task (connector may report RUNNING) [C]
│   ├── 12.4 Automated restart with backoff; page after N failures
│   ├── 12.5 Rebalances: cooperative behavior; worker restarts pause connector tasks briefly
│   ├── 12.6 Secrets: externalized config providers; rotation with overlapping validity
│   ├── 12.7 Control plane (CDC-09)
│   │   ├── REST API not exposed beyond control-plane network; authentication enforced
│   │   ├── All changes through C-CTL (config-as-code, approvals for T0, audit log)
│   │   └── Dangerous settings blocked by policy (slot drop on stop, error tolerance all, snapshot on outbox)
│   │
│   ├── F12.1 Unauthenticated REST — connector deleted/reconfigured by anyone with network access — SECURITY/LOSS [C]
│   ├── F12.2 Failed task unnoticed — slot inactive — WAL growth — OUTAGE [C]
│   ├── F12.3 Internal offsets topic misconfigured — offsets lost — unexpected snapshot/duplicates [L]
│   └── F12.4 Credential rotation breaks connector — streaming stops — OUTAGE [C]
│       └── Handling: rotation runbook with validation; slot inactive alert as backstop
│
├── 13. RECOVERY DECISION TREE
│   ├── 13.1 Slot lost / invalidated / missing after failover
│   │   ├── Step 1 Declare incident; record last confirmed position and invalidation/failover time
│   │   ├── Step 2 Drop the invalid slot (after recording); create new slot; start connector with no data
│   │   │          (new stream begins at new slot creation point)
│   │   ├── Step 3 Bound the gap: events committed between last confirmed position and new slot start
│   │   ├── Step 4 Recover by mode:
│   │   │   ├── M-OUTBOX: re-emit from C-AUD (event ids in gap window, original bytes) via outbox insert of
│   │   │   │            same event ids → idempotent consumers absorb overlap
│   │   │   └── M-RAWTABLE: incremental snapshot of affected tables (restores current state) PLUS
│   │   │                  delete reconciliation (target keys absent at source → delete in target)
│   │   │                  — intermediate changes are unrecoverable; documented as such
│   │   ├── Step 5 Reconcile (§16) before closing
│   │   └── Step 6 Root cause (consumer outage, cap too small, failover without sync)
│   │
│   ├── 13.2 Connector offsets lost (slot intact)
│   │   ├── Start with no data → connector streams from slot's confirmed position [L]
│   │   └── Duplicates possible around boundary; no gap if slot intact
│   │
│   ├── 13.3 Duplicate streams (two connectors / restored old connector)
│   │   └── Stop one immediately; consumers absorb duplicates; inventory corrected
│   │
│   ├── 13.4 Poison change (connector cannot convert a row)
│   │   ├── Task fails (error tolerance none) → slot inactive → clock starts (time-to-full)
│   │   ├── M-OUTBOX: fix row via controlled DB correction (audit-logged) or skip via signal only with C-AUD re-emit plan
│   │   └── M-RAWTABLE: fix converter/config; never tolerate errors
│   │
│   └── F13.1 Snapshot-only recovery for outbox — empty or stale — LOSS [C]
│       └── Handling: CDC-01; C-AUD mandatory for M-OUTBOX
│
├── 14. SECURITY & COMPLIANCE
│   ├── Captured columns minimized; PII columns excluded or encrypted
│   ├── Raw topics ACL-restricted to owning team
│   ├── Replication credentials least privilege; network path encrypted
│   ├── Control-plane audit trail retained per policy
│   └── F14.1 Erased data reappears via raw CDC history or snapshots — COMPLIANCE [C]
│       └── Handling: erasure inventory includes CDC topics and targets (multi-tenant tree §10.4)
│
├── 15. OBSERVABILITY
│   ├── Slot metrics (§9.1) per slot; time-to-full projection
│   ├── Connector: task state, restart count, streaming lag (source commit time → Kafka write time),
│   │   events per second, snapshot progress, heartbeat age
│   ├── End-to-end canary: synthetic outbox event every 30 s (outbox tree §9.1); raw-table canary row updates
│   ├── Config drift alerts; control-plane change log
│   ├── Outbox insert rate vs publish rate (M-OUTBOX)
│   └── Absence alerts on all of the above
│
├── 16. RECONCILIATION
│   ├── RC-01 M-OUTBOX: C-AUD event ids per window vs consumer-recorded event ids (gaps → re-emit)
│   ├── RC-02 M-RAWTABLE: row counts + checksums per key range (source vs target), sampled; full for T0
│   ├── RC-03 Deleted-key reconciliation: target keys missing at source
│   ├── RC-04 Slot inventory vs connectors vs databases (orphans, duplicates)
│   └── RC-05 Post-failover / post-upgrade mandatory run before incident closure
│
├── 17. RUNBOOKS
│   ├── RB-01 Time-to-full alert → identify cause (connector down, Kafka down, long tx, idle DB) → fix → watch drain
│   ├── RB-02 Slot lost → §13.1
│   ├── RB-03 Failed task → inspect error → fix → restart → verify streaming + canary
│   ├── RB-04 Raw table bootstrap → incremental snapshot signal → monitor chunks → reconcile
│   ├── RB-05 Captured-table DDL → expand/contract → registry compatibility → deploy consumers → migrate
│   ├── RB-06 PostgreSQL failover → verify slot (v17+) or execute §13.1 (≤16)
│   ├── RB-07 Connector rename/move → §11.3
│   └── RB-08 Decommission connector → stop → drop slot → drop publication (if unused) → remove topics/ACLs → inventory
│
└── 18. GO-LIVE TEST MATRIX
    ├── Connector first start on outbox with existing rows → zero events emitted (no snapshot)
    ├── Kafka unavailable 1 h → slot retains WAL; time-to-full alert accurate; recovery without loss
    ├── Forced slot invalidation (staging) → §13.1 executed; C-AUD re-emit closes gap; reconciliation clean
    ├── Primary failover (v17+) → slot present, no gap, no ghost events; (≤16) → reconciliation closes gap
    ├── Long idle-in-transaction session → alert before WAL threat; timeout terminates session
    ├── Idle database with busy cluster → heartbeat advances slot
    ├── Raw table: update without touching TOAST column → target value preserved (placeholder handled)
    ├── Raw table: NUMERIC money values → exact in target
    ├── Raw table: multi-row transaction → consumers needing atomicity apply all-or-nothing
    ├── Raw table: delete → target row removed; compacted topics only where intended
    ├── Incremental snapshot during heavy writes → no older snapshot read overwrites newer change
    ├── Router misconfiguration → canary missing → page within SLO
    ├── Unauthenticated REST call → rejected
    ├── Connector rename → same slot reused, no snapshot, no gap
    ├── Debezium upgrade in staging → identical key bytes, headers, payload format
    └── Metrics pipeline down → absence alerts fire
```

# Offset Management — Business Logic Decision Tree

```
OFFSET MANAGEMENT — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE
Failure node format: Scenario → Impact → Handling → Detection
│
├── 0. SEMANTICS PRIMER  (every engineer touching offsets must know these)
│   ├── 0.1 A committed/stored offset = NEXT offset to read, i.e., last processed + 1 [C]
│   ├── 0.2 Offsets are per (cluster, topic incarnation, partition); meaningless anywhere else [C]
│   ├── 0.3 Offsets are not dense: transaction control markers and compaction leave holes [C]
│   ├── 0.4 Consumers read up to the high watermark (read-uncommitted) or the last stable offset
│   │       (read-committed); "log end offset" is not what consumers can reach [C]
│   ├── 0.5 Out-of-range = stored offset below log start (retention deleted) or above log end
│   │       (truncation, recreated topic) → reset policy decides what happens [C]
│   ├── 0.6 Timestamp → offset lookup returns the earliest offset whose timestamp ≥ target;
│   │       with CreateTime, timestamps are not monotonic, so lookups are approximate [L]
│   └── 0.7 Group offsets in Kafka expire after the broker's offsets retention when the group is empty [C]
│
├── 1. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-OF   Offset & ownership store (consumer DB) — AUTHORITATIVE positions + partition epochs
│   ├── C-RT   Consumer runtime — seeks to C-OF on assignment; writes offsets in effect transactions
│   ├── C-KCO  Kafka committed offsets — ADVISORY mirror for tooling, lag exporters, ops visibility
│   ├── C-QS   Quarantine/held store — held keys explain "offset advanced, business not applied"
│   ├── C-LAG  Lag service — computes lag from C-OF + broker log offsets + record timestamps
│   ├── C-OPS  Offset operations tool — the ONLY path to change positions (wraps C-OF, audit, approvals)
│   └── C-MON  Monitoring, alerting, reconciliation
│
├── 2. INVARIANTS  (with enforcement)
│   ├── OF-01 One authoritative position per (group, topic incarnation, partition) [C]
│   │   └── Enforced by: C-OF is authoritative; C-RT ignores Kafka committed offsets on assignment
│   ├── OF-02 Position advances only with durable handling [C]
│   │   └── Enforced by: offset write in the same DB transaction as effect / hold / quarantine
│   ├── OF-03 Next-offset semantics everywhere [C]
│   │   └── Enforced by: shared library type "NextOffset"; tests for off-by-one at restart
│   ├── OF-04 Contiguity with parallel lanes [C]
│   │   └── Enforced by: stored offset = lowest offset not yet durably handled across all lanes
│   ├── OF-05 No implicit resets for critical groups [C]
│   │   └── Enforced by: reset policy "none" for T0/T1; out-of-range raises an incident, never auto-resets
│   ├── OF-06 Topic incarnation is part of the position [C]
│   │   └── Enforced by: stored topic id (and partition count snapshot) checked on every assignment
│   ├── OF-07 No position change outside C-OPS [C]
│   │   └── Enforced by: ACLs on Kafka group admin; consumer DB role grants; audit trail
│   ├── OF-08 No skipping without capture [C]
│   │   └── Enforced by: C-OPS skip = export to quarantine store first, then advance; approvals for T0/T1
│   ├── OF-09 Replays never move the production group [C]
│   │   └── Enforced by: replays/rebuilds use new groups and shadow stores (C-OPS refuses otherwise)
│   ├── OF-10 Lag is measured in time, per partition, against SLO and retention [C]
│   │   └── Enforced by: C-LAG; record-count lag is diagnostic only
│   ├── OF-11 Offsets are cluster-local [C]
│   │   └── Enforced by: DR procedures translate or re-derive positions explicitly (§10)
│   └── OF-12 Business staleness is tracked separately from offset lag [C]
│       └── Enforced by: held-key age metrics alongside lag (C-QS)
│
├── 3. STORAGE MODEL  (C-OF, described in words)
│   ├── 3.1 consumer_position
│   │   ├── Key: (group, topic, partition)
│   │   ├── Fields: next_offset, topic_id (incarnation), owner_epoch, last_record_timestamp,
│   │   │          last_updated_at, handler build version, cluster id
│   │   └── Constraint: next_offset never decreases except via C-OPS operation (with operation id)
│   │
│   ├── 3.2 position_operation_log (append-only)
│   │   └── Fields: operation id, type (rewind / skip / migrate / bootstrap / translate), scope, before/after
│   │              positions, actor, approver, reason, linked incident, exported-record reference
│   │
│   └── 3.3 group_topology_snapshot
│       └── Fields: topic id, partition count, first-seen offsets for each partition (detect added partitions)
│
├── 4. COMMIT MECHANICS
│   ├── 4.1 Per record (or same-key micro-batch) transaction
│   │   ├── Effect / hold / quarantine + update next_offset if contiguous → commit
│   │   └── Non-contiguous completion (lane finished later offset first) → recorded as completed;
│   │       next_offset advances when the gap below closes
│   │
│   ├── 4.2 Advisory Kafka commits
│   │   ├── Periodic async commit of C-OF positions [L: every 5 s]
│   │   ├── Best-effort sync commit on graceful revoke/close
│   │   ├── Never on lost partitions (consumer tree CG-04)
│   │   └── Commit metadata carries handler build version + epoch (diagnostics)
│   │
│   ├── 4.3 Auto-commit policy
│   │   ├── Banned: with worker lanes, auto-commit can mark records whose processing hasn't completed [C]
│   │   └── Note: with strictly synchronous in-poll-loop processing, auto-commit is at-least-once, not lossy [C]
│   │             (still banned here because the architecture uses lanes)
│   │
│   ├── F4.1 Off-by-one: storing last processed instead of next — reprocess last record on every restart [C]
│   │   ├── Impact: duplicate effect per partition per restart (dangerous for non-idempotent paths)
│   │   └── Detection: duplicate-absorbed spike exactly equal to partition count after each restart
│   │
│   ├── F4.2 Off-by-one the other way (last + 2, or advancing on fetch) — skip one record per restart — LOSS [C]
│   │   └── Detection: reconciliation gaps aligned to restart times
│   │
│   ├── F4.3 Offset advanced by lane completion order — LOSS on crash [C]
│   │   └── Handling: OF-04 contiguity tracking
│   │
│   ├── F4.4 Offset written in a separate transaction from effect — duplicates or LOSS [C]
│   │   └── Handling: OF-02
│   │
│   ├── F4.5 Advisory commit for partition no longer owned — commit errors / misleading tooling [C]
│   │   └── Handling: commit only owned partitions with current generation; ignore generation errors
│   │
│   ├── F4.6 Commit storm (many partitions × frequent sync commits) — coordinator load [L]
│   │   └── Handling: async batched advisory commits; no per-record Kafka commits
│   │
│   └── F4.7 Offset row hot spot (per record update on one row per partition) — DB contention [L]
│       ├── Handling: one writer per partition already (epoch); small transactions; fillfactor tuning
│       └── Detection: lock waits on consumer_position
│
├── 5. START POSITION, RESET & OUT-OF-RANGE
│   ├── 5.1 Assignment decision (per partition)
│   │   ├── Stored position exists, topic id matches, offset within [log start, log end] → seek
│   │   ├── Stored position exists, topic id DIFFERENT → topic recreated → INCIDENT (§5.4)
│   │   ├── Stored offset < log start → data deleted before processing → INCIDENT (LOSS) (§5.3)
│   │   ├── Stored offset > log end → truncation / wrong cluster / wrong topic → INCIDENT (§5.3)
│   │   ├── No stored position, partition is new (count increased) → start at partition's first offset (§5.5)
│   │   └── No stored position, group is new → explicit bootstrap policy (§5.2)
│   │
│   ├── 5.2 New group bootstrap policies (declared per group)
│   │   ├── FROM_RECORDED_POSITION: positions supplied by C-OPS (e.g., cutover from old group)
│   │   ├── SNAPSHOT_BOOTSTRAP: load producer snapshots, then start at recorded offsets (ordering tree §7.4)
│   │   ├── FULL_RETAINED_HISTORY: earliest retained — only for idempotent, side-effect-free rebuilds
│   │   └── LIVE_ONLY: latest — only for T2 telemetry where missing history is acceptable by design
│   │
│   ├── 5.3 Out-of-range handling (T0/T1 never auto-reset)
│   │   ├── Below log start: consumer was behind longer than retention → LOSS incident;
│   │   │   recovery via producer re-emit / resync for affected window
│   │   ├── Above log end: unclean truncation or wrong target → stop partition; investigate; never jump to latest
│   │   └── T2: may auto-reset per declared policy; event recorded in operation log
│   │
│   ├── 5.4 Topic recreated (same name, new incarnation)
│   │   ├── Impact: offsets restart; stored positions meaningless
│   │   ├── Handling: stop group; determine what the new topic contains (re-published from source? empty?);
│   │   │             reset positions via C-OPS with explicit decision; reconcile
│   │   └── Prevention: topic deletion disabled for business topics; IaC protection
│   │
│   ├── 5.5 Partitions added
│   │   ├── Impact (ordered streams): keys remap — ordering tree forbids; detection should catch it
│   │   ├── Impact (unordered streams): new partitions must start at their first offset, never latest
│   │   └── Detection: partition count change vs group_topology_snapshot → alert
│   │
│   ├── F5.1 Reset policy "latest" on a critical group — silent LOSS [C]
│   │   └── Handling: OF-05; config validation at deploy
│   │
│   ├── F5.2 Reset policy "earliest" on a critical group — silent mass replay + side effects [C]
│   │   └── Handling: OF-05; replays only through C-OPS with side-effect policy
│   │
│   ├── F5.3 Group offsets expired in Kafka while C-OF intact — harmless with OF-01 [C]
│   │   └── Without OF-01: reset policy fires after idle periods (scale-to-zero, long incident)
│   │
│   └── F5.4 Consumer pointed at wrong cluster/environment with valid-looking offsets — CORRUPT [C]
│       └── Handling: cluster id stored in position; mismatch → refuse to start
│
├── 6. OFFSET-SPACE EDGE CASES
│   ├── F6.1 Transactional producers: control markers occupy offsets [C]
│   │   ├── Impact: lag in records never reaches 0; "offset + 1 exists" assumptions break
│   │   └── Handling: next_offset taken from consumer position after processing, not computed as record + 1
│   │                 when using batch-level positions; lag uses LSO for read-committed consumers
│   │
│   ├── F6.2 Compacted topics: offsets have holes [C]
│   │   └── Handling: never assume density; seeks to a removed offset land on next available
│   │
│   ├── F6.3 Aborted transactions visible to read-uncommitted consumers — ghost events [C]
│   │   └── Handling: read-committed isolation where producers use transactions
│   │
│   ├── F6.4 Long-open producer transaction holds LSO — read-committed consumers stall [C]
│   │   ├── Impact: lag grows although consumers are healthy
│   │   └── Detection: LSO vs high watermark gap per partition
│   │
│   ├── F6.5 Timestamp-based rewind imprecise (non-monotonic CreateTime) — wrong replay window [L]
│   │   └── Handling: rewind by recorded offsets (C-OF history / operation log) or LogAppendTime topics;
│   │                 add safety margin and rely on idempotency for overlap
│   │
│   └── F6.6 Unclean leader election truncates log below stored offsets — positions beyond log end [C]
│       └── Handling: §5.3; durability config prevents (outbox tree §5)
│
├── 7. LAG MONITORING
│   ├── 7.1 Definitions
│   │   ├── Record lag (diagnostic) = consumable end (HW or LSO) − next_offset (from C-OF)
│   │   ├── Time lag (primary) = now − timestamp of the record at next_offset
│   │   │                        (if caught up: 0; if unknown: estimate from produce rate)
│   │   ├── Business staleness = oldest held-key age (C-QS) per handler
│   │   └── Retention headroom = retention − time lag (time until LOSS horizon)
│   │
│   ├── 7.2 Evaluation per partition (sliding window, not single sample)
│   │   ├── OK: time lag ≤ SLO
│   │   ├── LAGGING: time lag > SLO but next_offset advancing
│   │   ├── STALLED: next_offset not advancing while records are available
│   │   ├── STOPPED: no owner / no recent position updates
│   │   ├── REWINDING: next_offset decreased (only legitimate with a C-OPS operation id)
│   │   └── AT-RISK: retention headroom < threshold
│   │
│   ├── 7.3 Thresholds (per handler tier, derived from SLO) [L]
│   │   ├── T0: time-lag SLO 5–30 s; STALLED 2 min → page
│   │   ├── T1: SLO 1–5 min; STALLED 5 min → page
│   │   ├── T2: SLO 15–60 min; STALLED 30 min → ticket
│   │   └── AT-RISK: headroom < 50% of retention → page (all tiers)
│   │
│   ├── F7.1 Lag read from Kafka committed offsets while C-OF is authoritative — wrong numbers [C]
│   │   └── Handling: C-LAG reads C-OF; advisory commits only for tooling compatibility
│   │
│   ├── F7.2 Group-level aggregation hides one stuck partition — missed P1 [C]
│   │   └── Handling: per-partition evaluation; alert on max, not average
│   │
│   ├── F7.3 Record-count thresholds across topics with different rates — noise or blindness [C]
│   │   └── Handling: OF-10
│   │
│   ├── F7.4 Offsets current but keys held — "healthy" dashboard, stale business state [C]
│   │   └── Handling: OF-12; business staleness panel next to lag
│   │
│   ├── F7.5 Unexplained REWINDING — someone moved offsets — CORRUPT risk [C]
│   │   └── Handling: page; check operation log; unknown rewinds = security incident
│   │
│   └── F7.6 Idle topic (no new records) misread as stalled — false pages [C]
│       └── Handling: STALLED requires records available beyond next_offset
│
├── 8. ALERTING & AUTOSCALING INTERPLAY
│   ├── 8.1 Autoscale on time lag sustained over window; never on single spikes (consumer tree §11)
│   ├── 8.2 Do not scale on STALLED (poison/held keys/dependency) — scaling won't help and adds rebalances
│   ├── 8.3 Scale-out ceiling = partitions; at ceiling and still LAGGING → capacity incident (lanes, handler cost)
│   └── F8.1 Autoscaler reacts to lag caused by a paused breaker — rebalance storm on top of outage [C]
│       └── Handling: autoscaler ignores partitions paused for dependency reasons
│
├── 9. ADMINISTRATIVE OPERATIONS  (only via C-OPS; every operation logged, reviewed, reversible where possible)
│   ├── 9.1 Common pre-conditions
│   │   ├── Target group stopped (zero members) — verified, not assumed
│   │   ├── Operation scoped: group, topic id, partitions, exact positions (not "to-latest")
│   │   ├── Dry run shows before/after per partition, record counts, time range, affected keys estimate
│   │   ├── Side-effect policy declared (EFFECTS_ON / STATE_ONLY)
│   │   ├── Window checks: dedupe window, provider idempotency window (DLQ tree DL-09)
│   │   └── Approvals: T0/T1 require second approver
│   │
│   ├── 9.2 REPLAY / REBUILD (never on production group — OF-09)
│   │   ├── Create new group + shadow store
│   │   ├── Bootstrap policy: snapshot + recorded offsets, or full retained history if complete
│   │   ├── Side effects disabled (STATE_ONLY)
│   │   ├── Parity check vs live; cutover per DLQ/versioning trees
│   │   └── Delete shadow group positions after retirement
│   │
│   ├── 9.3 REWIND of production group (rare; e.g., handler bug corrupted state for a known window)
│   │   ├── Preferred alternative: resync affected aggregates (ordering tree §8.4)
│   │   ├── If used: stop group → set C-OF positions to recorded earlier offsets → mirror to Kafka → start
│   │   ├── Idempotency layers must absorb overlap; external effects suppressed or keyed (window check)
│   │   └── Post: reconciliation for the window
│   │
│   ├── 9.4 SKIP (emergency; replaces "shift-by +N")
│   │   ├── Step 1 Export every record in the skip range to the quarantine store as QUARANTINED
│   │   │          (raw bytes, headers, offsets), with keys HELD (ordering preserved for affected aggregates)
│   │   ├── Step 2 Verify export count equals range size (holes accounted for)
│   │   ├── Step 3 Advance C-OF position; mirror to Kafka
│   │   ├── Step 4 Restart group; held keys resolved via DLQ tree workflow (healthy records redriven)
│   │   └── Never: blind skip without export; skip across partitions in one command
│   │
│   ├── 9.5 MIGRATE positions (group rename, protocol migration, handler split)
│   │   └── Copy C-OF rows to new group id inside one transaction; verify; start new; retire old
│   │
│   ├── F9.1 Reset on running group — partially applied, members overwrite with their positions [C]
│   │   ├── Note: Kafka admin tooling refuses resets for active groups; C-OF writes must enforce the same
│   │   └── Handling: C-OPS verifies zero members AND zero partition epochs held
│   │
│   ├── F9.2 Reset applied to Kafka only (C-OF unchanged) — silently ignored [C]
│   │   └── Handling: C-OPS is the only tool; direct Kafka admin access restricted
│   │
│   ├── F9.3 Skip used to "unblock" poison — multi-key LOSS [C]
│   │   └── Handling: 9.4 only
│   │
│   ├── F9.4 Replay with side effects re-fires notifications/payments — CORRUPT financial [C]
│   │   └── Handling: STATE_ONLY default; EFFECTS_ON requires business approval and window checks
│   │
│   └── F9.5 Forensic audit planned "later from disk segments" — evidence deleted by retention [C]
│       └── Handling: capture-before-act (9.4 Step 1)
│
├── 10. DISASTER RECOVERY & MULTI-CLUSTER
│   ├── 10.1 Consumer DB restore/failover
│   │   ├── Positions rewind together with state (co-located) → consistent replay [C]
│   │   └── External effects need idempotency keys (replayed records re-attempt them)
│   │
│   ├── 10.2 Kafka cluster failover (mirrored)
│   │   ├── Offsets differ between clusters [C]
│   │   ├── Options: mirror-provided offset translation (checkpoint-based, approximate) [L]
│   │   │            or timestamp-based start with safety margin + idempotent absorption
│   │   ├── Positions in C-OF re-derived for the target cluster and recorded as TRANSLATE operation
│   │   └── Gaps/duplicates expected at boundary → reconciliation mandatory before declaring recovery
│   │
│   ├── 10.3 Kafka cluster rebuilt from scratch (data re-published) — offsets meaningless [C]
│   │   └── Handling: treat as topic recreation (§5.4) for every topic
│   │
│   ├── F10.1 Consumer started against DR cluster with primary-cluster offsets — out of range or wrong data [C]
│   │   └── Handling: cluster id check (F5.4)
│   │
│   └── F10.2 Fail back to primary — positions now behind/ahead of primary log [C]
│       └── Handling: same TRANSLATE procedure in reverse; reconcile
│
├── 11. OTHER TOPICS' OFFSETS  (retry tiers, DLQ notification topic, internal topics)
│   ├── Retry topic groups (if used) store positions in C-OF like main groups
│   ├── DLQ notification topic consumers are tooling — LIVE_ONLY acceptable (source of truth is C-QS)
│   └── Connect/relay internal offsets are producer-side concerns (outbox tree), never managed by C-OPS
│
├── 12. SECURITY & ACCESS
│   ├── Kafka group admin (describe/delete/reset) restricted to C-OPS service identity
│   ├── Consumer DB: only C-RT role writes positions during normal operation; C-OPS role for operations
│   ├── Every operation: identity, approval, reason, ticket — immutable log
│   └── F12.1 Engineer runs reset from laptop in incident — unaudited CORRUPT [C]
│       └── Handling: break-glass procedure through C-OPS with post-incident review
│
├── 13. OBSERVABILITY
│   ├── Per partition: time lag, record lag, state (OK/LAGGING/STALLED/STOPPED/REWINDING/AT-RISK)
│   ├── Retention headroom per partition
│   ├── Business staleness (held-key age) per handler
│   ├── LSO vs HW gap (transaction stalls)
│   ├── Out-of-range events, topic id mismatches, cluster id mismatches (all page)
│   ├── Partition count changes on subscribed topics
│   ├── Operation log events (every C-OPS action → notification)
│   ├── Advisory commit failures (diagnostic)
│   └── Absence alerts on all of the above
│
├── 14. RECONCILIATION
│   ├── RC-01 C-OF vs Kafka committed (advisory) — drift beyond commit interval → investigate
│   ├── RC-02 C-OF positions vs producer versions (ordering tree RC-01) — lag explained
│   ├── RC-03 Operation log vs observed REWINDING events — every rewind has an operation id
│   └── RC-04 Skip operations: exported record count = skipped range (holes accounted)
│
├── 15. RUNBOOKS
│   ├── RB-01 STALLED partition → check held caps, paused reasons, lane stalls, poison → DLQ path (never skip first)
│   ├── RB-02 AT-RISK retention headroom → extend retention → add capacity → escalate dependency
│   ├── RB-03 Out-of-range (below start) → LOSS incident → identify window → producer re-emit/resync → reconcile
│   ├── RB-04 Out-of-range (above end) / topic id mismatch → stop group → determine topic history → decide positions
│   ├── RB-05 Rebuild projection → new group + shadow store → parity → cutover
│   ├── RB-06 Emergency skip → export-to-quarantine → advance → redrive healthy records
│   ├── RB-07 Cluster failover → translate positions → reconcile boundary
│   └── RB-08 Unexplained rewind → freeze group → audit operation log → security review
│
└── 16. GO-LIVE TEST MATRIX
    ├── Restart under load → zero reprocessed and zero skipped records (off-by-one test)
    ├── Lanes complete out of order then crash → no loss (contiguity)
    ├── Consumer down longer than retention (test topic with short retention) → out-of-range incident, no auto-reset
    ├── Topic deleted and recreated (staging) → refusal to start + incident
    ├── Partition added to unordered topic → new partition consumed from its first offset
    ├── Transactional producer topic → lag reaches SLO "caught up" via LSO; no false stalls
    ├── Long-open producer transaction → LSO stall detected and attributed correctly
    ├── Kafka-only offset reset attempted → blocked/ignored with alert
    ├── Emergency skip → every skipped record present in quarantine, keys held, redrive restores
    ├── Replay via new group with STATE_ONLY → zero external effects; parity achieved
    ├── Consumer DB restore → positions and state consistent; idempotent external effects
    ├── DR failover with translated positions → boundary duplicates absorbed, zero gaps after reconciliation
    ├── One partition stalled while others healthy → per-partition page fires
    ├── Keys held while offsets current → business staleness alert fires
    └── Metrics pipeline down → absence alerts fire
```

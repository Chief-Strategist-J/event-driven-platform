# Offset Management: Failure Cases and Edge-Case Mitigations

This file was empty before the 2026-11-07 amendment; it currently lists only the failure modes handled by that fix. See `adr.md` and `business-logic.md` for the broader design.

## Amendment 2026-11-07: Failure Modes Handled

| ID | Failure Mode | Trigger | Detection | Response / Status | Operator Action |
|---|---|---|---|---|---|
| **F-OFF-A1** | Rewind used as forward jump | `POST /api/v1/offsets/rewind` with `targetOffset` > stored offset | `ErrRewindForwardJump` | `400` | Use `POST /api/v1/offsets/fast-forward` for forward moves; those are audited and bounded by log end. |
| **F-OFF-A2** | Rewind beyond log end | `targetOffset` > `logEndOffset` | `ErrRewindBeyondLogEnd` | `400` | Check the topic's actual log end; pick a valid earlier offset. |
| **F-OFF-A3** | Rewind of unknown partition | No stored offset for tenant/group/topic/partition | Repository not-found | `404` | Verify identifiers; commit an offset first if the partition is new. |
| **F-OFF-A4** | Invalid commit input | Blank/whitespace ids, `partition < 0`, `offset < 0`, `offset > logEndOffset`, non-UUID `correlationId` | Service validation | `400` with field and issue (no `pq:` text) | Fix the request. |
| **F-OFF-A5** | Offset regression on commit | Commit below stored offset without admin rewind | `ErrOffsetRegression` (pre-existing rule) | Rejected | Use the rewind endpoint (stop the consumer group first per policy). |
| **F-OFF-A6** | Non-positive fast-forward | `shiftBy <= 0` | `ErrInvalidFastForward` | Rejected | Send a positive `shiftBy`. |
| **F-OFF-A7** | Lag query missing identifiers | `GET /api/v1/offsets/lag` without tenant/group/topic or bad `partition` | Handler validation | `400` | Provide `tenant_id` (or `X-Tenant-Id`), `group_id`, `topic`. |

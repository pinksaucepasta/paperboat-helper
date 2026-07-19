# Paperboat Helper Recovery Runbooks

These runbooks use product-role vocabulary in user-facing diagnostics. Provider, transport,
and service-manager details belong only in operator evidence. Never attach tokens, claims,
terminal/config content, signed URLs, private paths, or raw state databases to a ticket.

## Version or capability skew

- Detect: `protocol_incompatible`, required capability unavailable, or repeated negotiation
  rejection grouped by helper version.
- Contain: stop automatic connection retries after the bounded retry policy; preserve the
  current compatible artifact and durable store.
- Recover: compare the signed compatibility matrix, install a compatible verified artifact,
  and use rollback only when its store range includes the current schema.
- Verify: negotiation succeeds, required capabilities are ready, and no mutation occurred
  during rejected handshakes.
- Escalate: release and helper owners when no signed artifact supports the current store.

## Signing-key rotation or mass revocation

- Detect: unknown-key refresh spikes, stale revocation snapshots, or revoked environment,
  helper, session, or connector credentials still attempting work.
- Contain: fail closed, stop new admission, retain only the bounded last-known snapshot for
  diagnosis, and never relax issuer/audience/scope/binding checks.
- Recover: restore control-plane reachability, fetch one authenticated bounded key/revocation
  update, rotate helper identity material atomically, and reconnect with a new generation.
- Verify: old keys/credentials fail, overlap keys pass only during the approved window, and
  logs/diagnostics contain no claims or bearer material.
- Escalate: security and control-plane owners for snapshot authenticity or propagation gaps.

## Corrupt or incompatible durable state

- Detect: typed `store is corrupt`, integrity-check failure, migration refusal, or a schema
  version newer than the helper supports.
- Contain: stop session admission and writes. Preserve the database, WAL, and SHM files with
  permissions intact; do not delete or rewrite unexplained durable data.
- Recover: use a verified compatible helper for newer schemas. For corruption, copy the
  state set offline, run approved SQLite recovery tooling, and restore only transactionally
  consistent records. Never claim a persisted running process survived helper restart.
- Verify: integrity check passes, sequence intervals do not overlap, event lengths match,
  and recovered running/restarting generations are represented as lost/exited.
- Escalate: storage and helper owners before discarding any session, input, or operation row.

## Replay-gap or slow-consumer spike

- Detect: increased `replay_gap` or `slow_consumer`, retained-history pressure, or attachment
  queues reaching their configured byte bound.
- Contain: evict only the affected attachment; keep the PTY/session and other attachments
  running. Do not enlarge queues beyond validated capacity during an incident.
- Recover: reconnect at `earliest_sequence`, communicate the explicit lost interval, and
  investigate output rate, disk pressure, and client acknowledgement latency.
- Verify: earliest sequence is an event boundary, replay intervals are exact, and no terminal
  input was resent because of the output gap.
- Escalate: CLI/helper owners when healthy clients cannot drain within configured limits.

## Connector rejection, replacement, or outage

- Detect: stale generation, replayed admission, QUIC and TCP/TLS failure, retry cap, or drain
  escalation.
- Contain: reject stale/replayed credentials, keep the last active generation until a newer
  connection is established, and stop admission first during shutdown.
- Recover: obtain a new single-use admission for the current helper generation, prefer QUIC,
  use TCP/TLS fallback, atomically replace, then drain the old connection.
- Verify: exactly one generation is active, stale routes are detached, previews distinguish
  target state from route/public-edge state, and no admission credential is reusable.
- Escalate: edge/control-plane owners after both transports exhaust bounded retry.

## Disk pressure or upload cleanup backlog

- Detect: storage readiness degraded, SQLite/full-sync failures, upload `resource_limit`,
  cleanup backlog, or partial/staging diagnostics.
- Contain: reject new bounded work before reading bodies, preserve unclassifiable files, and
  do not compact/delete outside generated scoped paths and verified metadata.
- Recover: free approved space, run bounded cleanup, reconcile upload/update/compaction
  journals, and retry idempotent operations by operation ID.
- Verify: no partial upload remains, published files are private and scoped, SQLite commits
  succeed, and retained history still has exact bounds.
- Escalate: operations/storage owners when cleanup cannot classify an artifact safely.

## Signed-update failure or rollback

- Detect: signature/digest/compatibility rejection, interrupted journal state, failed staged
  or post-activation health check, or startup health failure for a committed artifact.
- Contain: never execute an unverified artifact; preserve current and previous verified
  binaries plus the synced journal.
- Recover: follow `staged`, `backing_up`, `activating`, `checking`, or `committed` recovery.
  Restore previous only after proving it is a regular verified artifact. Stop on rollback
  failure rather than repeatedly deleting current.
- Verify: active digest/version matches the signed manifest, compatibility includes the
  current protocol/store, health passes, and previous remains available.
- Escalate: release/security owners for root rotation, revoked release, or rollback failure.

## Stuck or timed-out shutdown

- Detect: shutdown duration exceeds the configured bound, process-group termination
  escalates, connector drain fails, or owned resource counts remain nonzero.
- Contain: admission and new session/input/upload work stay stopped. Preserve durable state
  and record only safe component/result codes.
- Recover: cancel in-flight dialing/probes, terminate helper-owned process groups according
  to policy, close PTY/files/listeners, flush durable metadata, and let the service manager
  restart only after the prior process exits.
- Verify: zero owned processes, PTYs, descriptors, listeners, timers, connectors, and pending
  goroutines; a fresh runtime starts and stops repeatedly under the race suite.
- Escalate: helper and operations owners with bounded diagnostics and correlation IDs.

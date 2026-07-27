# AGENTS.md - paperboat-helper

Inherit [`../AGENTS.md`](../AGENTS.md). Helper, remote runtime, and environment runtime
mean this repo. `pbh` is the user-facing helper command.

## Ownership

Remote Go service owning the outbound frpc-compatible connector, scoped auth, PTYs,
durable sessions, replay history, Herdr processes, image staging, preview targets,
health, config application, and signed updates. Hosted-only modules additionally
own workspace/volume preparation, presets, setup, boot, pre-stop flush, and shutdown.

## Stack

Go `1.25.7`; standard library first; proven PTY, service, crypto, filesystem-watch, and
frp libraries where they materially reduce correctness risk.

## Local Rules

- Separate protocol, auth, terminal, history, process, upload, preview, config,
  connector, service, update, and hosted lifecycle ownership.
- Version and negotiate capabilities. Preserve sequence replay, explicit gaps, bounded
  history, compaction, multi-attach, exit status, and slow-consumer policy.
- Distinguish disconnect, close, clear, restart, and delete.
- Validate cwd, roots, environment, paths, symlinks, MIME, size, scope, identity, and
  expiry at trust boundaries.
- Every goroutine, PTY, process, stream, file, watcher, timer, and connector has one
  owner, bounded resources, cancellation, and cleanup.
- BYOD never receives hosted lifecycle behavior or config sync without assignment plus
  current warning consent.

## Verify

Run `make check` and relevant race, real PTY/process/filesystem, crash/restart, contract,
and hosted/BYOD tests. Help must never advertise unimplemented runtime behavior.

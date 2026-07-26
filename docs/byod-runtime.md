# User-Machine Runtime Operations

## Service Boundary

Linux installs `/etc/systemd/system/paperboat-helper.service` and
`paperboat-host-service.service`. macOS installs matching LaunchDaemons under
`/Library/LaunchDaemons`. The worker starts at system boot as the enrolled UID/GID and
does not depend on login, a GUI domain, or systemd linger.

`paperboat-host-service` runs as root. Its `0600` Unix socket authenticates the enrolled
UID with kernel peer credentials. The versioned protocol accepts only availability,
paired signed-update, and bounded diagnostic operations. It accepts no shell command,
caller path, credential, or environment override. Identity, SQLite, histories, uploads,
workspaces, PTYs, and connectors remain owned by the enrolled user.

## Bootstrap And Migration

`pbh bootstrap` verifies both signed artifacts before requesting one administrator
approval. The privileged installer independently verifies signature, digest, platform,
architecture, canonical paths, file ownership, modes, UID/GID, and fixed service inputs.
It stages through a root-owned journal, starts the host service, stops an active canonical
legacy user service, starts the system worker, and waits for all of:

- the exact signed worker version on loopback health;
- a worker generation newer than the pre-install generation;
- active system service scope;
- the control-plane installation heartbeat.

Only then does it commit and remove the legacy definition. Failure or interruption removes
partial system services, restores the legacy service when it had been active, restores the
previous verified binaries, revokes newly issued identity material, releases the seat, and
retains pre-existing durable user state.

## Availability

New user machines start with `keep_awake`. The dashboard and
`pb user-machine availability <machine> --mode allow-sleep|keep-awake` update authoritative
desired state. Keep-awake applies on battery and AC, can increase battery use and heat, and
may keep a closed-lid machine awake where the platform permits it.

Linux holds logind sleep, idle, and lid-switch inhibitors. Releasing their file descriptors
restores normal behavior. macOS records every original `pmset` power-source value before
applying `disablesleep`; disabling or uninstalling restores those exact values. Desired
local policy is persisted before application so a host-service restart or network outage
cannot silently enable sleep.

## Offline Recovery

Local state and health start without control-plane or DNS availability. Authentication,
JWKS refresh, policy resolution, activity delivery, previews, configuration, and connector
admission retry independently with bounded jitter. Interface and DNS changes wake connector
retry, whose maximum delay remains below 60 seconds. Missing or stale validated JWKS means
authorization unavailable, never permissive access.

Credential renewal uses the persisted Ed25519 identity and an exact request proof. It does
not require an unexpired bearer credential, so multi-day outages and server signing-key
rotation do not require re-enrollment. Revocation, replacement generation, environment
state, proof freshness, replay protection, idempotency, and key thumbprint remain mandatory.

After an OS reboot, the worker increments its generation and records the OS boot ID.
Durable terminal identity, cwd, dimensions, bounded history, and input decisions remain;
running or restarting process generations become exited with reason `machine_reboot`.
Agents are never relaunched automatically.

## Signed Updates

The worker forwards only paired signed worker/host manifests to the root host service.
The host service verifies both against its bootstrap-pinned public key, downloads over
HTTPS into the fixed root install filesystem, verifies size and digest, and uses a synced
journal. It activates both fixed binaries, restarts the worker, requires exact-version
health, preserves the prior verified pair, and restarts itself after replying. A failed or
interrupted activation restores both rollback binaries and restarts the old worker.
Replaying an already committed identical signed version is idempotent.

## Diagnostics And Alerts

Run `pbh doctor --json` locally. It reports system boot scope, OS boot ID, worker
generation, local health, connector recovery, and availability desired/observed state.
The worker's numeric loopback listener exposes `GET /metrics` for the trusted host-local
collector; it must never be routed publicly.
Inspect `journalctl -u paperboat-helper.service -u paperboat-host-service.service` on Linux
or the corresponding LaunchDaemon logs on macOS. Never include credentials, proof bodies,
terminal content, workspace paths, or private signing material in incident records.

Alert on boot failures, restart loops, credential-renewal failures, reconnect latency over
60 seconds, availability drift, privileged-service errors, update rollback, and stale
heartbeats. Correlate using bounded operation/request IDs and helper version.

## Uninstall

`pbh service uninstall` requests administrator approval and uses root-persisted metadata
bound to the invoking UID. It first applies `allow_sleep` and restores the captured macOS
power baseline, then disables/removes both system services and deletes root-owned current,
staged, rollback, previous, journal, metadata, socket, and policy files. The enrolled user's
identity, terminal history, uploads, and workspace data are retained. Purging durable user
state is a separate explicit operation.

After uninstall, verify there are no Paperboat system units/LaunchDaemons, no logind
inhibitors, original `pmset` values are restored, and the root install/state paths contain
no Paperboat service artifacts.

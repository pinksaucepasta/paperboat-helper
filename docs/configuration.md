# Paperboat Helper Runtime Configuration

Runtime configuration is local/static and validated before startup. Product entitlement,
route truth, catalog values, and desired lifecycle state remain control-plane data.

Current environment inputs:

- `PAPERBOAT_HELPER_PROFILE`: `byod` by default or `hosted`.
- `PAPERBOAT_HELPER_STATE_ROOT`: absolute private state directory; defaults under the user
  configuration directory.

Protocol maxima are 64 KiB structured frames, 256 KiB terminal frames, 1 MiB pending output
per attachment, 15-second heartbeat, 45-second peer timeout, and five-minute mutation
deadline. Local/credential configuration may lower but never raise frozen maxima. Default
upload size is 20 MiB and retention is 24 hours, both lowerable by credentials.

Default resource ceilings are 64 sessions, 16 attachments per session, 10,000 retained
input decisions per session, 4 MiB output history per session, two concurrent uploads, 128
preview targets, eight concurrent target probes, 32 concurrent protocol operations, and
1,000 queued activity events. Static configuration may tune these values within reviewed
hard bounds. Startup rejects zero, partial, or excessive values; saturation returns a typed
resource limit before side effects or unbounded body reads.

The state root owns SQLite session/history/input/operation records, update journals, and
scoped generated artifacts. Direct database edits, cross-version copying without the
compatibility gate, symlinked roots, and group/world-writable executables are unsupported.
Backups must capture SQLite database/WAL/SHM consistently while helper writes are stopped.

## Hosted Lifecycle Inputs

The hosted profile consumes `PAPERBOAT_PROJECT_ID`, `PAPERBOAT_REPOSITORY_URL`, and
`PAPERBOAT_DEFAULT_BRANCH`. `PAPERBOAT_WORKSPACE` defaults to `/workspace`; the checkout
is a validated child directory derived from the repository name or
`PAPERBOAT_PROJECT_DIR`. `PAPERBOAT_REPOSITORY_HOSTS` defaults to `github.com` and accepts
only comma-separated HTTPS repository hosts.

`PAPERBOAT_PRESET_CODES` resolves regular, non-symlink, non-writable catalog scripts from
`PAPERBOAT_PRESET_DIR` (default `/etc/paperboat/presets.d`). Setup content is read only
through the environment name in `PAPERBOAT_SETUP_SCRIPT_ENV`; the content and command
output are never persisted in lifecycle configuration. `PAPERBOAT_HOSTED_MAX_SCRIPT_BYTES`,
`PAPERBOAT_HOSTED_MAX_OUTPUT_BYTES`, `PAPERBOAT_HOSTED_OPERATION_TIMEOUT_SECONDS`, and
`PAPERBOAT_CONFIG_SHUTDOWN_DEADLINE_SECONDS` bound execution and shutdown flushes.

The state root also owns `helper-identity.json`, containing the helper's Ed25519 seed. Its
directory and file modes are `0700` and `0600`. Symlinks, hard links, non-regular files,
duplicate JSON keys, and mismatched JWK thumbprints are refused. Rotation uses an expected
key ID and atomic file replacement; private seed bytes never enter diagnostics or logs.

## Control-Plane Enrollment

`paperboat-helper enroll <absolute-config-path>` exchanges a short-lived, single-use
server grant for a helper identity bound to the persisted Ed25519 public key. The private
configuration file must be a regular, non-symlink file with no group or world permissions
and contains `control_url`, absolute `state_root`, and `enrollment_credential`. An optional
absolute `control_ca_file` adds a bounded PEM trust anchor for private deployments without
disabling certificate or hostname verification. The control URL must use HTTPS. Redirects,
oversized responses, unknown or duplicate JSON
fields, and trailing data fail closed.

Successful enrollment atomically writes `runtime-identity.json` with mode `0600`. Runtime
credential reads require an unexpired identity whose `key_id` still matches
`helper-identity.json`; key rotation therefore requires a new enrollment. Neither the
grant nor the resulting bearer credential is printed by the command.

`paperboat-helper run` is the production hosted daemon. On first boot it may consume the
one-time `PAPERBOAT_ENROLLMENT_CREDENTIAL`; subsequent boots load the volume-backed runtime
identity. It requires an HTTPS `PAPERBOAT_CONTROL_URL`, refreshes the control-plane JWKS,
verifies operation and connector credentials, waits for an admitted frp route before
reporting ready, and removes the enrollment credential from the process environment after
exchange. `PAPERBOAT_CONTROL_CA_FILE` may add a private TLS 1.3 trust anchor.

BYOD never advertises hosted lifecycle. Config application additionally requires an active
assignment and proof of the current warning consent. Optional capability failure degrades
only that readiness entry; liveness reports only whether the process can answer.

Normal diagnostics contain version/profile, safe capability states, bounded queue counts,
and correlation IDs. They intentionally exclude tokens/claims, terminal/config bytes,
signed URLs, filenames beyond scoped display forms, and private local paths.

## Runtime HTTP Boundaries

The helper HTTP service binds only an explicit numeric loopback address. Public TLS and
route ownership remain at the edge. Helper WebSockets require the
`paperboat.helper.v1` subprotocol, a single bearer credential, bounded messages, and normal
same-origin validation. Structured protocol bytes use WebSocket text messages; terminal
output uses binary messages. Application frames may still span or share WebSocket messages.

Image staging is a separate authenticated `POST` handler. It reserves concurrency and the
operation journal before reading multipart bytes, accepts exactly one `file` part, and uses
these required request headers:

- `X-Paperboat-Request-ID`
- `X-Paperboat-Operation-ID`
- `X-Paperboat-Deadline-Ms`
- `X-Paperboat-File-Name`
- `X-Paperboat-File-Mime`
- `X-Paperboat-File-Size`
- `X-Paperboat-File-Sha256`

Multipart filename and MIME must exactly match the headers, and the streamed byte count,
detected MIME, and SHA-256 must all match before publication. An idempotent replay returns
the recorded result with `X-Paperboat-Replayed: true` without reading the request body.

## Phase 2 Fake-Peer Harness

`paperboat-helper phase2-harness <absolute-config-path>` is an evidence-only composition
command. It is not production enrollment or endpoint discovery. The JSON file is bounded
to 64 KiB and must be a single-link, regular, non-symlink file that is not group/world
writable. Unknown, duplicate, or trailing JSON fields fail closed.

Required fields are `profile`, absolute `state_root` and `workspace_root`, numeric-loopback
`listen_address`, static `shell_path`, `shell_args`, `shell_environment`, HTTPS `issuer`,
`environment_id`, `helper_id`, and one or more Ed25519 `public_keys` encoded with unpadded
base64url. Optional `origin_patterns`, `revoked_jtis`, and `config_apply_proof` remain local
fake-peer inputs. No private key or reusable bearer credential belongs in this file.

The harness verifies operation credentials against exact frozen policies and starts the
real durable store, PTY/session manager, protocol server, upload handler, preview registry
and monitor, activity collector, health endpoint, and bounded shutdown path. It remains
separate from the production `enroll` command and does not consume its runtime identity.

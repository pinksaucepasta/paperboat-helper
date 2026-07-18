# Non-Terminal Helper Operations 1.0

## Image staging

`upload.v1` accepts authenticated streaming multipart data with exactly one file. The
default maximum is 20 MiB and the credential may lower it. Allowed MIME types are
`image/png`, `image/jpeg`, `image/gif`, and `image/webp`; detected content must match the
declared type. Names are reduced to a safe basename. Absolute paths, traversal, NUL, device
files, hard links, and symlinks are rejected. The helper writes to a private temporary file,
fsyncs, atomically publishes a helper-generated scoped path, and returns its SHA-256. A
repeated operation ID is idempotent. Partial files are removed on failure or cancellation.
Staged images expire after 24 hours by default and never later than the credential expiry.
Concurrent uploads are limited by helper configuration and excess work returns
`resource_limit` without reading an unbounded body.

## Preview identity and readiness

The control plane owns `preview_base_domain`. A preview key is lowercase ASCII matching
`p-[a-z2-7]{26}`: `p-` plus the first 130 bits of
`HMAC-SHA256(preview_identity_key, environment_id || 0x00 || logical_name)`, base32 without
padding. The public hostname is `{preview_key}.{preview_base_domain}`. The server detects
the cryptographically improbable collision and derives again with a persisted positive
counter. Keys are retained for 30 days after expiry or removal and cannot be reassigned to
another environment during retention. Deleting the environment permanently tombstones
its keys for the same period.

Changing a target port or reconnecting preserves the key. Preview states are
`registering`, `ready`, `degraded`, `offline`, `expired`, and `removed`. A route alone is
not readiness: `ready` requires helper, route, public edge, and target probes. Public HTTP
returns `503` with `Retry-After` for registering/degraded, `502` for an unhealthy target,
`503` for offline, `410` for expired, and `404` for removed or unknown. Public WSS closes
with `1013` for retryable readiness failures. All responses carry
`X-Robots-Tag: noindex, nofollow, noarchive`. First creation requires an explicit public
access acknowledgement; no field may imply privacy.

## Activity

Trusted activity events carry environment ID, helper/process/session identity, source,
wall-clock timestamp, per-source monotonic sequence, and observed freshness. Accepted
sources are terminal input, agent user interaction, explicit CLI activity, and a signed
agent activity signal. Open processes, PTYs, sockets, routes, previews, output-only work,
and health checks are not activity. Events are deduplicated by source identity and sequence;
older sequences are rejected. Batches contain at most 100 events or 64 KiB. Events older
than five minutes are recorded for diagnostics but cannot extend idle policy.

## Config application

`config.apply.v1` is advertised for hosted profiles and for BYOD only when the server has
issued an active assignment plus proof of acceptance of the current warning revision.
Assignments use immutable revision IDs. Pull, apply, and report carry operation IDs and the
expected assignment revision. Revision mismatch returns `config_revision_conflict` without
writing. Apply stages all validated files, rejects absolute/traversal/symlink paths and
limit violations, then atomically switches the assignment view. Conflicts preserve both
sides and stop automated writes. Revocation cancels queued work and prevents subsequent
reads; it does not claim already applied bytes were erased.

## Health and diagnostics

Liveness reports only that the process can answer. Readiness is per capability and reports
`ready`, `degraded`, or `unavailable` with stable safe reason codes. Dependencies are named
by product role (`control_plane`, `edge`, `storage`, `target`), not provider. Diagnostics
may include versions, capability states, bounded queue sizes, and correlation IDs, but not
tokens, claims, terminal/config content, local source paths, or signed URLs.

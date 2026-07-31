# Non-Terminal Helper Operations 1.0

## File transfer

`/v1/file-transfers` accepts opaque regular files between identified machines with resumable
HEAD/PATCH content, SHA-256 verification, idempotent operation IDs, and atomic batch
publication. Defaults are 50 MiB per file, ten files and 500 MiB per batch, and two active
streams. MIME is not inspected or restricted. `pb send <path> --to <machine>` obtains an
exact `file:transfer` credential bound to the owned source and destination machine IDs.
Files remain for seven days. Pending deliveries to an attached interactive machine remain
for ten minutes within a 1 GiB spool and are pinned
to one active writer. Cancellation, rejection, checksum failure, expiry, and typed storage
failures clean partial content.

The same machine-addressed manifest is returned for every transfer:

```json
{
  "transfer_id": "ft_...",
  "batch_id": "fb_...",
  "source_machine_id": "mch_source",
  "destination_machine_id": "mch_destination",
  "initiating_user_id": "usr_...",
  "basename": "archive.bin",
  "size": 42,
  "sha256": "lowercase-hex-sha256",
  "committed_offset": 42,
  "state": "published",
  "result_code": "published",
  "created_at": "2026-07-29T12:00:00Z",
  "expires_at": "2026-08-05T12:00:00Z"
}
```

`session_id` is optional context. Recipient client IDs and machine filesystem paths are
internal and never appear in a manifest. A published completion result may separately
return its opaque Paperboat Inbox path.

| Method | Resource | Contract |
| --- | --- | --- |
| `POST` | `/v1/file-transfers` | Create one idempotent batch and one manifest per declared file. |
| `GET` | `/v1/file-transfers/{id}` | Inspect current manifest state and result. |
| `HEAD` | `/v1/file-transfers/{id}/content` | Return committed offset, length, digest, and strong ETag. |
| `PATCH` | `/v1/file-transfers/{id}/content` | Append `application/offset+octet-stream` at exact `Upload-Offset`. |
| `POST` | `/v1/file-transfers/{id}/complete` | Verify every file and atomically publish or offer the entire batch. |
| `GET` | `/v1/file-transfers/pending?session_id=...&wait_seconds=...` | Long-poll offers pinned to the authenticated CLI client. |
| `GET` | `/v1/file-transfers/{id}/content` | Download with strong `If-Match`, byte `Range`, and resume support. |
| `POST` | `/v1/file-transfers/{id}/receipt` | Record identical idempotent durable-storage success or a typed failure. |
| `DELETE` | `/v1/file-transfers/{id}` | Cancel the complete batch and remove incomplete or pending content. |

HTTP/3 and HTTP/2 use identical transfer IDs and machine-runtime state. An application HTTP status
never selects another transport. An uncertain upload is followed by `HEAD`; an interrupted
download is resumed only after hashing the private partial file. Credential refresh retries
once with the same operation and transfer IDs. Credential expiry does not alter resource
retention or state.

Stable file-transfer errors are `invalid_path`, `invalid_size`, `batch_limit`,
`offset_conflict`, `digest_mismatch`, `no_active_writer`, `recipient_unavailable`,
`storage_unavailable`, `resource_limit`, `canceled`, and `delivery_timeout`.

## Preview identity and readiness

Preview registration, listing, and removal are agent operations handled by the local
the `pb` host runtime. Every operation is bound to the machine's assigned environment;
cross-environment IDs, names, state, and URLs are rejected and never disclosed. A
successful registration response returns the public URL to the calling agent, which
surfaces it in the existing terminal session. `pb` and the dashboard may list the user's
active previews/tunnels account-wide with associated project, machine, and user context,
and may revoke an existing preview, but cannot create one.

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

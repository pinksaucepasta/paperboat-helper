# Contract Trust-Boundary Threat Model 1.0

## Boundaries

| Boundary | Authority entering | Required verification | Forbidden disclosure |
| --- | --- | --- | --- |
| CLI to server | CLI client session | issuer, user, client session, exact scope, expiry, revocation | browser cookie, refresh token, provider response |
| CLI to helper through edge | operation credential | user, environment, session/operation scope, audience, protocol | connector address/token, other environment existence |
| Helper to server | helper identity or scoped report credential | helper key, environment, generation, scope, sequence | terminal/config content, local paths |
| Helper to tunnel | single-use connector admission | environment, helper, generation, edge pool, replay | server/provider credentials |
| Server to tunnel | edge-control credential | node, route revision, connector generation, operation ID | user/runtime reusable credentials |
| Tunnel to server | usage credential | node, counter epoch, route ownership, monotonic counter | request/response content or headers |
| Public preview to target | public route only | exact normalized host, current route revision, readiness | control routes, helper APIs, private metadata |
| Config source to helper | config assignment credential | assignment/revision, environment, warning consent, limits | repository credential or unrelated paths |
| Update source to helper | update credential plus artifact signature | environment, channel, digest, signature, rollback policy | signing private key |

## Required Negative Cases

- Downgrade: no mutual version or missing required capability fails before mutation.
- Confused deputy: credentials for terminal, upload, preview, activity, config, connector,
  edge control, usage, or update are rejected at every other audience and operation.
- Cross-environment: every resource and credential binding is checked together; errors do
  not distinguish another owner's resource from an unknown resource.
- Replay: single-use token IDs are consumed atomically; mutation operation IDs return the
  recorded result only for byte-equivalent canonical requests.
- Enumeration: unauthorized lookup, delete, route collision, and preview removal expose a
  common `not_found_or_forbidden` result.
- Request smuggling: edge and helper reject conflicting content lengths, invalid transfer
  encoding, illegal header bytes, ambiguous authority/host, and hop-by-hop forwarded headers.
- Oversized input: declared and actual body/frame limits are enforced during streaming,
  with bounded reads and cleanup of partial state.
- Filesystem escape: absolute, traversal, NUL, symlink, hard-link, and device paths fail
  before staging or config application.
- Log injection: externally supplied fields are structured, length-bounded, and escaped;
  terminal bytes, config contents, tokens, claims, signed URLs, and local paths are absent.
- Compromise: environment revocation advances helper/connector generation, revokes all
  descendant credentials and routes, and requires a new helper key before admission.

Security review must verify each case against an applicable negative fixture and repository
test. Any algorithm downgrade, reusable admission credential, cross-environment acceptance,
or secret-bearing descriptor blocks release.

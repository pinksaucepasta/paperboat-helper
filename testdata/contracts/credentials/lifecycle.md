# Credential Lifecycle 1.0

Every signed credential has `iss`, `aud`, `sub`, `jti`, `iat`, `exp`, `scope`, `kid`, and
the bindings declared by `classes.json`. Verifiers require an exact audience and exact
scope set; a broader token is never accepted for a narrower operation. Browser cookies,
CLI sessions, helper credentials, data-plane credentials, and infrastructure credentials
are not interchangeable. Tokens never appear in query strings, descriptors, logs, traces,
metrics, diagnostics, or error details.

Issuance requires current identity, entitlement, environment ownership, and non-revoked
parent authority. Child expiry never exceeds parent expiry. Enrollment, connector
admission, and edge-control credentials are single-use; their `jti` is atomically consumed
with the mutation and retained through expiry plus skew. Other mutating requests still use
an operation ID for replay-safe results.

Signing keys are selected by `kid`. An unknown key triggers one bounded JWKS refresh, then
fails closed. Rotation publishes old and new public keys before the issuer switches signing;
the old key remains only through the maximum token lifetime plus skew. Private keys never
leave their owning service. Algorithms other than EdDSA and Ed25519 keys are rejected.

Revocation is keyed by token ID, parent session, user, environment, helper generation, and
signing key as applicable. Environment compromise revokes enrollment, helper identity,
connector, operation, preview, activity, config, and update descendants, replaces the
helper key, and advances connector generation. Connector replacement invalidates the prior
generation before route attachment. Authorization failures return the same `not_found_or_forbidden`
classification for missing and other-owner resources.

Expired, not-yet-valid, wrong issuer/audience/environment/scope/key, replayed, revoked, and
malformed credentials fail before mutation. Error responses expose a stable code and
recovery action but never echo claims or reveal another user's resource existence.

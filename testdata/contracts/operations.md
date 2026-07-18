# Contract Observability And Operations 1.0

## Correlation And Redaction

Every boundary propagates `request_id`, `operation_id` where applicable, `environment_id`,
selected protocol version, and a stable result code. Session, route, helper, connector,
node, and assignment IDs are included only in operator telemetry with access controls.
Never record terminal input/output, pasted/local paths, staged bytes, config content,
tokens, signatures, credential claims, signed URLs, request authorization/cookie headers,
or raw provider responses.

Metric dimensions are allowlisted: service, operation, result, protocol major/minor,
capability, profile, readiness state/reason, direction, and bounded retry bucket. User,
environment, session, request, operation, route, connector, node, file, hostname, and token
identifiers are prohibited metric labels. Logs retain correlation identifiers for 30 days;
aggregate protocol/security metrics retain 13 months; debug sampling defaults off and may
not bypass redaction. Trace payloads follow the 30-day log policy.

Required counters/histograms cover negotiation, incompatibility, capability rejection,
authentication/authorization rejection, revocation latency, reconnect, replay gaps,
uncertain writes, slow-consumer eviction, upload rejection/cleanup, preview readiness and
route/target failure, activity freshness, config conflict, connector admission/replacement,
route attachment, and monotonic usage reconciliation.

Liveness means the process answers. Readiness is per capability and uses stable reasons:
`control_plane_unavailable`, `edge_unavailable`, `storage_unavailable`, `target_unhealthy`,
`route_pending`, `credential_unavailable`, `draining`, and `resource_limit`. Provider and
infrastructure names appear only in restricted operator diagnostics.

## Runbooks

### Version skew

1. Identify the rejected adjacent pair and offered/selected versions from bounded labels.
2. Stop rollout before any default switch; do not force or downgrade required capabilities.
3. Restore the last approved compatible producer/consumer pair and artifact digest.
4. Confirm incompatibility returns to baseline before resuming expand/observe sequencing.

### Key rotation and mass revocation

1. Publish the new public key, verify all verifiers observe it, then switch the signer.
2. Retain the prior public key through maximum credential lifetime plus skew.
3. For compromise, revoke the key and affected parent identities immediately, advance
   helper/connector generations, detach routes, and require enrollment with a new key.
4. Verify revocation latency and negative vectors; never roll back to compromised acceptance.

### Replay-gap or uncertain-write increase

1. Separate history eviction from sequence/parser faults using requested and earliest
   sequence metrics; do not inspect terminal content.
2. Stop unsafe retries. Preserve uncertain input IDs until queried or expired.
3. Drain affected helper versions if sequence monotonicity or deduplication is violated.
4. Restore the last compatible helper/CLI pair and retain evidence for incident review.

### Connector, route, or usage failure

1. Compare desired connector generation and route revision with edge observations.
2. Reject stale ownership, drain the old connector, and attach only the current generation.
3. Reconcile absolute counters by node/epoch/route; never synthesize deltas from a reset.
4. If ownership is ambiguous, stop new admission and restore the last approved control pair.

### Fixture drift and contract rollback

1. Run the workspace validator and identify the exact canonical/copy/provenance mismatch.
2. Do not edit a released fixture in place. Restore the complete last approved artifact set
   and every consumer copy by immutable commit and digest.
3. Run all four local validators and conformance suites before reopening rollout.
4. Record the cause, affected consumers, restored digest, and follow-up decision.

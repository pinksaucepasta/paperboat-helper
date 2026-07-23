# Terminal Stream Semantics 1.0

Output sequence is a zero-based byte offset within a session's retained logical output
stream. Each output event carries `[start_sequence, end_sequence)`, where the interval
length equals the decoded byte count. Sequence spans stdout and stderr in one total order;
the channel is metadata. Acknowledgements are cumulative and mean all bytes before
`next_sequence` were delivered to that attachment.

Replay requests carry `from_sequence` and an optional byte limit. Attach requests may
set `at_live_boundary` to atomically join at the helper's current sequence. A complete response
states the exact returned interval. If the cursor precedes retained history, the helper
returns `replay_gap` with `requested_sequence`, `earliest_sequence`, and `latest_sequence`;
it does not return partial output. A reconnecting CLI treats a valid boundary as normal
history compaction: it emits a visible `Earlier terminal output is unavailable` marker,
advances its cursor to `earliest_sequence`, and retries once from that boundary. It only
surfaces `replay_gap` when the boundary is missing/invalid or recovery fails. Duplicate
ranges are removed by sequence, not content.

Input uses ordered binary channel `3`, a connection-local monotonically increasing
sequence, attachment ID, and process generation. The helper authorizes and writes frames
to the PTY synchronously in WebSocket order without creating operation or input-idempotency
rows. Socket backpressure is bounded. A disconnect makes only the last unconfirmed frame
uncertain; the client discards it and never replays terminal input.

All authorized attachments receive the same ordered output. Accepted input is ordered by
the helper's monotonic input order. Resize ownership belongs to the most recently active
interactive attachment, with attachment ID breaking equal timestamps lexicographically.
Signals require explicit scope and never follow a detach implicitly.

An attachment queue is bounded to 1 MiB. At the boundary, the helper stops enqueueing,
emits `slow_consumer` when possible, evicts only that attachment, and leaves the session
running. History limits are configuration-driven, but eviction always removes whole output
events and advances `earliest_sequence` to the next event boundary. Clear is serialized
with output and returns the first sequence after the cleared interval. Snapshots state their
generation and exact sequence interval. Delete guarantees subsequent lookup is
indistinguishable from an unknown session to unauthorized callers.

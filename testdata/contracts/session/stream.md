# Terminal Stream Semantics 1.0

Output sequence is a zero-based byte offset within a session's retained logical output
stream. Each output event carries `[start_sequence, end_sequence)`, where the interval
length equals the decoded byte count. Sequence spans stdout and stderr in one total order;
the channel is metadata. Acknowledgements are cumulative and mean all bytes before
`next_sequence` were delivered to that attachment.

Replay requests carry `from_sequence` and an optional byte limit. A complete response
states the exact returned interval. If the cursor precedes retained history, the helper
returns `replay_gap` with `requested_sequence`, `earliest_sequence`, and `latest_sequence`;
it does not return partial output unless the caller explicitly retries at the earliest
sequence. Duplicate ranges are removed by sequence, not content.

Input uses a client-generated `input_id`, attachment ID, and process generation. The helper
records one of `accepted`, `duplicate`, or `rejected`. A disconnect before a recorded result
is `uncertain`; the client queries that input ID and never repeats bytes without a recorded
`rejected` result. Input IDs are retained through the maximum reconnect window.

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

# Helper Application Protocol 1.0

The application protocol runs over authenticated HTTPS and WSS through the Paperboat
edge. Transport establishment does not authorize an operation; every request is checked
against its environment-bound credential and advertised capability.

## Negotiation

The client sends `hello` before any operation. The helper selects the highest mutually
supported minor version within major version 1 and replies with `welcome`. Failure returns
`protocol_incompatible` and closes without creating or changing runtime state. Required
capabilities must be selected exactly; optional unknown capabilities are ignored.

## Limits

- Structured JSON frame: 64 KiB encoded.
- Binary terminal frame: 256 KiB.
- HTTP body: operation-specific and never unbounded.
- Pending outbound data per attachment: 1 MiB.
- Heartbeat interval: 15 seconds; peer timeout: 45 seconds.
- Operation deadline: required for mutations, at most 5 minutes.

WebSocket message boundaries are not application boundaries. Parsers accept fragmented
and coalesced frames. A structured frame is a four-byte unsigned big-endian length followed
by UTF-8 JSON. Binary terminal data is a four-byte length, one-byte channel (`1` stdout,
`2` stderr, `3` client input), eight-byte unsigned big-endian sequence, then bytes.
Length includes the channel and sequence header. Client-input data begins with two
unsigned big-endian 16-bit lengths for session and attachment IDs, an unsigned
big-endian 64-bit process generation, both IDs, then the PTY bytes. Input sequences
start at one per connection and frames remain ordered; input frames receive no
per-frame response and are never replayed.

After all queued terminal bytes have been delivered, an exited or closed terminal emits
a structured `event` frame with `event: terminal_stream_end`, the session ID, final
sequence, state, and exit result when present. Clients use this event for exact remote
exit status; transport closure alone never implies process success.

Every mutation carries `operation_id`. A duplicate operation ID with the same canonical
request returns the recorded result; reuse with different content returns
`operation_id_conflict`. Cancellation is explicit and idempotent. A disconnect neither
cancels nor repeats an operation unless its operation contract says so.

Slow consumers receive `slow_consumer` before close code `4408` when the control frame can
still be delivered. Authentication uses `4401`, authorization `4403`, protocol/version
failure `4406`, malformed or oversized frames `4409`, deadline/cancellation `4410`, and
internal unavailable `4503`. Normal detach uses `1000`.

Errors use `common.error-envelope`. Error details never contain tokens, terminal content,
config contents, staged paths outside their scoped display form, or provider identifiers.

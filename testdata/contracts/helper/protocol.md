# Helper Application Protocol 2.0

The application protocol runs through the Paperboat edge over either WSS or native QUIC.
The bearer credential is authenticated once on the native control stream or WSS connection.
Lifecycle operations are authorized against their advertised capability; terminal data
frames use the connection-local stream binding established by attach.

## Negotiation

The client sends `hello` for exactly version `2.0` before any operation and the helper
replies with `welcome`. Failure returns `protocol_incompatible` and closes without creating
or changing runtime state. Protocol 1.x is not supported. Required capabilities must be
selected exactly; optional unknown capabilities are ignored.

## Limits

- Structured JSON application frame: 64 KiB encoded.
- Binary terminal application frame: 256 KiB.
- Native QUIC records are bounded per frame; streams have no cumulative byte ceiling.
- Other HTTP bodies are operation-specific and bounded.
- Pending outbound data per attachment: 1 MiB.
- Heartbeat interval: 15 seconds; peer timeout: 45 seconds.
- Operation deadline: required for mutations, at most 5 minutes.

WSS maps each text or binary message to exactly one application frame. Native QUIC uses
one connection with independently flow-controlled control, input, and output streams.
Each begins with a bounded `PBT1` preface containing version, role, a 16-byte connection
ID, and bounded binding/token lengths. Control carries the bearer token. Its welcome
returns a random 32-byte binding secret required by input and output.

Control records use:

```text
+----------+----------------+------------------+
| kind u8  | length u32 BE  | payload          |
+----------+----------------+------------------+
```

Kind `1` is a nonempty structured JSON frame. Kind `2` is a control binary frame such as
ACK or resize. Input and output use the same big-endian length without a kind byte because
their roles are fixed. Unknown kinds, zero or oversized lengths, malformed JSON, and
truncated records are protocol errors. Fragmentation does not alter record boundaries.

Native QUIC uses ALPN `paperboat-terminal/1` on UDP 443. WSS continues at `/v1/runtime`
with subprotocol `paperboat.terminal.v2`. Both transports use the same scoped bearer
credential, negotiation, application frames, limits, authorization, session state, and
close semantics. Auxiliary streams are usable only after control authorization succeeds;
duplicates, incorrect roles or bindings, and incomplete sets are rejected. Control loss
closes the attachment, and uncertain input is never replayed.

Structured lifecycle frames are UTF-8 JSON. Attach returns a nonzero connection-local
`uint32 stream_id`.
Terminal input, output, cumulative ACK and resize are fixed-header binary messages defined
by `fixtures/helper/terminal-v2.json`; they carry the stream ID rather than string session
or attachment identifiers. Input and resize sequences start at one for each attached stream
and remain contiguous. Input frames receive no per-frame response and are never replayed.

After all queued terminal bytes have been delivered, an exited or closed terminal emits
a structured `event` frame with `event: terminal_stream_end`, the session ID, final
sequence, state, and exit result when present. Clients use this event for exact remote
exit status; transport closure alone never implies process success.

Lifecycle mutations carry `operation_id`. A duplicate operation ID with the same canonical
request returns the recorded result; reuse with different content returns
`operation_id_conflict`. Terminal input is intentionally outside the operation journal.
Cancellation is explicit and idempotent. A disconnect neither cancels nor repeats an
operation unless its operation contract says so.

Slow consumers receive `slow_consumer` before close code `4408` when the control frame can
still be delivered. Authentication uses `4401`, authorization `4403`, protocol/version
failure `4406`, malformed or oversized frames `4409`, deadline/cancellation `4410`, and
internal unavailable `4503`. Normal detach uses `1000`.

Errors use `common.error-envelope`. Error details never contain tokens, terminal content,
config contents, staged paths outside their scoped display form, or provider identifiers.

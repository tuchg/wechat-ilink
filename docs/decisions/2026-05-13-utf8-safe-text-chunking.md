# UTF-8 Safe Text Chunking

Date: 2026-05-13

## Context

`WechatIlinkClient::send_text` splits long text messages into chunks before
sending them. The previous implementation sliced `&str` with a fixed byte index
of 4000. When that byte index landed inside a multi-byte UTF-8 character, Rust
panicked with `end byte index 4000 is not a char boundary`.

## Decision

Keep the message limit as a byte limit, because the send path was already using
`str::len()` and the transport limit is expressed in bytes. Before slicing, move
the chunk window end backward to the nearest UTF-8 character boundary. Paragraph,
newline, and space split preferences continue to operate within that safe
window.

## Consequences

Long messages containing Chinese or other multi-byte text no longer panic during
send chunking. Chunks remain at or below the existing byte limit when the limit
is large enough to hold at least one UTF-8 scalar value, which is true for the
4000-byte send path.

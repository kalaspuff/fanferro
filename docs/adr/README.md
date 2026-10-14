# Architecture decision records

Short records of decisions that shape fanferro. Add one (next number, `NNNN-title.md`) when a
change alters a layer boundary, a wire format, a default with behavioural impact, or a guarantee.
Status values: Accepted, Superseded by NNNN, Deprecated.

| ADR | Decision |
| --- | --- |
| [0001](0001-rust-core-python-api.md) | Rust core for protocol, timing and I/O; Python for application objects |
| [0002](0002-cloudevents-default-envelope.md) | CloudEvents 1.0 (JSON) as the default envelope (default superseded by 0008) |
| [0003](0003-claim-checks-in-the-transport-layer.md) | Claim checks and base64 live in the transport layer, below envelopes |
| [0004](0004-events-pulled-from-the-core.md) | Core events are pulled into Python, never pushed from Rust threads |
| [0005](0005-in-memory-backend.md) | An in-memory backend implements the same ports as AWS |
| [0006](0006-ministack-for-end-to-end-tests.md) | MiniStack as the end-to-end test target |
| [0007](0007-shutdown-and-orphaned-long-polls.md) | Shutdown waits for the last long poll by default (default superseded by 0008) |
| [0008](0008-lean-defaults.md) | Lean defaults: raw messages, opt-in CloudEvents and MD5 checks, immediate stop |

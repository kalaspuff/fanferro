# 0004: Core events are pulled into Python, never pushed from Rust threads

Status: Accepted

## Context

Users hook into events (logging, alerts, metrics). Hooks are Python callables, often async, that
must run on the application's event loop. Calling Python from Tokio worker threads would contend
for the GIL and run hooks on the wrong thread.

## Decision

* The core emits typed events into a bounded buffer. Emission is filtered per kind with atomic
  flags that Python sets to the kinds logging or hooks want.
* Python drains the buffer with an event pump task (while a consumer runs or the client is
  entered) or synchronously after operations, and dispatches on the event loop.
* Overflow drops events and is reported as `events.dropped`.

## Consequences

* Hooks can be sync or async and may use asyncio freely.
* Events are slightly delayed (milliseconds) relative to the action.
* Adding an event kind touches Rust, Python and docs; CI checks they agree.

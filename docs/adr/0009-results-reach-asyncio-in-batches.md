# 0009: Results of Rust futures reach asyncio in batches, without runtime threads taking the GIL

Status: Accepted

## Context

Every awaitable of `fanferro._core` (publishing, deliveries, provisioning, events) was bridged
with `pyo3-async-runtimes`. For each one it spawned two Tokio tasks, copied the context variables
and registered a done callback; when the Rust future completed, a blocking-pool thread attached to
the interpreter, waiting for the GIL that the event loop holds while it runs Python code, to call
`loop.call_soon_threadsafe` for that one result. A trivial awaitable cost about 75 µs of CPU and
99 µs of latency (Linux, aarch64, 10 cores), 100 concurrent ones about 58 µs of CPU each, and they
grew the blocking pool to more than a hundred threads. It was the largest single cost of
publishing a message.

## Decision

`crates/fanferro-py/src/runtime.rs` bridges futures itself:

* An awaitable is a `RustFuture`, an `asyncio.Future` whose `cancel()` also aborts the Tokio task
  that drives the Rust future (one task per awaitable).
* A finished task queues its outcome on the bridge of its event loop. Outcomes are converted into
  Python objects on the loop's thread, in one callback for every outcome that arrived meanwhile.
* The first outcome of a batch wakes the loop. On Unix, the loop watches one end of a socket pair
  (`loop.add_reader`) and the task writes a byte to the other end: runtime threads never attach
  to the interpreter. Loops that cannot watch file descriptors (the proactor event loop on
  Windows) are woken with one `call_soon_threadsafe` per batch, from a blocking task.
* Settlements resolve their awaitables through the same bridge (without cancellation: a started
  settlement completes either way).

## Consequences

* A trivial awaitable costs about 14 µs of CPU and 16 µs of latency, 100 concurrent ones about
  10 µs of CPU each; the runtime keeps its worker threads only.
* Each event loop that awaits fanferro operations holds a socket pair (two file descriptors) and
  is kept alive until it is closed; bridges of closed loops are released when the next loop
  starts using fanferro.
* Results are delivered in the order the futures completed, as before; a panicking Rust future
  raises `FanferroError` (`rust future panicked: ...`) instead of
  `pyo3_async_runtimes.RustPanic`.
* `pyo3-async-runtimes` is no longer a dependency.

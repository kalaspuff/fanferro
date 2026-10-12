# 0001: Rust core for protocol, timing and I/O; Python for application objects

Status: Accepted

## Context

fanferro must be fast and robust (retries, heartbeats, batching) yet feel native to Python
developers whose messages are pydantic models, dataclasses or protobuf classes and whose
handlers are `async def` functions instrumented with OpenTelemetry.

## Decision

* The Rust crate `fanferro-core` owns everything that does not need Python objects: AWS calls
  (AWS SDK for Rust), retries, envelopes, transport encoding, claim checks, provisioning, and the
  consumer engine (polling, leases, heartbeats, settlement, shutdown). It has no PyO3 dependency
  and can be used from Rust directly.
* `fanferro-py` is a thin binding: conversions, the asyncio bridge, exception mapping.
* Python owns codecs, routing, handler invocation, middleware, hooks, logging and
  OpenTelemetry (so spans join the application's traces through the Python SDK).
* Custom envelopes are Python objects implementing the same protocol as the Rust built-ins; the
  Python consumer drives envelope selection, so both kinds are interchangeable.

## Consequences

* Heartbeats and settlement keep running even if a handler blocks the event loop.
* Every Rust feature needs a Python surface (`_core.pyi`, wrapper, docs). `scripts/check_consistency.py`
  verifies the stub against the native module.
* Python handler code runs on the application's event loop; the core never calls back into
  Python from its threads (see ADR 0004).

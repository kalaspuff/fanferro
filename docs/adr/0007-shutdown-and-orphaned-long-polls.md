# 0007: Shutdown waits for the last long poll by default

Status: Superseded by [0008](0008-lean-defaults.md) (`abort_receives=True` is the default; the protection of released messages below still applies)

## Context

Dropping an in-flight `ReceiveMessage` long poll closes the connection, but SQS (and emulators)
keep serving the poll on the server until its wait time ends. Messages handed to that orphaned
poll become invisible for their whole visibility timeout - including messages the consumer
releases during shutdown, and new messages arriving during a deploy.

## Decision

* `ShutdownPolicy.abort_receives` defaults to `False`: stopping lets the current long poll finish
  (at most `wait_time`, overlapping with draining handlers) and releases whatever it returns.
* With `abort_receives=True` (and on a second SIGINT/SIGTERM) the poll is aborted; on AWS
  backends every message the engine releases or retries afterwards is kept invisible until the
  orphaned poll is over, so it cannot be captured by it.

## Consequences

* Idle consumers take up to `wait_time` (default 20 s) to stop; set a lower `wait_time` or
  `abort_receives=True` where faster shutdown matters more.
* No message is hidden for a full visibility timeout by fanferro's own shutdown.

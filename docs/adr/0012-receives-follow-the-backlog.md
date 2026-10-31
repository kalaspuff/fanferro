# 0012: Receives in flight follow the backlog

Status: Accepted

## Context

A `ReceiveMessage` call returns at most ten messages, and each receive loop (`pollers`, 1 by
default) keeps one call in flight. A consumer therefore received at most `10 × pollers` messages
per round trip, whatever its `concurrency`: with 15 ms per call (SQS from inside its region),
`concurrency=100` and handlers that wait 50 ms, a capacity of 2,000 messages per second, the
default single poller reached 451 (#172). In-memory and local emulator benchmarks did not show it,
because their receives take well under a millisecond. Raising `pollers` helped (4 reached 1,880),
but the right value depends on the latency the consumer sees, and every poller adds an always
running long poll, which SQS bills per request also when the queue is idle.

A receive also holds its in-flight permits during the round trip, before the messages exist. With
the default prefetch of one batch per poller, receives that overlap take permits from the
handlers: more receives alone reached about 1,450 messages per second in that benchmark.

## Decision

* `pollers` receive loops always run, as before. After a receive that came back full, the engine
  starts another loop if a full batch of in-flight permits is free and fewer than `max_pollers`
  loops run (default: as many as `max_in_flight` has full batches for).
* An extra loop ends at its first receive that is not full (short, empty or failed), or when no
  full batch of permits becomes free within `receive_batch_wait`. It waits at most a second for
  messages (not `wait_time`), so its permits come back soon after the backlog is gone. An idle
  queue costs `pollers` long polls per `wait_time`, as before.
* With the default prefetch, each extra loop adds one batch of in-flight permits while it runs
  (`extra_poller_slots` in the core) and gives them back once they are free after it ended: the
  prefetch grows with the receives in flight, as "one batch per poller" did. An explicit
  `prefetch` stays a strict bound, which extra loops only use the free slots of.

## Consequences

* The benchmark above reaches about 1,860 messages per second with the default options
  (`scripts/benchmark.py --latency 0.015 --scenarios consume-io`), without tuning `pollers`.
* With the default prefetch, a consumer leases at most `concurrency + prefetch` plus one batch per
  extra loop (`max_pollers - pollers` of them): a consumer whose handler slots are all free (at
  start, or after slow handlers finished together) can lease that many before its handlers take
  them. Slow handlers that finish at different times rarely leave a full batch free, so receives
  then stay close to one at a time.
* While a backlog drains, each extra loop ends with one receive that is not full: a few more
  requests per backlog, not per `wait_time`.
* `consumer.stats()["engine"]["pollers"]` shows the receive loops running.

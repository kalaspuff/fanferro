# 0017: Batch routes collect decoded messages in Python

Status: Accepted

## Context

Handlers received one message at a time (#209). Work that is cheaper in bulk (database inserts,
bulk indexing, APIs that take batches) had no supported shape: applications either made one round
trip per message, or buffered messages themselves with `auto_ack=False` and explicit settlements,
and had to get timeouts, shutdown and partial failures right on their own. The engine already
receives ten messages at a time and batches deletes, but it splits the batch before handlers see
it, and a receive batch is too small and too accidental to be the unit of work.

## Decision

* A route takes `batch=Batch(max_size, max_wait)`. Its handler takes `list[Message[T]]` or
  `list[T]` and is called once per batch: when the batch holds `max_size` messages, or `max_wait`
  seconds after its first message joined it.
* Batches are collected in the Python consumer, after routing, decoding and consume middleware,
  which still run for each message: a batch holds messages of one route only, all decoded into its
  type, and per-message middleware (idempotency, tracing, metrics) keeps working unchanged. Each
  message's task joins the batch at the end of its middleware chain and waits for its own outcome,
  which it then settles as a single message's: returning acks, raising applies the retry policy to
  every message, and a `BatchResult` fails the messages it lists, each with its own error (like
  Lambda's `batchItemFailures`). No Rust change was needed.
* `concurrency` counts handler calls: a message gives its slot back when it joins a batch, and the
  batch's call takes one. Each collected message keeps its in-flight permit and its heartbeats
  until it is settled, so `concurrency + prefetch` bounds collected messages too. A consumer
  without an explicit `prefetch` prefetches at least the largest `max_size`, so that a full batch
  fits; an explicit `prefetch` that leaves no room for it fails at start.
* FIFO queues keep their per-group sequencing: a group's next message is processed once the
  previous one is settled, so a batch holds at most the next message of each group, and no group
  is blocked behind a batch it is not in.
* Collected messages that were not handed to a handler when the consumer stops are treated like
  prefetched ones: released with `release_prefetched` (the default), else handed to the handler at
  once. Where no more messages can arrive (Lambda invocations, `fanferro.testing.dispatch_batch`,
  a stopping consumer), a batch is handed over as soon as every message on its way joined it.
* The batch handler runs in a task of its own, in the context the consumer started in; each
  message's `process` span covers its wait in the batch.

## Consequences

* A batch call's partial failures, retries and dead-lettering are per message, with the same
  policies and events as single-message handlers (`message.failed` per message;
  `BatchItemFailedError` for messages a result lists without an error).
* Messages wait up to `max_wait` in a batch, while holding leases: `max_wait` adds latency, and
  long waits need heartbeats (on by default).
* FIFO queues with few active groups get small batches, each after `max_wait`.
* The handler's own spans have no message as parent; a batch span linked to its messages' spans
  would be a later addition.

# Performance

Per message, everything that needs no Python objects runs in Rust: request signing, HTTP, the SQS
and SNS protocols, built-in envelopes, base64, leases and batching. The event loop is left with
codecs, routing and your handlers.

## Numbers

| Scenario | CPU per message, in-memory broker | CPU per message, MiniStack over HTTP | Messages per second, in-memory broker |
| --- | ---: | ---: | ---: |
| `publish`: `await queue.send(...)`, one after another | 31 µs | 140 µs | 30,000 |
| `publish-concurrent`: 64 sends at a time | 28 µs | 140 µs | 51,000 |
| `publish-batch-10`: `send_batch()` of 10 messages | 13 µs | 30 µs | 79,000 |
| `publish-batch-100`: `send_batch()` of 100 messages | 10 µs | 21 µs | 97,000 |
| `consume`: handler taking `fanferro.Message`, concurrency 100 | 18 µs | 55 µs | 86,000 |
| `consume-typed`: body decoded into a dataclass | 21 µs | 61 µs | 68,000 |

[`scripts/benchmark.py`](../scripts/benchmark.py), medians of ten runs: fanferro 0.4.0, Python
3.14, Linux aarch64 (10 cores, a VM on Apple silicon), a small JSON body, an empty handler. CPU is
that of the whole process: the event loop and the Rust threads. The in-memory broker answers at
once, so its columns are fanferro's own cost; over HTTP the CPU adds signing and the SQS protocol,
while the elapsed time is MiniStack's (an emulator written in Python). Against SQS, round trips of
a few milliseconds dominate, and batches, prefetch and concurrency overlap them.

Round trips to SQS cap a consumer that receives one batch at a time. With a proxy that adds 15 ms
to every call in front of MiniStack (`--latency 0.015`), `consume-io` (handlers that wait 50 ms,
`concurrency=100`: at most 2,000 messages per second, 3,000 messages queued) reached:

| Receive loops | Messages per second |
| --- | ---: |
| one at a time (`max_pollers=1`, the earlier behaviour) | 451 |
| default (extra loops while receives come back full) | 1,856 |
| `pollers=4` (four always running) | 1,880 |

## What keeps the cost down

* **Full batches, overlapping.** Pollers receive full batches of ten and prefetch the next one
  while handlers run; while receives come back full, more receives run at once. Acks and visibility changes go out ten per call, also while earlier calls are
  in flight, and heartbeats extend ten leases per call. `send_batch()` and `publish_batch()` keep
  up to eight calls in flight (standard queues and topics).
* **Acks do not hold handler slots.** A handler's slot goes to the next message as soon as it
  returns, while the delete is batched with others.
* **Little Python per message.** Built-in envelopes are unwrapped in Rust while a batch is decoded,
  dataclass converters are compiled once per type, and a trivial awaitable costs about 14 µs of CPU
  instead of 75 µs with `pyo3-async-runtimes`
  ([ADR 0009](adr/0009-results-reach-asyncio-in-batches.md)).
* **Lean defaults.** Raw bodies with few attributes, no MD5 digests on top of TLS, events built only
  for kinds that something logs or hooks, and no span work without an OpenTelemetry SDK
  ([ADR 0008](adr/0008-lean-defaults.md)).

## Run it yourself

```bash
uv run python scripts/benchmark.py --repeat 5                                        # in-memory broker
uv run python scripts/benchmark.py --endpoint http://localhost:4566 -n 2000 --repeat 3  # MiniStack
uv run python scripts/benchmark.py --endpoint http://localhost:4566 --latency 0.015 -n 3000 --scenarios consume-io
```

`-o NAME=JSON` passes a consumer option to the consume scenarios (`-o max_pollers=1`). `--body-size
BYTES` sends text bodies of that size instead of the small JSON object, and `--offload` stores every
payload in S3; run them with a few messages (`--body-size 4000000 --offload -n 40`).

Start MiniStack with `uv run ministack` first.

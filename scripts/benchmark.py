"""Publishing and consuming throughput, per message: wall time and CPU time of this process.

    uv run python scripts/benchmark.py                        # in-memory broker
    uv run python scripts/benchmark.py --endpoint http://127.0.0.1:4566 -n 2000   # MiniStack
    uv run python scripts/benchmark.py --endpoint http://127.0.0.1:4566 --latency 0.015 --scenarios consume-io

CPU time is the whole process (the event loop and the Rust runtime's threads), so it shows what
fanferro costs per message even when the broker is the bottleneck of the wall time. Each scenario
uses a queue of its own; consumers handle messages with an empty handler, except in ``consume-io``,
whose handlers wait 50 ms (I/O-bound, ``concurrency=100``: at most 2,000 messages per second).
``--latency`` puts a proxy in front of the endpoint that delays every call, like the round trip
to SQS from inside its region (about 15 ms), which local endpoints do not have. Results vary
between runs: compare the best of a few (``--repeat``), on the same machine.
"""

from __future__ import annotations

import argparse
import asyncio
import http.client
import http.server
import json
import threading
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import fanferro
from fanferro import testing

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

BODY = {"order_id": 1, "total": 12.5, "customer": "c-123", "items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 1}]}


@dataclass(frozen=True)
class Order:
    """The body above as a dataclass (typed consumers decode into it)."""

    order_id: int
    total: float
    customer: str
    items: list[dict[str, Any]]


@dataclass(frozen=True)
class Result:
    scenario: str
    messages: int
    wall: float
    cpu: float

    def row(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "messages": self.messages,
            "messages_per_second": round(self.messages / self.wall),
            "wall_us_per_message": round(self.wall / self.messages * 1e6, 1),
            "cpu_us_per_message": round(self.cpu / self.messages * 1e6, 1),
        }


async def measure(scenario: str, messages: int, work: Callable[[], Awaitable[None]]) -> Result:
    cpu, wall = time.process_time(), time.perf_counter()
    await work()
    return Result(scenario, messages, time.perf_counter() - wall, time.process_time() - cpu)


async def publish(queue: fanferro.Queue, messages: int) -> None:
    for _ in range(messages):
        await queue.send(BODY)


async def publish_concurrently(queue: fanferro.Queue, messages: int, concurrency: int = 64) -> None:
    limit = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with limit:
            await queue.send(BODY)

    await asyncio.gather(*(one() for _ in range(messages)))


async def publish_batches(queue: fanferro.Queue, messages: int, size: int) -> None:
    for start in range(0, messages, size):
        await queue.send_batch([BODY] * min(size, messages - start))


async def fill(queue: fanferro.Queue, messages: int) -> None:
    calls = [queue.send_batch([BODY] * min(100, messages - start)) for start in range(0, messages, 100)]
    for start in range(0, len(calls), 8):
        await asyncio.gather(*calls[start : start + 8])


class _DelayingProxy(http.server.BaseHTTPRequestHandler):
    """Forwards every request to ``target`` after ``delay`` seconds (keep-alive, no Nagle delays)."""

    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    wbufsize = -1
    target: tuple[str, int] = ("127.0.0.1", 4566)
    delay = 0.0

    def setup(self) -> None:
        super().setup()
        self.upstream = http.client.HTTPConnection(*self.target)

    def finish(self) -> None:
        self.upstream.close()
        super().finish()

    def do_POST(self) -> None:
        self._forward()

    def do_GET(self) -> None:
        self._forward()

    def do_PUT(self) -> None:
        self._forward()

    def do_DELETE(self) -> None:
        self._forward()

    def do_HEAD(self) -> None:
        self._forward()

    def _forward(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        time.sleep(self.delay)
        headers = {key: value for key, value in self.headers.items() if key.lower() != "connection"}
        self.upstream.request(self.command, self.path, body=body, headers=headers)
        response = self.upstream.getresponse()
        data = response.read()
        self.send_response(response.status)
        for key, value in response.getheaders():
            if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        pass


def start_proxy(endpoint: str, delay: float) -> str:
    """Starts a proxy in front of ``endpoint`` that delays every call by ``delay``; returns its URL."""
    parsed = urllib.parse.urlsplit(endpoint)
    handler = type(
        "Proxy", (_DelayingProxy,), {"target": (parsed.hostname or "127.0.0.1", parsed.port or 80), "delay": delay}
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_address[1]}"


async def consume(
    client: fanferro.Client,
    queue: fanferro.Queue,
    messages: int,
    *,
    typed: bool,
    io: bool = False,
    options: dict[str, Any] | None = None,
) -> Result:
    await fill(queue, messages)
    done = asyncio.Event()
    handled = 0

    def count() -> None:
        nonlocal handled
        handled += 1
        if handled >= messages:
            done.set()

    async def handle_message(message: fanferro.Message[Any]) -> None:
        if io:
            await asyncio.sleep(0.05)
        count()

    async def handle_order(order: Order) -> None:
        count()

    handler = handle_order if typed else handle_message
    consumer = client.consumer(queue, handler, **({"concurrency": 100, "wait_time": 1} | (options or {})))
    scenario = "consume-typed" if typed else "consume-io" if io else "consume"

    async def work() -> None:
        await consumer.start()
        await asyncio.wait_for(done.wait(), timeout=600)

    try:
        return await measure(scenario, messages, work)
    finally:
        await consumer.stop()


SCENARIOS = (
    "publish",
    "publish-concurrent",
    "publish-batch-10",
    "publish-batch-100",
    "consume",
    "consume-typed",
    "consume-io",
)


async def run(scenario: str, messages: int, endpoint: str | None, options: dict[str, Any] | None = None) -> Result:
    quiet = fanferro.LoggingConfig(enabled=False)  # e.g. warnings about the missing dead-letter queue
    if endpoint is None:
        client = testing.memory_client(logging=quiet)
    else:
        client = fanferro.Client(
            region="us-east-1",
            endpoint_url=endpoint,
            credentials=fanferro.Credentials("test", "test"),
            account_id="000000000000",
            provisioning=fanferro.Provisioning.ensure(),
            logging=quiet,
        )
    async with client:
        queue = client.queue(f"benchmark-{scenario}-{uuid.uuid4().hex[:8]}")
        await queue.resolve()
        try:
            if scenario == "publish":
                return await measure(scenario, messages, lambda: publish(queue, messages))
            if scenario == "publish-concurrent":
                return await measure(scenario, messages, lambda: publish_concurrently(queue, messages))
            if scenario.startswith("publish-batch-"):
                size = int(scenario.removeprefix("publish-batch-"))
                return await measure(scenario, messages, lambda: publish_batches(queue, messages, size))
            return await consume(
                client,
                queue,
                messages,
                typed=scenario == "consume-typed",
                io=scenario == "consume-io",
                options=options,
            )
        finally:
            if endpoint is not None:
                await queue.delete()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--endpoint", help="an AWS-compatible endpoint, e.g. MiniStack (default: in-memory broker)")
    parser.add_argument("-n", "--messages", type=int, default=5000, help="messages per scenario (default 5000)")
    parser.add_argument("--repeat", type=int, default=1, help="runs per scenario; the best one is reported")
    parser.add_argument(
        "--scenarios", default=",".join(SCENARIOS), help=f"comma-separated, from {', '.join(SCENARIOS)}"
    )
    parser.add_argument(
        "--latency", type=float, default=0.0, help="seconds added to every call to --endpoint (e.g. 0.015)"
    )
    parser.add_argument(
        "-o",
        "--consumer-option",
        action="append",
        default=[],
        metavar="NAME=JSON",
        help="a consumer option for the consume scenarios, e.g. -o pollers=4 (repeatable)",
    )
    parser.add_argument("--json", action="store_true", help="print one JSON object per scenario")
    args = parser.parse_args()
    try:
        options = {key: json.loads(value) for key, _, value in (o.partition("=") for o in args.consumer_option)}
    except json.JSONDecodeError as error:
        parser.error(f"--consumer-option values are JSON: {error}")
    if args.latency and not args.endpoint:
        parser.error("--latency needs --endpoint")
    endpoint = start_proxy(args.endpoint, args.latency) if args.latency else args.endpoint
    scenarios = [scenario.strip() for scenario in args.scenarios.split(",") if scenario.strip()]
    unknown = sorted(set(scenarios) - set(SCENARIOS))
    if unknown:
        parser.error(f"unknown scenarios: {', '.join(unknown)}")
    if not args.json:
        print(f"{'scenario':20} {'messages/s':>11} {'wall µs/msg':>12} {'CPU µs/msg':>11}")
    for scenario in scenarios:
        runs = [await run(scenario, args.messages, endpoint, options) for _ in range(max(1, args.repeat))]
        row = min(runs, key=lambda result: result.wall).row()
        if args.json:
            print(json.dumps(row), flush=True)
        else:
            print(
                f"{row['scenario']:20} {row['messages_per_second']:>11} {row['wall_us_per_message']:>12} "
                f"{row['cpu_us_per_message']:>11}",
                flush=True,
            )


if __name__ == "__main__":
    asyncio.run(main())

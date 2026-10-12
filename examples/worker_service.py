"""A production-style worker: configuration, several consumers, policies and graceful shutdown.

``client.run()`` handles SIGINT/SIGTERM: the first signal drains in-flight messages (up to the
shutdown policy's timeout), a second one stops immediately.
Run: uv run python examples/worker_service.py   (stops by itself after the demo messages)
"""

from __future__ import annotations

import asyncio
import os

import fanferro
from examples._common import configure_logging, make_client, unique


def build(client: fanferro.Client) -> tuple[fanferro.Topic, list[fanferro.Consumer]]:
    events = client.topic(os.environ.get("EVENTS_TOPIC", unique("events")))
    emails = client.queue(unique("emails"), dead_letter=fanferro.DeadLetterQueue(unique("emails-dlq")))
    search = client.queue(unique("search-index"))
    client.subscription(events, emails, filter_policy={"fanferro.type": ["user.signed_up"]})
    client.subscription(events, search)

    email_consumer = client.consumer(
        emails,
        concurrency=20,
        rate_limit=fanferro.RateLimit(per_second=50),  # protect the mail provider
        retry=fanferro.RetryPolicy(max_attempts=5, backoff=fanferro.Backoff.exponential(initial=2, maximum=300)),
        shutdown=fanferro.ShutdownPolicy(drain_timeout=25),  # below the orchestrator's grace period
        wait_time=1,
    )
    search_consumer = client.consumer(search, concurrency=5, prefetch=10, handler_timeout=30, wait_time=1)

    @email_consumer.route("user.signed_up")
    async def send_welcome(message: fanferro.Message[dict[str, str]]) -> None:
        print(f"welcome email to {message.body['email']}")

    @search_consumer.default
    async def index(message: fanferro.Message[dict[str, str]]) -> None:
        print(f"indexing {message.type}")

    return events, [email_consumer, search_consumer]


async def main() -> None:
    configure_logging()
    client = make_client(source="urn:service:accounts")
    events, consumers = build(client)
    async with client:
        await events.publish({"email": "ada@example.com"}, type="user.signed_up")
        await events.publish({"user": "ada"}, type="user.profile_updated")

        async def stop_soon() -> None:
            await asyncio.sleep(3)
            client.stop()  # in production a signal does this

        stopper = asyncio.create_task(stop_soon())
        await client.run(*consumers, install_signal_handlers=False)
        await stopper


if __name__ == "__main__":
    asyncio.run(main())

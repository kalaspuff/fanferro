"""Large payloads (claim checks): bodies above the threshold go to S3, a pointer goes through SNS/SQS.

Consumers download payloads transparently. With ``reference="both"`` the pointer also carries a
presigned URL, so consumers without S3 permissions (other teams, other accounts) can read it.
Run: uv run python examples/large_payloads.py
"""

from __future__ import annotations

import asyncio

import fanferro
from examples._common import make_client, unique


async def main() -> list[int]:
    client = make_client(
        large_payloads=fanferro.LargePayloads(
            bucket=unique("payloads"),
            prefix="events/",
            threshold=64 * 1024,
            reference="both",
            create_bucket=True,  # usually the bucket is managed by IaC with a lifecycle rule
        )
    )
    reports = client.topic(unique("reports"))
    archive = client.queue(unique("archive"))
    client.subscription(reports, archive)
    sizes: list[int] = []
    consumer = client.consumer(archive, wait_time=1)

    @consumer.default
    async def store(message: fanferro.Message[bytes]) -> None:
        pointer = message.pointer
        sizes.append(len(message.body))
        where = f"s3://{pointer.bucket}/{pointer.key}" if pointer else "inline"
        print(f"received {len(message.body)} bytes ({where})")

    async with client:
        small = await reports.publish(b"x" * 1_000)
        large = await reports.publish(b"y" * 2_000_000)
        assert small.offloaded is None
        assert large.offloaded is not None
        await consumer.run_until_idle(timeout=60)
    return sorted(sizes)


if __name__ == "__main__":
    asyncio.run(main())

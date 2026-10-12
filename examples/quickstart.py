"""Quickstart: a topic fanning out to a queue, a typed consumer, and graceful shutdown.

Run: uv run python examples/quickstart.py
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

import fanferro
from examples._common import configure_logging, make_client, unique


@fanferro.message_type("shop.order_placed.v1")
class OrderPlaced(BaseModel):
    order_id: str
    amount_cents: int


async def main() -> list[OrderPlaced]:
    configure_logging()
    client = make_client(source="urn:service:checkout")

    # Declare infrastructure. With Provisioning.ensure() fanferro creates what is missing and fixes
    # drifted attributes when the client starts; in production you often use Provisioning.existing()
    # and manage infrastructure with Terraform/CDK instead.
    orders = client.topic(unique("orders"))
    billing = client.queue(
        unique("billing"),
        attributes=fanferro.QueueAttributes(visibility_timeout=30),
        dead_letter=fanferro.DeadLetterQueue(unique("billing-dlq"), max_receive_count=5),
    )
    client.subscription(orders, billing, filter_policy={"fanferro.type": ["shop.order_placed.v1"]})

    received: list[OrderPlaced] = []
    consumer = client.consumer(billing, concurrency=10, wait_time=1)

    @consumer.route(OrderPlaced)
    async def charge(order: OrderPlaced) -> None:
        received.append(order)
        if len(received) == 3:
            consumer.request_stop()

    async with client:  # connects and provisions everything declared above
        for i in range(3):
            await orders.publish(OrderPlaced(order_id=f"o-{i}", amount_cents=1000 + i), attributes={"tenant": "acme"})
        await asyncio.wait_for(consumer.run(install_signal_handlers=False), timeout=60)

    print(f"charged {[order.order_id for order in received]}")
    return received


if __name__ == "__main__":
    asyncio.run(main())

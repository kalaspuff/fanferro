"""Routing by message type, version header, attributes and body fields.

Producers evolve messages over time; consumers route each version to the right handler.
Run: uv run python examples/routing_versions.py
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import fanferro
from examples._common import make_client, unique
from fanferro import matchers as m


@fanferro.message_type("crm.customer_created", "crm.customer_created.v1")
@dataclasses.dataclass
class CustomerCreatedV1:
    id: int
    name: str


@fanferro.message_type("crm.customer_created.v2")
@dataclasses.dataclass
class CustomerCreatedV2:
    id: int
    first_name: str
    last_name: str


async def main() -> list[str]:
    client = make_client()
    queue = client.queue(unique("customers"))
    log: list[str] = []
    router = fanferro.Router()

    @router.route(CustomerCreatedV2)
    async def created_v2(customer: CustomerCreatedV2) -> None:
        log.append(f"v2 {customer.first_name} {customer.last_name}")

    @router.route(CustomerCreatedV1, when=m.header("schema") == "legacy")
    async def created_legacy(customer: CustomerCreatedV1) -> None:
        log.append(f"legacy {customer.name}")

    @router.route(CustomerCreatedV1)
    async def created_v1(customer: CustomerCreatedV1) -> None:
        log.append(f"v1 {customer.name}")

    @router.route(when=(m.attribute("priority") >= 5) & m.body("vip").exists(), priority=10)
    async def vip(message: fanferro.Message[dict[str, Any]]) -> None:
        log.append(f"vip {message.body['vip']}")

    @router.default
    async def unknown(message: fanferro.Message[bytes]) -> None:
        log.append(f"unknown type {message.type}")

    consumer = client.consumer(queue, router, wait_time=1)
    async with client:
        await queue.send(CustomerCreatedV1(1, "Ada Lovelace"))
        await queue.send(CustomerCreatedV1(2, "Grace Hopper"), headers={"schema": "legacy"})
        await queue.send(CustomerCreatedV2(3, "Alan", "Turing"))
        await queue.send({"vip": "Margaret"}, attributes={"priority": 7}, type="crm.anything")
        await queue.send({"what": "?"}, type="crm.customer_deleted")
        await consumer.run_until_idle(timeout=30)

    for line in sorted(log):
        print(line)
    return sorted(log)


if __name__ == "__main__":
    asyncio.run(main())

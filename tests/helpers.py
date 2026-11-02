"""Message types shared by integration and e2e tests (the flow helpers are in ``fanferro.testing``)."""

from __future__ import annotations

import dataclasses

import pydantic

import fanferro


@fanferro.message_type("test.order_created.v1")
class OrderCreated(pydantic.BaseModel):
    order_id: int
    total: float = 0.0


@fanferro.message_type("test.order_cancelled.v1")
@dataclasses.dataclass
class OrderCancelled:
    order_id: int
    reason: str = ""

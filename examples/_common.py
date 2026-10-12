"""Shared setup for the examples.

Every example runs against the in-memory broker by default, so it works without AWS or Docker:

    uv run python examples/quickstart.py

Point them at MiniStack (``ministack`` from the dev dependencies) or AWS with environment variables:

    FANFERRO_ENDPOINT_URL=http://localhost:4566 uv run python examples/quickstart.py
    FANFERRO_USE_AWS=1 AWS_REGION=eu-west-1 uv run python examples/quickstart.py
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

import fanferro
from fanferro import testing


def make_client(**options: Any) -> fanferro.Client:
    """A client for the configured backend; resources are created on demand."""
    options.setdefault("provisioning", fanferro.Provisioning.ensure())
    endpoint = os.environ.get("FANFERRO_ENDPOINT_URL")
    if endpoint:
        return fanferro.Client(
            endpoint_url=endpoint,
            region=os.environ.get("AWS_REGION", "us-east-1"),
            credentials=fanferro.Credentials("test", "test"),
            **options,
        )
    if os.environ.get("FANFERRO_USE_AWS"):
        return fanferro.Client(**options)
    return testing.memory_client(**options)


def unique(name: str) -> str:
    """Resource names that do not collide between runs on a shared emulator or account."""
    return f"{name}-{uuid.uuid4().hex[:8]}"


def configure_logging() -> None:
    """Readable logs for the examples."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")

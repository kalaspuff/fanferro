"""Idempotency, outbox, testing helpers, CLI and package consistency."""

from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path

import pytest

import fanferro
from fanferro import _core
from fanferro.__main__ import main
from fanferro.consumer import Duplicate
from fanferro.idempotency import Claim, InMemoryIdempotencyStore, idempotent
from fanferro.outbox import InMemoryOutbox
from fanferro.testing import build_message

ROOT = Path(__file__).resolve().parents[2]


async def test_in_memory_idempotency_store() -> None:
    now = [0.0]
    store = InMemoryIdempotencyStore(clock=lambda: now[0])
    assert await store.claim("k", lock_ttl=10) is Claim.ACQUIRED
    assert await store.claim("k", lock_ttl=10) is Claim.IN_PROGRESS
    now[0] = 11
    assert await store.claim("k", lock_ttl=10) is Claim.ACQUIRED, "expired claims can be taken over"
    await store.complete("k", ttl=100)
    assert await store.claim("k", lock_ttl=10) is Claim.COMPLETED
    await store.release("k")
    assert await store.claim("k", lock_ttl=10) is Claim.ACQUIRED
    assert len(store) == 1


async def test_idempotency_middleware() -> None:
    middleware = idempotent()
    message = build_message({"a": 1}, id="same")
    calls: list[str] = []

    async def handler(msg: fanferro.Message[object]) -> None:
        calls.append(msg.id)

    await middleware(message, handler)
    with pytest.raises(Duplicate):
        await middleware(build_message({"a": 1}, id="same"), handler)

    async def failing(msg: fanferro.Message[object]) -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await middleware(build_message({}, id="other"), failing)
    await middleware(build_message({}, id="other"), handler)
    assert calls == ["same", "other"]


async def test_idempotency_middleware_releases_messages_handed_back() -> None:
    """A handler that settles the message for another attempt must run again on the next delivery."""
    middleware = idempotent()
    handed_back = {
        "retry": lambda msg: msg.retry(delay=0),
        "requeue": lambda msg: msg.requeue(delay=0),
        "release": lambda msg: msg.release(),
        "dead_letter": lambda msg: msg.dead_letter("later"),
    }
    for name, settle in handed_back.items():
        calls: list[str] = []

        async def handler(msg: fanferro.Message[object], settle=settle, calls=calls) -> None:
            calls.append(msg.id)
            if len(calls) == 1:
                await settle(msg)

        await middleware(build_message({}, id=name), handler)
        await middleware(build_message({}, id=name), handler)
        assert calls == [name, name], name

    finished = {"ack": lambda msg: msg.ack(), "discard": lambda msg: msg.discard("irrelevant")}
    for name, settle in finished.items():

        async def settling(msg: fanferro.Message[object], settle=settle) -> None:
            await settle(msg)

        await middleware(build_message({}, id=name), settling)
        with pytest.raises(Duplicate):
            await middleware(build_message({}, id=name), settling)


async def test_concurrent_claims_retry() -> None:
    middleware = idempotent(retry_delay=7)
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(msg: fanferro.Message[object]) -> None:
        started.set()
        await release.wait()

    first = asyncio.create_task(middleware(build_message({}, id="x"), slow))
    await started.wait()
    with pytest.raises(fanferro.Retry) as raised:
        await middleware(build_message({}, id="x"), slow)
    assert raised.value.delay == 7
    release.set()
    await first


async def test_in_memory_outbox() -> None:
    outbox = InMemoryOutbox()
    prepared = fanferro.PreparedMessage(
        destination={"kind": "queue"}, destination_name="q", wire=fanferro.WireMessage("x"), message_id="1"
    )
    first = outbox.add(prepared)
    outbox.add(prepared, record_id="custom")
    claimed = await outbox.claim_batch(10)
    assert [r.id for r in claimed] == [first, "custom"]
    assert await outbox.claim_batch(10) == []
    await outbox.mark_failed(claimed[1], RuntimeError("x"))
    await outbox.mark_published([first])
    assert outbox.published == [first]
    assert [r.attempts for r in outbox.pending] == [1]


def test_cli(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == fanferro.__version__
    main(["events"])
    assert "message.acked" in capsys.readouterr().out
    main(["attributes"])
    assert "fanferro.claim-check" in capsys.readouterr().out
    main(["guide"])
    assert "fanferro" in capsys.readouterr().out
    main([])
    assert "usage" in capsys.readouterr().out


def test_public_api_is_exported() -> None:
    for name in fanferro.__all__:
        assert hasattr(fanferro, name), name
    assert sorted(fanferro.__all__) == fanferro.__all__


def test_stub_matches_native_module() -> None:
    tree = ast.parse((ROOT / "python" / "fanferro" / "_core.pyi").read_text())
    stub_names = {node.name for node in tree.body if isinstance(node, ast.ClassDef | ast.FunctionDef)} | {
        node.target.id for node in tree.body if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    stub_names -= {"AttributeInput", "HeaderValue"}
    runtime_names = {name for name in dir(_core) if not name.startswith("__") or name == "__version__"}
    assert stub_names == runtime_names


def test_version_consistency() -> None:
    cargo = (ROOT / "Cargo.toml").read_text()
    version = re.search(r'^version = "([^"]+)"', cargo, re.MULTILINE)
    assert version is not None
    assert fanferro.__version__ == version.group(1)

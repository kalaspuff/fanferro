"""Idempotency, outbox, testing helpers, CLI and package consistency."""

from __future__ import annotations

import ast
import asyncio
import re
from collections.abc import Sequence
from pathlib import Path

import pytest

import fanferro
from fanferro import _core
from fanferro.__main__ import main
from fanferro.consumer import Duplicate, _Postpone
from fanferro.idempotency import Claim, InMemoryIdempotencyStore, idempotent
from fanferro.outbox import InMemoryOutbox, OutboxRecord, OutboxRelay
from fanferro.testing import build_message
from tests.helpers import drain_queue, wait_for

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


async def test_in_memory_idempotency_store_forgets_expired_keys() -> None:
    now = [0.0]
    store = InMemoryIdempotencyStore(clock=lambda: now[0])
    for index in range(100):
        await store.claim(f"done-{index}", lock_ttl=10)
        await store.complete(f"done-{index}", ttl=600)
        await store.claim(f"abandoned-{index}", lock_ttl=10)
    now[0] = 300.0
    await store.claim("new", lock_ttl=10)
    assert len(store) == 101, "expired claims are forgotten, completed keys are kept for their ttl"
    now[0] = 700.0
    await store.claim("newer", lock_ttl=10)
    assert len(store) == 1
    assert await store.claim("done-1", lock_ttl=10) is Claim.ACQUIRED


async def test_concurrent_claims_retry() -> None:
    middleware = idempotent(retry_delay=7)
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(msg: fanferro.Message[object]) -> None:
        started.set()
        await release.wait()

    first = asyncio.create_task(middleware(build_message({}, id="x"), slow))
    await started.wait()
    with pytest.raises(_Postpone) as raised:
        await middleware(build_message({}, id="x"), slow)
    assert raised.value.delay == 7, "postponed, not retried: no attempt is used up"
    release.set()
    await first


async def test_idempotency_stores_may_return_strings() -> None:
    """Claims are compared by value: a store reading them back from a database returns strings."""

    class StringStore:
        def __init__(self) -> None:
            self.state: dict[str, str] = {}

        async def claim(self, key: str, *, lock_ttl: float) -> str:
            if key in self.state:
                return self.state[key]
            self.state[key] = "in_progress"
            return "acquired"

        async def complete(self, key: str, *, ttl: float) -> None:
            self.state[key] = "completed"

        async def release(self, key: str) -> None:
            self.state.pop(key, None)

    store = StringStore()
    middleware = idempotent(store)
    calls: list[str] = []

    async def handler(msg: fanferro.Message[object]) -> None:
        calls.append(msg.id)

    await middleware(build_message({}, id="s"), handler)
    with pytest.raises(Duplicate):
        await middleware(build_message({}, id="s"), handler)
    store.state["queue:t"] = "in_progress"
    with pytest.raises(_Postpone):
        await middleware(build_message({}, id="t", queue="queue"), handler)
    assert calls == ["s"]


async def test_idempotency_middleware_survives_a_failed_completion() -> None:
    class FailingStore(InMemoryIdempotencyStore):
        async def complete(self, key: str, *, ttl: float) -> None:
            raise ConnectionError("store unavailable")

    calls: list[str] = []

    async def handler(msg: fanferro.Message[object]) -> None:
        calls.append(msg.id)

    await idempotent(FailingStore())(build_message({}, id="c"), handler)  # does not raise
    assert calls == ["c"]


def test_idempotent_keeps_an_empty_store() -> None:
    store = InMemoryIdempotencyStore()
    assert idempotent(store).store is store, "an empty store has a length of 0, but is the store to use"


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
    retried = await outbox.claim_batch(10)
    await outbox.mark_abandoned(retried[0], RuntimeError("x"))
    assert outbox.pending == []
    assert [(r.id, r.attempts) for r in outbox.abandoned] == [("custom", 2)]
    assert await outbox.claim_batch(10) == []


async def test_the_outbox_relay_never_gives_up_on_temporary_failures(
    memory_client: fanferro.Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = memory_client.queue("outbox-throttled")
    outbox = InMemoryOutbox()
    throttled = fanferro.ThrottledError("Rate exceeded")
    throttled.retryable = True  # as raised by the core

    async def fail(messages: list[fanferro.PreparedMessage], **_: object) -> fanferro.BatchPublishResult:
        return fanferro.BatchPublishResult(
            tuple(fanferro.BatchFailure(i, m, throttled) for i, m in enumerate(messages))
        )

    async with memory_client:
        outbox.add(await queue.prepare({"n": 1}))
        relay = OutboxRelay(memory_client, outbox, max_attempts=2)
        with monkeypatch.context() as patched:
            patched.setattr(memory_client, "publish_prepared_batch", fail)
            for _ in range(4):
                assert await relay.run_once() == 0
        assert outbox.abandoned == []
        assert [r.attempts for r in outbox.pending] == [4]
        assert await relay.run_once() == 1


async def test_the_outbox_relay_counts_only_failures_for_good(
    memory_client: fanferro.Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Rounds of throttling (or of waiting behind an earlier record of a FIFO group) are attempts for
    # the store, but must not bring a record closer to being given up.
    queue = memory_client.queue("outbox-later-gone")
    outbox = InMemoryOutbox()
    errors: list[BaseException] = []
    throttled = fanferro.ThrottledError("Rate exceeded")
    throttled.retryable = True

    async def fail(messages: list[fanferro.PreparedMessage], **_: object) -> fanferro.BatchPublishResult:
        error = errors.pop(0)
        return fanferro.BatchPublishResult(tuple(fanferro.BatchFailure(i, m, error) for i, m in enumerate(messages)))

    async with memory_client:
        outbox.add(await queue.prepare({"n": 1}))
        relay = OutboxRelay(memory_client, outbox, max_attempts=3)
        monkeypatch.setattr(memory_client, "publish_prepared_batch", fail)
        errors.extend([throttled] * 5 + [fanferro.ResourceNotFoundError("gone")] * 3)
        for _ in range(7):
            await relay.run_once()
        assert outbox.abandoned == [], "two failures for good of three"
        await relay.run_once()
        assert [(r.attempts, len(outbox.pending)) for r in outbox.abandoned] == [(8, 0)]


class _FlakyOutbox(InMemoryOutbox):
    """Its ``mark_failed`` raises once (a database blip)."""

    def __init__(self) -> None:
        super().__init__()
        self.blips = 1

    async def mark_failed(self, record: OutboxRecord, error: BaseException) -> None:
        if self.blips:
            self.blips -= 1
            raise ConnectionError("database went away")
        await super().mark_failed(record, error)


async def test_an_outbox_store_error_on_a_failed_record_does_not_end_the_round(memory_client: fanferro.Client) -> None:
    # A mark_failed() that raised ended the round before mark_published: the records already sent
    # stayed claimed (and pending), so they were never relayed again, nor marked.
    outbox = _FlakyOutbox()
    gone = memory_client.queue("outbox-blip-gone")
    worker = memory_client.queue("outbox-blip-worker")
    fifo = memory_client.queue("outbox-blip.fifo")
    async with memory_client:
        lost = outbox.add(await gone.prepare({"n": 0}))
        sent = [outbox.add(await worker.prepare({"n": i})) for i in (1, 2)]
        ordered = outbox.add(await fifo.prepare({"n": 3}, group_id="g"))
        await gone.delete()
        relay = OutboxRelay(memory_client, outbox)
        assert await relay.run_once() == 3
        assert outbox.published == [*sent, ordered]
        assert outbox.claimed == [], "the record whose failure was not recorded is released"
        assert [(r.id, r.attempts) for r in outbox.pending] == [(lost, 0)]
        assert await relay.run_once() == 0
        assert [(r.id, r.attempts) for r in outbox.pending] == [(lost, 1)]
        assert len(await drain_queue(memory_client, worker)) == 2


async def test_an_outbox_store_error_marking_published_records_releases_them(memory_client: fanferro.Client) -> None:
    class Store(InMemoryOutbox):
        blips = 1

        async def mark_published(self, ids: Sequence[str]) -> None:
            if self.blips:
                self.blips -= 1
                raise ConnectionError("database went away")
            await super().mark_published(ids)

    outbox = Store()
    worker = memory_client.queue("outbox-mark-blip")
    async with memory_client:
        record = outbox.add(await worker.prepare({"n": 1}))
        relay = OutboxRelay(memory_client, outbox)
        assert await relay.run_once() == 1
        assert outbox.claimed == [], "released, to be published again (at least once)"
        assert await relay.run_once() == 1
        assert outbox.published == [record]


async def test_a_failing_prepared_batch_call_fails_its_records(
    memory_client: fanferro.Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    outbox = InMemoryOutbox()
    queue = memory_client.queue("outbox-batch-raises")

    async def fail(messages: list[fanferro.PreparedMessage], **_: object) -> fanferro.BatchPublishResult:
        raise fanferro.AccessDeniedError("not allowed")

    async with memory_client:
        outbox.add(await queue.prepare({"n": 1}))
        relay = OutboxRelay(memory_client, outbox)
        with monkeypatch.context() as patched:
            patched.setattr(memory_client, "publish_prepared_batch", fail)
            assert await relay.run_once() == 0
        assert outbox.claimed == []
        assert [r.attempts for r in outbox.pending] == [1]
        assert await relay.run_once() == 1


async def test_a_cancelled_outbox_round_marks_what_it_published(
    memory_client: fanferro.Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Cancelling relay.run() at shutdown mid-round left the standard records it had sent unmarked
    # (they were published again) and every claim held (the in-memory store never handed them out).
    outbox = InMemoryOutbox()
    worker = memory_client.queue("outbox-cancel-worker")
    fifo = memory_client.queue("outbox-cancel.fifo")
    sending = asyncio.Event()

    async def hang(prepared: fanferro.PreparedMessage) -> fanferro.PublishResult:
        sending.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with memory_client:
        sent = [outbox.add(await worker.prepare({"n": i})) for i in range(2)]
        held = [outbox.add(await fifo.prepare({"n": i}, group_id="g")) for i in range(2)]
        relay = OutboxRelay(memory_client, outbox)
        with monkeypatch.context() as patched:
            patched.setattr(memory_client, "publish_prepared", hang)
            task = asyncio.create_task(relay.run())
            await asyncio.wait_for(sending.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert outbox.published == sent
        assert outbox.claimed == []
        assert [(r.id, r.attempts) for r in outbox.pending] == [(i, 0) for i in held]
        assert await relay.run_once() == 2
        assert outbox.published == [*sent, *held]
        assert len(await drain_queue(memory_client, worker)) == 2


async def test_an_outbox_round_falls_back_to_mark_failed_without_release(
    memory_client: fanferro.Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Store:
        """An in-memory store without the optional ``release``."""

        def __init__(self, inner: InMemoryOutbox) -> None:
            self.inner = inner
            self.claim_batch = inner.claim_batch
            self.mark_published = inner.mark_published
            self.mark_failed = inner.mark_failed
            self.mark_abandoned = inner.mark_abandoned

    outbox = InMemoryOutbox()
    fifo = memory_client.queue("outbox-norelease.fifo")

    async def boom(prepared: fanferro.PreparedMessage) -> fanferro.PublishResult:
        raise KeyboardInterrupt

    async with memory_client:
        outbox.add(await fifo.prepare({"n": 1}, group_id="g"))
        relay = OutboxRelay(memory_client, Store(outbox), max_attempts=1)
        with monkeypatch.context() as patched:
            patched.setattr(memory_client, "publish_prepared", boom)
            with pytest.raises(KeyboardInterrupt):
                await relay.run_once()
        assert outbox.claimed == []
        assert outbox.abandoned == [], "a record not attempted does not count towards giving up"
        assert [r.attempts for r in outbox.pending] == [1]


async def test_in_memory_outbox_claims_fifo_groups_exclusively(memory_client: fanferro.Client) -> None:
    outbox = InMemoryOutbox()
    fifo = memory_client.queue("outbox-groups.fifo")
    standard = memory_client.queue("outbox-groups")
    a1, b1, s1, a2, b2 = [
        outbox.add(await fifo.prepare({"n": 1}, group_id="a")),
        outbox.add(await fifo.prepare({"n": 1}, group_id="b")),
        outbox.add(await standard.prepare({"n": 1}, group_id="a")),
        outbox.add(await fifo.prepare({"n": 2}, group_id="a")),
        outbox.add(await fifo.prepare({"n": 2}, group_id="b")),
    ]
    first = await outbox.claim_batch(1)
    assert [r.id for r in first] == [a1]
    assert first[0].message.fifo_group == "queue:outbox-groups.fifo:a"
    second = await outbox.claim_batch(10)
    assert [r.id for r in second] == [b1, s1, b2], "group a is claimed by the first relay"
    assert await outbox.claim_batch(10) == []
    await outbox.mark_published([a1])
    assert [r.id for r in await outbox.claim_batch(10)] == [a2]


async def test_several_outbox_relays_keep_each_fifo_group_in_order(memory_client: fanferro.Client) -> None:
    # With claims that skipped only claimed rows, a second relay sent the later records of a group
    # while the first still sent the earlier ones: one group of six arrived as [1, 4, 2, 5, 3, 6].
    outbox = InMemoryOutbox()
    fifo = memory_client.queue("outbox-relays.fifo")
    async with memory_client:
        for n in range(6):
            for group in ("a", "b", "c"):
                outbox.add(await fifo.prepare({"group": group, "n": n}, group_id=group))
        relays = [OutboxRelay(memory_client, outbox, batch_size=3) for _ in range(3)]

        async def drive(relay: OutboxRelay) -> None:
            for _ in range(50):
                if not outbox.pending:
                    return
                await relay.run_once()
                await asyncio.sleep(0)

        await asyncio.gather(*(drive(relay) for relay in relays))
        assert outbox.pending == []
        received = [message.json() for message in await drain_queue(memory_client, fifo)]
    for group in ("a", "b", "c"):
        assert [body["n"] for body in received if body["group"] == group] == list(range(6))


async def test_prepared_batches_report_events(memory_client: fanferro.Client) -> None:
    # Records for standard destinations went out with publish_prepared_batch, which emitted no
    # events: a relay failing against a missing queue showed nothing.
    events: list[fanferro.Event] = []
    memory_client.events.subscribe("publish.*", events.append)
    gone = memory_client.queue("prepared-batch-gone")
    worker = memory_client.queue("prepared-batch-worker")
    async with memory_client:
        messages = [await gone.prepare({"n": 0}), await worker.prepare({"n": 1}), await worker.prepare({"n": 2})]
        await gone.delete()
        events.clear()
        batch = await memory_client.publish_prepared_batch(messages)
        await wait_for(lambda: len(events) >= 2)
    assert [result.__class__.__name__ for result in batch.results] == ["BatchFailure", "PublishResult", "PublishResult"]
    failed = [e for e in events if e.kind is fanferro.EventKind.PUBLISH_FAILED]
    succeeded = [e for e in events if e.kind is fanferro.EventKind.PUBLISH_SUCCEEDED]
    assert [(e.fields["destination"], e.fields["message_id"]) for e in failed] == [
        ("prepared-batch-gone", messages[0].message_id)
    ]
    assert failed[0].error is not None
    assert [(e.fields["destination"], e.fields["count"]) for e in succeeded] == [("prepared-batch-worker", 2)]


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

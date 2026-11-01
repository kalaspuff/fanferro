"""Probes fanferro on real AWS, AWS emulators and the in-memory broker, and compares the results.

    uv run python scripts/probe_backends.py run --target memory
    uv run python scripts/probe_backends.py run --target ministack                # starts MiniStack itself
    uv run python scripts/probe_backends.py run --target localstack --endpoint http://localhost:4566
    uv run python scripts/probe_backends.py iam-policy --account 123456789012     # what the AWS run needs
    AWS_PROFILE=sandbox uv run python scripts/probe_backends.py run --target aws --region eu-north-1
    uv run python scripts/probe_backends.py compare probe-aws.json probe-memory.json probe-ministack.json
    AWS_PROFILE=sandbox uv run python scripts/probe_backends.py cleanup --target aws --region eu-north-1

Run it on real AWS with credentials of a test account, limited to the ``iam-policy`` (the
``ffprobe-*`` queues, topics and buckets): a run takes about 15 minutes and a few thousand requests,
well within the free tier. Results for resources of other accounts and regions depend on the
credentials' IAM policy (IAM may refuse before the service looks), so use that policy.

Each scenario drives fanferro's public API the way applications do and records *facts*: outcomes,
exception classes, error kinds, codes, HTTP statuses and messages, what was delivered where and in
which order. Resource names, account ids, the region, ids and URLs are normalised, so the facts of
different targets can be compared: ``compare`` lists every fact that differs from the first file
(real AWS). Facts whose name starts with ``info.`` are shown but never count as differences.

Every resource is named ``ffprobe-<run>-...`` and deleted after its scenario; ``cleanup`` deletes
what an interrupted run left behind (queues, topics and buckets whose names start with
``ffprobe-``). The result files contain no credentials and no account ids.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import io
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import fanferro
from fanferro import AttributeValue, Backoff, OutgoingMessage, Provisioning, QueueAttributes, TopicAttributes

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

PREFIX = "ffprobe-"
TARGETS = ("memory", "ministack", "localstack", "aws")
EMULATOR_ACCOUNT = "000000000000"
FOREIGN_ACCOUNT = "123456789012"
PROBE_VERSION = 1


# -- targets -------------------------------------------------------------------------------------


@dataclass
class Target:
    """Where the probe runs and how to reach it."""

    kind: str
    region: str
    endpoint: str | None = None
    profile: str | None = None
    broker: fanferro.MemoryBroker | None = None

    @property
    def aws_protocol(self) -> bool:
        """Whether the target speaks the AWS protocol (everything but the in-memory broker)."""
        return self.kind != "memory"

    @property
    def real(self) -> bool:
        """Whether the target is real AWS."""
        return self.kind == "aws"

    @property
    def quiet(self) -> float:
        """How long to keep listening after the expected messages arrived (to catch extra ones)."""
        return {"aws": 3.0, "memory": 0.5}.get(self.kind, 1.5)

    @property
    def delivery_timeout(self) -> float:
        """How long to wait for a message that should arrive."""
        return 45.0 if self.real else 15.0

    def client(self, **options: Any) -> fanferro.Client:
        """A client on this target; resources are created on demand unless the options say otherwise."""
        options.setdefault("provisioning", Provisioning.ensure())
        options.setdefault("logging", fanferro.LoggingConfig(enabled=False))
        if self.kind == "memory":
            options.pop("credentials", None)
            options.pop("profile", None)
            return fanferro.Client(broker=self.broker, **options)
        if self.kind == "aws":
            options.setdefault("profile", self.profile)
            return fanferro.Client(region=self.region, **options)
        options.setdefault("credentials", fanferro.Credentials("test", "test"))
        options.setdefault("account_id", EMULATOR_ACCOUNT)
        return fanferro.Client(region=self.region, endpoint_url=self.endpoint, **options)

    def foreign_queue_url(self, name: str) -> str:
        """The URL a queue of another account would have on this target."""
        if self.kind == "memory":
            return f"memory://{self.region}/{FOREIGN_ACCOUNT}/{name}"
        if self.kind == "aws":
            return f"https://sqs.{self.region}.amazonaws.com/{FOREIGN_ACCOUNT}/{name}"
        return f"{self.endpoint}/{FOREIGN_ACCOUNT}/{name}"

    def boto3_client(self, service: str) -> Any:
        """A boto3 client for cleanup (AWS-protocol targets only)."""
        import boto3  # only needed for cleanup

        session: Any = boto3.Session(profile_name=self.profile, region_name=self.region)
        if self.kind == "aws":
            return session.client(service)
        return session.client(
            service,
            region_name=self.region,
            endpoint_url=self.endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",  # the emulators' dummy credentials
        )


# -- normalisation -------------------------------------------------------------------------------

_UUID = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_HEX = re.compile(r"\b[0-9a-fA-F]{32,}\b")
_CALLER = re.compile(r"arn:aws[\w-]*:(?:iam|sts)::\d{12}:[^\s,)\"']+")
_ACCOUNT = re.compile(r"(?<!\d)\d{12}(?!\d)")
_ISO_TIME = re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2}| UTC)?")
_REQUEST_ID = re.compile(r" \(request id [^)]*\)")
_QUERY = re.compile(r"\?[^\s\"']*")
_FANFERRO_ID = re.compile(r"\b[0-9A-HJKMNP-TV-Z]{26}\b")  # fanferro message ids (ULIDs)


@dataclass
class Normalizer:
    """Replaces what differs between runs and targets (names, accounts, ids) with placeholders."""

    run: str
    region: str
    endpoint: str | None = None

    def text(self, value: str) -> str:
        """Normalises one string."""
        out = value.replace(f"{PREFIX}{self.run}", "<run>")
        if self.endpoint:
            out = out.replace(self.endpoint.rstrip("/"), "<endpoint>")
        out = _REQUEST_ID.sub("", out)  # whether there is one is recorded as info.request_id
        out = _CALLER.sub("<caller-arn>", out)
        out = _ACCOUNT.sub("<account>", out)
        out = out.replace(self.region, "<region>")
        out = _QUERY.sub("?<query>", out)
        out = _UUID.sub("<uuid>", out)
        out = _HEX.sub("<hex>", out)
        out = _ISO_TIME.sub("<time>", out)
        return _FANFERRO_ID.sub("<id>", out)

    def value(self, value: Any) -> Any:
        """Normalises a JSON-like value recursively."""
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, bytes):
            return {"bytes": value.hex()}
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {self.text(str(key)): self.value(item) for key, item in value.items()}
        if isinstance(value, list | tuple):
            return [self.value(item) for item in value]
        if value is None or isinstance(value, bool | int | float):
            return value
        return self.text(repr(value))


def describe_error(error: BaseException | None) -> dict[str, Any] | None:
    """What an exception tells an application: class, classification, code, status, message."""
    if error is None:
        return None
    facts: dict[str, Any] = {"type": type(error).__name__, "message": str(error)}
    for name in ("kind", "code", "status", "category", "service", "operation", "retryable"):
        value = getattr(error, name, None)
        if value is not None:
            facts[name] = value
    if getattr(error, "aws_message", None) is not None:
        facts["aws_message"] = error.aws_message  # type: ignore[attr-defined]
    for name in ("size", "limit"):
        if isinstance(getattr(error, name, None), int):
            facts[name] = getattr(error, name)
    report = getattr(error, "report", None)
    if isinstance(report, fanferro.ProvisioningReport):
        facts["report"] = describe_report(report)
    result = getattr(error, "result", None)
    if isinstance(result, fanferro.BatchPublishResult):
        facts["failed"] = [{"index": f.index, "error": describe_error(f.error)} for f in result.failed]
    facts["info.request_id"] = bool(getattr(error, "request_id", None))
    return facts


def messages_as_info(facts: Any) -> Any:
    """``facts`` with every error message turned into an ``info.`` fact (also nested ones)."""
    if isinstance(facts, dict):
        return {
            (f"info.{key}" if key in {"message", "aws_message"} else key): messages_as_info(value)
            for key, value in facts.items()
        }
    return facts


def describe_report(report: fanferro.ProvisioningReport | None) -> dict[str, Any] | None:
    """The outcome of provisioning one resource."""
    if report is None:
        return None
    return {
        "status": report.status,
        "actions": sorted(report.actions),
        "drift": sorted([d.attribute, d.expected, d.actual, d.mutable] for d in report.drift),
        "error": describe_error(report.error),
        "dead_letter": describe_report(report.dead_letter),
    }


def describe_attribute(value: AttributeValue) -> list[Any]:
    """An attribute as ``[data type, value]``."""
    raw = value.value
    return [value.data_type, {"bytes": raw.hex()} if isinstance(raw, bytes) else raw]


def describe_message(message: fanferro.Message[Any]) -> dict[str, Any]:
    """What a handler sees of a message."""
    try:
        body: Any = message.json()
    except (ValueError, fanferro.FanferroError):
        body = {"bytes": message.data.hex()}
    delivery = message.delivery
    sns = delivery.sns if delivery is not None else None
    return {
        "body": body,
        "type": message.type,
        "content_type": message.content_type,
        "envelope": message.envelope,
        "attributes": {name: describe_attribute(value) for name, value in sorted(message.attributes.items())},
        "raw_attribute_names": sorted(message.raw_attributes),
        "receive_count": message.receive_count,
        "group_id": message.group_id,
        "has_sequence_number": message.sequence_number is not None,
        "topic_arn": message.topic_arn,
        "trace_header": message.trace_header,
        "sns": {key: value for key, value in sorted(sns.items()) if key != "timestamp"} if sns else None,
        "sns_has_timestamp": bool(sns and sns.get("timestamp")),
        "has_sent_at": message.sent_at is not None,
        "info.sqs_message_id": message.sqs_message_id,
    }


def label_of(message: fanferro.Message[Any]) -> str:
    """The ``label`` the probe put into a message body."""
    try:
        body = message.json()
    except (ValueError, fanferro.FanferroError):
        return "?"
    return str(body.get("label", "?")) if isinstance(body, dict) else "?"


def delay_bucket(seconds: float) -> str:
    """A coarse duration, comparable between targets."""
    for limit in (1, 2, 5, 10, 20, 40):
        if seconds < limit:
            return f"<{limit}s"
    return ">=40s"


# -- probe context -------------------------------------------------------------------------------


class Probe:
    """The context of one scenario: names, clients, facts and cleanup."""

    def __init__(self, target: Target, run: str, scenario: str, normalizer: Normalizer) -> None:
        self.target = target
        self.run = run
        self.scenario = scenario
        self.normalizer = normalizer
        self.facts: dict[str, Any] = {}
        self.clients: list[fanferro.Client] = []
        self.buckets: set[str] = set()

    def name(self, label: str) -> str:
        """A resource name unique to this run (queue and topic names allow 80 characters)."""
        name = f"{PREFIX}{self.run}-{label}"
        assert len(name) <= 75, name
        return name

    def bucket(self, label: str) -> str:
        """A bucket name unique to this run (lowercase, at most 63 characters)."""
        name = f"{PREFIX}{self.run}-{label}".lower()
        assert len(name) <= 63, name
        self.buckets.add(name)
        return name

    def client(self, **options: Any) -> fanferro.Client:
        """A client on the target, closed (and its resources deleted) after the scenario."""
        client = self.target.client(**options)
        self.clients.append(client)
        return client

    def fact(self, key: str, value: Any) -> None:
        """Records a fact (normalised)."""
        self.facts[key] = self.normalizer.value(value)

    async def outcome(self, key: str, awaitable: Awaitable[Any], *, messages: bool = True) -> Any:
        """Records ``"ok"`` or the error an operation raised; returns its result (``None`` on errors).

        ``messages=False`` keeps the error messages out of the comparison (as ``info.`` facts), for
        errors whose text depends on the machine rather than on the target.
        """
        try:
            result = await awaitable
        except fanferro.FanferroError as error:
            facts = describe_error(error) or {}
            self.fact(key, facts if messages else messages_as_info(facts))
            return None
        self.fact(key, "ok")
        return result

    async def close(self) -> None:
        """Deletes what the scenario's clients created and closes them."""
        for client in self.clients:
            for subscription in client.subscriptions:
                if subscription.resolved:
                    with contextlib.suppress(Exception):
                        await subscription.delete()
            for queue in client.queues.values():
                if queue.resolved:
                    with contextlib.suppress(Exception):
                        await queue.delete()
            for topic in client.topics.values():
                if topic.resolved:
                    with contextlib.suppress(Exception):
                        await topic.delete()
            with contextlib.suppress(Exception):
                await client.close()


async def wait_until(condition: Callable[[], bool], timeout: float, interval: float = 0.1) -> bool:
    """Waits until ``condition()`` is true; returns whether it became true in time."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(interval)
    return True


class Collector:
    """Runs consumers on several queues and keeps what they receive."""

    def __init__(self, probe: Probe, client: fanferro.Client, queues: Iterable[fanferro.Queue], **options: Any) -> None:
        self.probe = probe
        self.messages: dict[str, list[fanferro.Message[Any]]] = {}
        self.consumers: list[fanferro.Consumer] = []
        options.setdefault("wait_time", 1)
        # Stopping lets the last (one second) poll finish, so it cannot capture later messages.
        options.setdefault("shutdown", fanferro.ShutdownPolicy(abort_receives=False))
        for queue in queues:
            received: list[fanferro.Message[Any]] = []
            self.messages[queue.name] = received

            async def handle(message: fanferro.Message[Any], received: list[fanferro.Message[Any]] = received) -> None:
                received.append(message)

            self.consumers.append(client.consumer(queue, handle, name=f"probe-{queue.name}", **options))

    def __getitem__(self, queue: fanferro.Queue) -> list[fanferro.Message[Any]]:
        return self.messages[queue.name]

    def labels(self, queue: fanferro.Queue) -> list[str]:
        """The labels of the messages a queue received, in order."""
        return [label_of(message) for message in self[queue]]

    async def __aenter__(self) -> Self:
        for consumer in self.consumers:
            await consumer.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await asyncio.gather(*(consumer.stop() for consumer in self.consumers), return_exceptions=True)

    async def wait(self, condition: Callable[[], bool], timeout: float | None = None) -> bool:
        """Waits for ``condition``, then keeps listening for the target's quiet period."""
        met = await wait_until(condition, self.probe.target.delivery_timeout if timeout is None else timeout)
        await asyncio.sleep(self.probe.target.quiet)
        return met


async def collect(
    probe: Probe, client: fanferro.Client, queue: fanferro.Queue, expect: int, **options: Any
) -> list[fanferro.Message[Any]]:
    """Receives (and acks) messages until ``expect`` arrived and the queue stayed quiet."""
    async with Collector(probe, client, [queue], **options) as collector:
        await collector.wait(lambda: len(collector[queue]) >= expect)
    return collector[queue]


async def warm_up(probe: Probe, topic: fanferro.Topic, collector: Collector, canary: fanferro.Queue) -> None:
    """Publishes canaries until the topic delivers to ``canary`` (new subscriptions and queue
    policies take a moment on AWS), then lets other subscriptions settle as long again."""
    started = time.monotonic()
    rounds = 0
    while not collector[canary] and time.monotonic() - started < 90:
        rounds += 1
        await topic.publish({"label": f"canary-{rounds}"}, attributes={"canary": "yes"})
        await wait_until(lambda: bool(collector[canary]), 3.0)
    elapsed = time.monotonic() - started
    probe.fact("info.warm_up", {"rounds": rounds, "seconds": delay_bucket(elapsed), "live": bool(collector[canary])})
    if probe.target.real:
        await asyncio.sleep(min(30.0, max(5.0, elapsed)))


def sized_body(target_size: int, attributes_size: int, label: str) -> dict[str, str]:
    """A JSON body that makes the message exactly ``target_size`` bytes with the given attributes."""
    empty = len(json.dumps({"label": label, "blob": ""}, separators=(",", ":")).encode())
    return {"label": label, "blob": "x" * (target_size - attributes_size - empty)}


def attributes_size(attributes: dict[str, AttributeValue]) -> int:
    """How SQS and SNS count message attributes: name, data type and value bytes."""
    total = 0
    for name, value in attributes.items():
        raw = value.value
        total += len(name.encode()) + len(value.data_type.encode())
        total += len(raw) if isinstance(raw, bytes) else len(str(raw).encode())
    return total


async def enter(client: fanferro.Client) -> None:
    """Enters and leaves the client (connects and provisions declared resources)."""
    async with client:
        pass


# -- scenarios -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    """A registered scenario."""

    name: str
    run: Callable[[Probe], Awaitable[None]]
    targets: tuple[str, ...]
    timeout: float
    doc: str


SCENARIOS: dict[str, Scenario] = {}


def scenario(
    name: str, *, targets: tuple[str, ...] = TARGETS, timeout: float = 300.0
) -> Callable[[Callable[[Probe], Awaitable[None]]], Callable[[Probe], Awaitable[None]]]:
    """Registers a scenario."""

    def register(function: Callable[[Probe], Awaitable[None]]) -> Callable[[Probe], Awaitable[None]]:
        doc = (function.__doc__ or "").strip().split("\n\n")[0].replace("\n", " ")
        SCENARIOS[name] = Scenario(name, function, targets, timeout, " ".join(doc.split()))
        return function

    return register


QUEUE_ATTRIBUTES = (
    "VisibilityTimeout",
    "MessageRetentionPeriod",
    "DelaySeconds",
    "MaximumMessageSize",
    "ReceiveMessageWaitTimeSeconds",
    "SqsManagedSseEnabled",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
    "FifoQueue",
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "FifoThroughputLimit",
    "RedrivePolicy",
    "RedriveAllowPolicy",
)
TOPIC_ATTRIBUTES = (
    "DisplayName",
    "FifoTopic",
    "ContentBasedDeduplication",
    "FifoThroughputScope",
    "KmsMasterKeyId",
    "SignatureVersion",
    "TracingConfig",
)
SUBSCRIPTION_ATTRIBUTES = (
    "Protocol",
    "RawMessageDelivery",
    "FilterPolicyScope",
    "PendingConfirmation",
    "ConfirmationWasAuthenticated",
)


def pick(attributes: Any, names: Iterable[str]) -> dict[str, Any]:
    """The listed attributes (JSON documents parsed, so that formatting does not matter)."""
    picked: dict[str, Any] = {}
    for name in names:
        value = attributes.get(name)
        if value is not None and value.startswith("{"):
            with contextlib.suppress(ValueError):
                value = json.loads(value)
        picked[name] = value
    return picked


def describe_policy(policy: str | None) -> Any:
    """The statements of an access policy, without their ids (they are generated)."""
    if not policy:
        return None
    try:
        document = json.loads(policy)
    except ValueError:
        return {"unparsable": policy}
    statements = document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    return sorted(
        (json.dumps({k: v for k, v in s.items() if k != "Sid"}, sort_keys=True) for s in statements),
    )


@scenario("provision.queue_defaults")
async def provision_queue_defaults(probe: Probe) -> None:
    """A queue created without attributes: its default attributes, and a second lookup."""
    client = probe.client()
    queue = client.queue(probe.name("defaults"))
    info = await queue.resolve()
    probe.fact("report", describe_report(queue.last_report))
    probe.fact("attributes", pick(info.attributes, QUEUE_ATTRIBUTES))
    probe.fact("attribute_names", sorted(info.attributes))
    probe.fact("arn", info.arn)
    probe.fact("info.url", info.url)
    observer = probe.client(provisioning=Provisioning.existing())
    again = observer.queue(queue.name)
    await probe.outcome("lookup_again", again.resolve())
    probe.fact("lookup_again_report", describe_report(again.last_report))
    probe.fact("counts", await queue.approximate_count())


@scenario("provision.queue_attributes")
async def provision_queue_attributes(probe: Probe) -> None:
    """A queue with every typed attribute and tags; drift checks; an update."""
    attributes = QueueAttributes(
        visibility_timeout=45,
        message_retention=3600,
        delay=1,
        receive_wait_time=2,
        max_message_size=4096,
        encryption=fanferro.SqsManagedEncryption(),
        redrive_allow_policy={"redrivePermission": "allowAll"},
    )
    client = probe.client()
    queue = client.queue(probe.name("attrs"), attributes=attributes, tags={"team": "probe", "purpose": "parity"})
    info = await queue.resolve()
    probe.fact("report", describe_report(queue.last_report))
    probe.fact("attributes", pick(info.attributes, QUEUE_ATTRIBUTES))
    observer = probe.client(provisioning=Provisioning.existing())
    same = observer.queue(queue.name, attributes=attributes, tags={"team": "probe", "purpose": "parity"})
    await probe.outcome("verify_same", same.resolve())
    probe.fact("verify_same_report", describe_report(same.last_report))
    changed_attributes = dataclasses.replace(attributes, visibility_timeout=60)
    drifted = probe.client(provisioning=Provisioning.existing(on_drift="raise"))
    changed = drifted.queue(queue.name, attributes=changed_attributes, tags={"team": "other"})
    await probe.outcome("verify_changed", changed.resolve())
    updater = probe.client()
    update = updater.queue(queue.name, attributes=changed_attributes, tags={"team": "other"})
    updated = await probe.outcome("update", update.resolve())
    probe.fact("update_report", describe_report(update.last_report))
    if updated is not None:
        probe.fact("updated_visibility", updated.attributes.get("VisibilityTimeout"))


@scenario("provision.invalid_queue_attributes")
async def provision_invalid_queue_attributes(probe: Probe) -> None:
    """Attribute values SQS refuses: out of range, contradicting FIFO settings, a bad redrive policy."""
    account = await probe.client().account()
    missing_dlq = f"arn:aws:sqs:{probe.target.region}:{account}:{probe.name('nope')}"
    cases: dict[str, tuple[str, QueueAttributes | dict[str, str]]] = {
        "visibility_over_12h": ("vis", QueueAttributes(visibility_timeout=43_201)),
        "retention_under_minute": ("ret", QueueAttributes(message_retention=59)),
        "max_size_over_1mib": ("size", QueueAttributes(max_message_size=1_048_577)),
        "max_size_under_1kib": ("small", QueueAttributes(max_message_size=1023)),
        "delay_over_15min": ("delay", QueueAttributes(delay=901)),
        "wait_over_20s": ("wait", QueueAttributes(receive_wait_time=21)),
        "fifo_attribute_on_standard": ("cbd", QueueAttributes(content_based_deduplication=True)),
        "per_group_limit_with_queue_scope": (
            "scope.fifo",
            QueueAttributes(deduplication_scope="queue", fifo_throughput_limit="perMessageGroupId"),
        ),
        "redrive_to_missing_queue": (
            "redrive",
            {
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": missing_dlq,
                        "maxReceiveCount": 3,
                    }
                )
            },
        ),
        "unknown_attribute": ("unknown", {"NoSuchAttribute": "1"}),
    }
    for key, (suffix, attributes) in cases.items():
        client = probe.client()
        queue = client.queue(probe.name(f"bad-{suffix}"), attributes=attributes)
        await probe.outcome(key, queue.resolve())


@scenario("provision.fifo_queue")
async def provision_fifo_queue(probe: Probe) -> None:
    """A FIFO queue with high-throughput settings; drift checks."""
    attributes = QueueAttributes(
        content_based_deduplication=True, deduplication_scope="messageGroup", fifo_throughput_limit="perMessageGroupId"
    )
    client = probe.client()
    queue = client.queue(probe.name("fifo.fifo"), attributes=attributes)
    info = await queue.resolve()
    probe.fact("report", describe_report(queue.last_report))
    probe.fact("attributes", pick(info.attributes, QUEUE_ATTRIBUTES))
    probe.fact("fifo", info.fifo)
    observer = probe.client(provisioning=Provisioning.existing())
    same = observer.queue(queue.name, attributes=attributes)
    await probe.outcome("verify_same", same.resolve())
    probe.fact("verify_same_report", describe_report(same.last_report))


@scenario("provision.dead_letter_queue")
async def provision_dead_letter_queue(probe: Probe) -> None:
    """A queue with a dead-letter queue: the redrive policy as SQS returns it; drift checks."""
    client = probe.client()
    dead_letter = fanferro.DeadLetterQueue(probe.name("work-dlq"), max_receive_count=3)
    queue = client.queue(probe.name("work"), dead_letter=dead_letter)
    info = await queue.resolve()
    assert queue.dead_letter_queue is not None
    dlq = await queue.dead_letter_queue.resolve()
    probe.fact("report", describe_report(queue.last_report))
    probe.fact("redrive_policy", info.redrive_policy)
    probe.fact("raw_redrive_policy", info.attributes.get("RedrivePolicy"))
    probe.fact("dlq_attributes", pick(dlq.attributes, QUEUE_ATTRIBUTES))
    observer = probe.client(provisioning=Provisioning.existing())
    same = observer.queue(queue.name, dead_letter=dead_letter)
    await probe.outcome("verify_same", same.resolve())
    probe.fact("verify_same_report", describe_report(same.last_report))
    fifo_client = probe.client()
    fifo_dlq = fanferro.DeadLetterQueue(probe.name("fifo-work-dlq.fifo"), max_receive_count=3)
    fifo = fifo_client.queue(probe.name("fifo-work.fifo"), dead_letter=fifo_dlq)
    fifo_info = await probe.outcome("fifo_with_fifo_dlq", fifo.resolve())
    if fifo_info is not None:
        probe.fact("fifo_redrive_policy", fifo_info.redrive_policy)


@scenario("provision.topics")
async def provision_topics(probe: Probe) -> None:
    """Standard and FIFO topics with attributes and tags; drift checks."""
    client = probe.client()
    attributes = TopicAttributes(display_name="Probe", signature_version=2, tracing="Active")
    topic = client.topic(probe.name("topic"), attributes=attributes, tags={"team": "probe"})
    info = await topic.resolve()
    probe.fact("report", describe_report(topic.last_report))
    probe.fact("attributes", pick(info.attributes, TOPIC_ATTRIBUTES))
    probe.fact("attribute_names", sorted(info.attributes))
    probe.fact("arn", info.arn)
    fifo_attributes = TopicAttributes(content_based_deduplication=True, fifo_throughput_scope="MessageGroup")
    fifo = client.topic(probe.name("topic.fifo"), attributes=fifo_attributes)
    fifo_info = await probe.outcome("fifo", fifo.resolve())
    if fifo_info is not None:
        probe.fact("fifo_attributes", pick(fifo_info.attributes, TOPIC_ATTRIBUTES))
        probe.fact("fifo_attribute_names", sorted(fifo_info.attributes))
    observer = probe.client(provisioning=Provisioning.existing())
    same = observer.topic(topic.name, attributes=attributes, tags={"team": "probe"})
    await probe.outcome("verify_same", same.resolve())
    probe.fact("verify_same_report", describe_report(same.last_report))
    if fifo_info is not None:
        same_fifo = observer.topic(fifo.name, attributes=fifo_attributes)
        await probe.outcome("verify_same_fifo", same_fifo.resolve())
        probe.fact("verify_same_fifo_report", describe_report(same_fifo.last_report))
    updater = probe.client()
    update = updater.topic(topic.name, attributes=dataclasses.replace(attributes, display_name="Changed"))
    await probe.outcome("update", update.resolve())
    probe.fact("update_report", describe_report(update.last_report))
    invalid = probe.client()
    await probe.outcome(
        "fifo_attribute_on_standard",
        invalid.topic(probe.name("cbd-topic"), attributes=TopicAttributes(content_based_deduplication=True)).resolve(),
    )
    await probe.outcome(
        "invalid_signature_version",
        invalid.topic(probe.name("sig-topic"), attributes={"SignatureVersion": "3"}).resolve(),
    )


@scenario("provision.subscriptions")
async def provision_subscriptions(probe: Probe) -> None:
    """Raw and wrapped subscriptions with attribute and body filter policies, the queue policy
    fanferro writes, and drift checks."""
    client = probe.client()
    topic = client.topic(probe.name("subs"))
    raw = client.queue(probe.name("subs-raw"))
    body = client.queue(probe.name("subs-body"))
    attribute_filter = {"color": ["red", {"prefix": "bl"}], "price": [{"numeric": [">", 10]}]}
    body_filter = {"order": {"total": [{"numeric": [">=", 100]}]}}
    raw_sub = client.subscription(topic, raw, filter_policy=attribute_filter)
    body_sub = client.subscription(
        topic, body, raw_message_delivery=False, filter_policy=body_filter, filter_policy_scope="MessageBody"
    )
    await client.provision()
    for key, subscription in (("raw", raw_sub), ("body", body_sub)):
        info = await subscription.resolve()
        probe.fact(f"{key}.report", describe_report(subscription.last_report))
        probe.fact(f"{key}.attributes", pick(info.attributes, SUBSCRIPTION_ATTRIBUTES))
        policy = info.attributes.get("FilterPolicy")
        probe.fact(f"{key}.filter_policy", json.loads(policy) if policy else None)
        probe.fact(f"{key}.arn_prefix", info.arn.rsplit(":", 1)[0])
        probe.fact(f"{key}.attribute_names", sorted(info.attributes))
    queue_info = await raw.resolve(force=True)
    probe.fact("queue_policy", describe_policy(queue_info.attributes.get("Policy")))
    observer = probe.client(provisioning=Provisioning.existing())
    observed_topic = observer.topic(topic.name)
    variants: tuple[tuple[str, fanferro.Queue, dict[str, Any]], ...] = (
        ("raw", raw, {"filter_policy": attribute_filter}),
        (
            "body",
            body,
            {"raw_message_delivery": False, "filter_policy": body_filter, "filter_policy_scope": "MessageBody"},
        ),
    )
    for key, queue, options in variants:
        same = observer.subscription(observed_topic, observer.queue(queue.name), **options)
        await probe.outcome(f"{key}.verify_same", same.resolve())
        probe.fact(f"{key}.verify_same_report", describe_report(same.last_report))
    changer = probe.client()
    changed = changer.subscription(
        changer.topic(topic.name), changer.queue(raw.name), filter_policy={"color": ["green"]}
    )
    await probe.outcome("update_filter", changed.resolve())
    probe.fact("update_filter_report", describe_report(changed.last_report))


@scenario("provision.missing_resources")
async def provision_missing_resources(probe: Probe) -> None:
    """Resources that do not exist, with ``Provisioning.existing()``: what the errors say."""
    client = probe.client(provisioning=Provisioning.existing())
    queue = client.queue(probe.name("missing-queue"))
    await probe.outcome("queue", queue.resolve())
    await probe.outcome("queue_send", queue.send({"label": "x"}))
    topic = client.topic(probe.name("missing-topic"))
    await probe.outcome("topic", topic.resolve())
    await probe.outcome("topic_publish", topic.publish({"label": "x"}))
    creator = probe.client()
    existing_topic = creator.topic(probe.name("present-topic"))
    existing_queue = creator.queue(probe.name("present-queue"))
    await existing_topic.resolve()
    await existing_queue.resolve()
    subscription = client.subscription(client.topic(existing_topic.name), client.queue(existing_queue.name))
    await probe.outcome("subscription", subscription.resolve())
    probe.fact("subscription_report", describe_report(subscription.last_report))
    ignoring = probe.client(provisioning=Provisioning(on_missing="ignore"))
    await probe.outcome("queue_ignored", ignoring.queue(probe.name("missing-queue2")).resolve())


@scenario("provision.foreign_resources")
async def provision_foreign_resources(probe: Probe) -> None:
    """Queues and topics of another account (or region) the credentials cannot use."""
    client = probe.client(provisioning=Provisioning.existing())
    account = await client.account()
    other_region = "us-west-2" if probe.target.region != "us-west-2" else "us-east-1"
    by_owner = client.queue(probe.name("foreign-q"), owner_account_id=FOREIGN_ACCOUNT)
    await probe.outcome("queue_by_owner", by_owner.resolve())
    by_url = client.queue(url=probe.target.foreign_queue_url(probe.name("foreign-u")))
    await probe.outcome("queue_by_url", by_url.resolve())
    await probe.outcome("queue_by_url_send", by_url.send({"label": "x"}))
    assumed = probe.client(provisioning=Provisioning.assume())
    assumed_queue = assumed.queue(url=probe.target.foreign_queue_url(probe.name("foreign-a")))
    await probe.outcome("assumed_queue_send", assumed_queue.send({"label": "x"}))
    foreign_topic = client.topic(arn=f"arn:aws:sns:{probe.target.region}:{FOREIGN_ACCOUNT}:{probe.name('foreign-t')}")
    await probe.outcome("topic_by_arn", foreign_topic.resolve())
    assumed_topic = assumed.topic(arn=f"arn:aws:sns:{probe.target.region}:{FOREIGN_ACCOUNT}:{probe.name('foreign-t')}")
    await probe.outcome("assumed_topic_publish", assumed_topic.publish({"label": "x"}))
    other_region_topic = assumed.topic(arn=f"arn:aws:sns:{other_region}:{account}:{probe.name('elsewhere')}")
    await probe.outcome("assumed_topic_in_other_region_publish", other_region_topic.publish({"label": "x"}))


@scenario("provision.queue_deleted_recently")
async def provision_queue_deleted_recently(probe: Probe) -> None:
    """A queue created again right after it was deleted (SQS refuses that for 60 seconds)."""
    client = probe.client()
    name = probe.name("phoenix")
    queue = client.queue(name)
    await queue.resolve()
    await queue.delete()
    again = probe.client()
    await probe.outcome("create_again", again.queue(name).resolve())


@scenario("provision.kms", timeout=240)
async def provision_kms(probe: Probe) -> None:
    """SSE-KMS with the AWS managed keys: attributes, a round trip, and SNS delivering to such a queue."""
    client = probe.client()
    queue = client.queue(
        probe.name("kms"), attributes=QueueAttributes(encryption=fanferro.Kms("alias/aws/sqs", data_key_reuse=300))
    )
    topic = client.topic(probe.name("kms-topic"), attributes=TopicAttributes(encryption=fanferro.Kms("alias/aws/sns")))
    canary = client.queue(probe.name("kms-canary"))
    client.subscription(topic, queue)
    client.subscription(topic, canary)
    await client.provision()
    probe.fact("queue_attributes", pick((await queue.resolve()).attributes, QUEUE_ATTRIBUTES))
    probe.fact("topic_attributes", pick((await topic.resolve()).attributes, TOPIC_ATTRIBUTES))
    await queue.send({"label": "direct"})
    async with Collector(probe, client, [queue, canary]) as collector:
        await warm_up(probe, topic, collector, canary)
        await topic.publish({"label": "via-topic"})
        await collector.wait(lambda: "via-topic" in collector.labels(canary), timeout=30)
        await wait_until(lambda: "via-topic" in collector.labels(queue), probe.target.delivery_timeout / 2)
    probe.fact("kms_queue_received", sorted(set(collector.labels(queue)) - {"?"} - _canaries(collector, queue)))
    probe.fact("canary_received_via_topic", "via-topic" in collector.labels(canary))


def _canaries(collector: Collector, queue: fanferro.Queue) -> set[str]:
    return {label for label in collector.labels(queue) if label.startswith("canary-")}


@scenario("provision.payload_buckets")
async def provision_payload_buckets(probe: Probe) -> None:
    """Payload buckets: created on demand, found again, and missing ones."""
    bucket = probe.bucket("bucket")
    client = probe.client(large_payloads=fanferro.LargePayloads(bucket, create_bucket=True))
    await probe.outcome("create", enter(client))
    again = probe.client(large_payloads=fanferro.LargePayloads(bucket, create_bucket=True))
    await probe.outcome("create_again", enter(again))
    existing = probe.client(large_payloads=fanferro.LargePayloads(bucket))
    await probe.outcome("existing", enter(existing))
    missing = probe.client(large_payloads=fanferro.LargePayloads(probe.bucket("nobucket"), always=True))
    await probe.outcome("missing_enter", enter(missing))
    queue = missing.queue(probe.name("nobucket-q"))
    await probe.outcome("missing_send", queue.send({"label": "x"}))


# -- SQS -------------------------------------------------------------------------------------------


@scenario("sqs.roundtrip")
async def sqs_roundtrip(probe: Probe) -> None:
    """A message with attributes of every type, sent and received with MD5 verification."""
    client = probe.client(verify_md5=True)
    queue = client.queue(probe.name("roundtrip"))
    attributes: dict[str, Any] = {
        "text": "hello",
        "unicode": "räksmörgås ✓ 🎉",
        "int": 42,
        "decimal": Decimal("1.50"),
        "float": 2.5,
        "big": AttributeValue.number("12345678901234567890"),
        "bytes": b"\x00\xff\x10",
        "custom": AttributeValue("String.custom", "x"),
        "number_custom": AttributeValue("Number.int", "7"),
    }
    result = await queue.send({"label": "roundtrip", "nested": {"a": [1, 2.5, None, True]}}, attributes=attributes)
    probe.fact(
        "send", {"has_transport_id": bool(result.transport_message_id), "sequence_number": result.sequence_number}
    )
    probe.fact("info.send_size", result.size)
    messages = await collect(probe, client, queue, 1)
    probe.fact("received", [describe_message(message) for message in messages])
    probe.fact("transport_id_matches", [m.sqs_message_id == result.transport_message_id for m in messages])
    cloudevents = probe.client(verify_md5=True, envelope=fanferro.envelopes.CloudEvents(), source="urn:probe")
    ce_queue = cloudevents.queue(probe.name("roundtrip-ce"))
    await ce_queue.send({"label": "ce"}, subject="order/1", headers={"version": 2}, attributes={"tenant": "acme"})
    ce_messages = await collect(probe, cloudevents, ce_queue, 1)
    probe.fact(
        "cloudevents",
        [{**describe_message(m), "source": m.source, "subject": m.subject, "headers": m.headers} for m in ce_messages],
    )


@scenario("sqs.bodies")
async def sqs_bodies(probe: Probe) -> None:
    """Bodies of every kind: text, binary, characters SQS refuses, an empty string, JSON scalars."""
    client = probe.client(verify_md5=True)
    queue = client.queue(probe.name("bodies"))
    bodies: dict[str, Any] = {
        "text": "plain text",
        "emoji": "emoji 🎉 and ü",
        "nul": "nul \x00 char",
        "noncharacter": "bad ￾ char",
        "empty": "",
        "binary": b"\x00\x01\xfe\xff",
        "empty_bytes": b"",
        "json_null": None,
        "json_number": 42,
    }
    sent: dict[str, str] = {}
    for key, body in bodies.items():
        result = await probe.outcome(f"send.{key}", queue.send(body, id=f"body-{key}"))
        if result is not None:
            sent[result.message_id] = key
    messages = await collect(probe, client, queue, len(sent))
    received = {}
    for message in messages:
        received[sent.get(message.id, message.id)] = {
            "data": message.data.hex(),
            "content_type": message.content_type,
            "encoding": message.raw_attributes.get("fanferro.encoding").value  # type: ignore[union-attr]
            if "fanferro.encoding" in message.raw_attributes
            else None,
        }
    probe.fact("received", dict(sorted(received.items())))


@scenario("sqs.delay")
async def sqs_delay(probe: Probe) -> None:
    """A per-message delay and a queue delay: when the messages become visible."""
    client = probe.client()
    queue = client.queue(probe.name("delay"))
    delayed_queue = client.queue(probe.name("delay-q"), attributes=QueueAttributes(delay=3))
    await queue.resolve()
    await delayed_queue.resolve()
    started = time.monotonic()
    await queue.send({"label": "per-message"}, delay=3)
    await delayed_queue.send({"label": "queue-delay"})
    seen_delayed = False  # the counts are approximate: poll them
    for _ in range(10):
        counts = await queue.approximate_count()
        if counts["delayed"] == 1:
            seen_delayed = True
            break
        await asyncio.sleep(0.2)
    probe.fact("info.delayed_count_seen", seen_delayed)
    async with Collector(probe, client, [queue, delayed_queue]) as collector:
        arrived: dict[str, float] = {}

        def check() -> bool:
            for q in (queue, delayed_queue):
                if collector[q] and q.name not in arrived:
                    arrived[q.name] = time.monotonic() - started
            return len(arrived) == 2

        await collector.wait(check, timeout=20)
    probe.fact("per_message_delay", delay_bucket(arrived.get(queue.name, 99)) if queue.name in arrived else None)
    probe.fact("per_message_delay_at_least_2_5s", arrived.get(queue.name, 0) >= 2.5)
    probe.fact("queue_delay_at_least_2_5s", arrived.get(delayed_queue.name, 0) >= 2.5)
    fifo = client.queue(probe.name("delay.fifo"))
    await probe.outcome("fifo_per_message_delay", fifo.send({"label": "x"}, group_id="g", delay=2))
    await probe.outcome("delay_over_15min", queue.send({"label": "x"}, delay=901))


@scenario("sqs.batches")
async def sqs_batches(probe: Probe) -> None:
    """Batches of 25 messages (three calls) and a batch with an invalid entry."""
    client = probe.client(verify_md5=True)
    queue = client.queue(probe.name("batch"))
    result = await queue.send_batch([{"label": f"m{i}"} for i in range(25)], attributes={"batch": "yes", "n": 1})
    probe.fact("result", {"ok": result.ok, "successful": len(result.successful), "failed": len(result.failed)})
    messages = await collect(probe, client, queue, 25)
    probe.fact("received", sorted(label_of(m) for m in messages))
    probe.fact("attributes", sorted({json.dumps(describe_message(m)["attributes"]) for m in messages}))
    mixed = [OutgoingMessage({"label": "good"}), OutgoingMessage({"label": "bad"}, attributes={"bad name": "x"})]
    await probe.outcome("invalid_entry", queue.send_batch(mixed))
    fifo = client.queue(probe.name("batch.fifo"))
    fifo_result = await fifo.send_batch([OutgoingMessage({"label": f"f{i}"}, group_id="g") for i in range(12)])
    probe.fact(
        "fifo_result",
        {"ok": fifo_result.ok, "with_sequence": sum(bool(r.sequence_number) for r in fifo_result.successful)},
    )
    fifo_messages = await collect(probe, client, fifo, 12)
    probe.fact("fifo_received", [label_of(m) for m in fifo_messages])


@scenario("sqs.size_limits", timeout=240)
async def sqs_size_limits(probe: Probe) -> None:
    """Messages exactly at and one byte over the queue limits (a 1 KiB queue and the default)."""
    client = probe.client()
    small = client.queue(probe.name("size-1k"), attributes=QueueAttributes(max_message_size=1024))
    default = client.queue(probe.name("size-default"))
    info = await default.resolve()
    probe.fact("default_limit", info.max_message_size)
    for key, queue, limit in (("1k", small, 1024), ("default", default, info.max_message_size or 1_048_576)):
        id_attributes = {"fanferro.id": AttributeValue.string(f"size-{key}")}
        overhead = attributes_size(id_attributes)
        at_limit = await probe.outcome(
            f"{key}.at_limit", queue.send(sized_body(limit, overhead, f"{key}-at"), id=f"size-{key}")
        )
        if at_limit is not None:
            probe.fact(f"{key}.at_limit_size", at_limit.size)
        await probe.outcome(
            f"{key}.over_limit", queue.send(sized_body(limit + 1, overhead, f"{key}-over"), id=f"size-{key}")
        )
        messages = await collect(probe, client, queue, 1 if at_limit else 0)
        probe.fact(f"{key}.received", [(label_of(m), len(m.data)) for m in messages])
    raw_check = client.queue(probe.name("size-attr"), attributes=QueueAttributes(max_message_size=1024))
    attributes = {f"a{i}": "v" * 50 for i in range(9)}
    await probe.outcome(
        "attributes_count_towards_limit", raw_check.send(sized_body(700, 0, "attrs"), attributes=attributes)
    )


@scenario("sqs.fifo")
async def sqs_fifo(probe: Probe) -> None:
    """FIFO order per group, deduplication of a resent message, and group blocking across consumers."""
    client = probe.client()
    queue = client.queue(probe.name("order.fifo"))
    results = [await queue.send({"label": f"m{i}"}, group_id="ab"[i % 2], id=f"fifo-{i}") for i in range(6)]
    duplicate = await queue.send({"label": "m0-again"}, group_id="a", id="fifo-0")
    probe.fact(
        "sequence_numbers_increase",
        [int(r.sequence_number or 0) for r in results] == sorted(int(r.sequence_number or 0) for r in results),
    )
    probe.fact("duplicate_same_transport_id", duplicate.transport_message_id == results[0].transport_message_id)
    probe.fact("duplicate_same_sequence_number", duplicate.sequence_number == results[0].sequence_number)
    messages = await collect(probe, client, queue, 6, concurrency=4)
    by_group: dict[str, list[str]] = {}
    for message in messages:
        by_group.setdefault(message.group_id or "", []).append(label_of(message))
    probe.fact("received_by_group", dict(sorted(by_group.items())))
    probe.fact(
        "received_attributes", sorted({json.dumps(describe_message(m)["raw_attribute_names"]) for m in messages})
    )
    await probe.outcome("missing_group_id", queue.send({"label": "x"}))
    await probe.outcome("long_group_id", queue.send({"label": "x"}, group_id="g" * 129))
    await probe.outcome("invalid_group_id", queue.send({"label": "x"}, group_id="space is invalid"))
    await probe.outcome("long_deduplication_id", queue.send({"label": "x"}, group_id="g", deduplication_id="d" * 129))

    blocking = client.queue(probe.name("block.fifo"))
    await blocking.send_batch([OutgoingMessage({"label": f"b{i}"}, group_id="g") for i in range(2)])
    held: list[fanferro.Message[Any]] = []

    async def hold(message: fanferro.Message[Any]) -> None:
        held.append(message)

    holder_client = probe.client()
    holder = holder_client.consumer(
        holder_client.queue(blocking.name),
        hold,
        wait_time=1,
        auto_ack=False,
        concurrency=1,
        prefetch=0,
        batch_size=1,  # SQS hands out several messages of a group in one receive
        name="probe-holder",
        shutdown=fanferro.ShutdownPolicy(abort_receives=False),
    )
    await holder.start()
    try:
        await wait_until(lambda: bool(held), probe.target.delivery_timeout)
        holder.pause()  # it must not take the group's next message once the first one is acked
        other_client = probe.client()
        async with Collector(probe, other_client, [other_client.queue(blocking.name)]) as other:
            other_queue = other_client.queue(blocking.name)
            await asyncio.sleep(4)
            probe.fact("blocked_while_in_flight", other.labels(other_queue))
            if held:
                await held[0].ack()
            await other.wait(lambda: bool(other[other_queue]), timeout=15)
            probe.fact("after_ack", other.labels(other_queue))
    finally:
        await holder.stop()
    probe.fact("held", [label_of(m) for m in held])


@scenario("sqs.fifo_deduplication")
async def sqs_fifo_deduplication(probe: Probe) -> None:
    """Deduplication scopes (queue and message group) and content-based deduplication."""
    client = probe.client()
    for scope in ("queue", "messageGroup"):
        queue = client.queue(
            probe.name(f"dedup-{scope.lower()}.fifo"),
            attributes=QueueAttributes(deduplication_scope=scope, fifo_throughput_limit="perQueue"),  # type: ignore[arg-type]
        )
        for group in ("a", "a", "b"):
            await queue.send({"label": group}, group_id=group, deduplication_id="same")
        messages = await collect(probe, client, queue, 1)
        probe.fact(f"{scope}.received", sorted(label_of(m) for m in messages))
    content = client.queue(
        probe.name("dedup-content.fifo"), attributes=QueueAttributes(content_based_deduplication=True)
    )
    for i in range(2):
        await content.send({"label": "same body"}, group_id="g", id=f"content-{i}")
    messages = await collect(probe, client, content, 1)
    probe.fact("content_based.received", [m.id for m in messages])


@scenario("sqs.retries")
async def sqs_retries(probe: Probe) -> None:
    """A failed attempt retried by visibility after two seconds: receive count, attempt, timing."""
    client = probe.client()
    queue = client.queue(probe.name("retry"))
    attempts: list[tuple[int, int, float]] = []
    started = time.monotonic()
    retry = fanferro.RetryPolicy(max_attempts=3, backoff=Backoff.constant(2))

    async def handle(message: fanferro.Message[Any]) -> None:
        attempts.append((message.attempt, message.receive_count, time.monotonic() - started))
        if message.attempt == 1:
            raise ConnectionError("downstream unavailable")

    consumer = client.consumer(queue, handle, wait_time=1, retry=retry)
    await queue.send({"label": "retry"})
    await consumer.start()
    try:
        await wait_until(lambda: len(attempts) >= 2, probe.target.delivery_timeout)
        await asyncio.sleep(probe.target.quiet)
    finally:
        await consumer.stop()
    probe.fact("attempts", [[attempt, count] for attempt, count, _ in attempts])
    probe.fact("retry_waited_at_least_1_5s", len(attempts) >= 2 and attempts[1][2] - attempts[0][2] >= 1.5)
    probe.fact("counts", await queue.approximate_count())


@scenario("sqs.handler_actions")
async def sqs_handler_actions(probe: Probe) -> None:
    """Reject (dead-letter copy with metadata), Discard, Requeue (a new copy) and Retry."""
    client = probe.client()
    queue = client.queue(
        probe.name("actions"), dead_letter=fanferro.DeadLetterQueue(probe.name("actions-dlq"), max_receive_count=10)
    )
    seen: dict[str, list[list[int]]] = {}
    retry = fanferro.RetryPolicy(max_attempts=5, backoff=Backoff.constant(0))

    async def handle(message: fanferro.Message[Any]) -> None:
        label = label_of(message)
        seen.setdefault(label, []).append([message.attempt, message.receive_count])
        if label == "reject":
            raise fanferro.Reject("invalid order")
        if label == "discard":
            raise fanferro.Discard("test order")
        if label == "requeue" and message.attempt == 1:
            raise fanferro.Requeue("again", delay=0)
        if label == "retry" and message.attempt == 1:
            raise fanferro.Retry("not yet", delay=0)

    consumer = client.consumer(queue, handle, wait_time=1, retry=retry)
    for label in ("reject", "discard", "requeue", "retry"):
        await queue.send({"label": label}, attributes={"tenant": "acme"})
    await consumer.start()
    try:
        await wait_until(
            lambda: len(seen.get("requeue", [])) >= 2 and len(seen.get("retry", [])) >= 2,
            probe.target.delivery_timeout,
        )
        await asyncio.sleep(probe.target.quiet)
    finally:
        await consumer.stop()
    probe.fact("attempts", dict(sorted(seen.items())))
    assert queue.dead_letter_queue is not None
    dead = await collect(probe, client, queue.dead_letter_queue, 1)
    probe.fact(
        "dead_lettered",
        [
            {
                "label": label_of(m),
                "info": {k: v for k, v in (m.dead_letter_info or {}).items() if k not in {"time", "dead_lettered_at"}},
                "attributes": describe_message(m)["attributes"],
            }
            for m in dead
        ],
    )
    probe.fact("counts", await queue.approximate_count())


@scenario("sqs.native_redrive", timeout=240)
async def sqs_native_redrive(probe: Probe) -> None:
    """SQS moving a message to the dead-letter queue after ``maxReceiveCount`` receives."""
    client = probe.client()
    queue = client.queue(
        probe.name("redrive"), dead_letter=fanferro.DeadLetterQueue(probe.name("redrive-dlq"), max_receive_count=2)
    )
    attempts: list[int] = []
    retry = fanferro.RetryPolicy(max_attempts=None, backoff=Backoff.immediate())

    async def fail(message: fanferro.Message[Any]) -> None:
        attempts.append(message.receive_count)
        raise ConnectionError("always failing")

    consumer = client.consumer(queue, fail, wait_time=1, retry=retry)
    sent = await queue.send({"label": "poison"}, attributes={"tenant": "acme"})
    await consumer.start()
    try:
        await wait_until(lambda: len(attempts) >= 2, probe.target.delivery_timeout)
        await asyncio.sleep(max(3.0, probe.target.quiet))  # more receives move it to the dead-letter queue
    finally:
        await consumer.stop()
    probe.fact("source_receive_counts", attempts)
    assert queue.dead_letter_queue is not None
    dead = await collect(probe, client, queue.dead_letter_queue, 1)
    probe.fact(
        "dead_lettered",
        [
            {
                "label": label_of(m),
                "receive_count": m.receive_count,
                "same_message_id": m.id == sent.message_id,
                "same_sqs_message_id": m.sqs_message_id == sent.transport_message_id,
                "source_arn": m.delivery.dead_letter_source_arn if m.delivery is not None else None,
                "attributes": describe_message(m)["attributes"],
            }
            for m in dead
        ],
    )


async def dead_letter_two(probe: Probe, client: fanferro.Client, label: str) -> fanferro.Queue:
    """A queue with a dead-letter queue holding two messages fanferro dead-lettered: ``requeued``
    (after two requeued attempts) and ``rejected`` (at its first attempt)."""
    queue = client.queue(
        probe.name(label), dead_letter=fanferro.DeadLetterQueue(probe.name(f"{label}-dlq"), max_receive_count=10)
    )
    dead: list[fanferro.Event] = []
    client.on("message.dead_lettered", dead.append)
    retry = fanferro.RetryPolicy(max_attempts=2, backoff=Backoff.constant(0), strategy="requeue")

    async def fail(message: fanferro.Message[Any]) -> None:
        if label_of(message) == "rejected":
            raise fanferro.Reject("invalid order")
        raise ConnectionError("downstream unavailable")

    polite = fanferro.ShutdownPolicy(abort_receives=False)
    consumer = client.consumer(queue, fail, wait_time=1, retry=retry, shutdown=polite)
    for name in ("requeued", "rejected"):
        await queue.send({"label": name}, attributes={"tenant": "acme"})
    await consumer.start()
    try:
        await wait_until(lambda: len(dead) >= 2, probe.target.delivery_timeout)
    finally:
        await consumer.stop()
    return queue


@scenario("sqs.dead_letter_redrive", timeout=240)
async def sqs_dead_letter_redrive(probe: Probe) -> None:
    """``queue.peek()`` and ``queue.redrive()`` of messages fanferro dead-lettered: back to their
    source without fanferro's metadata, processed at their first attempt again."""
    client = probe.client()
    queue = await dead_letter_two(probe, client, "dlqtools")
    dlq = queue.dead_letter_queue
    assert dlq is not None
    peeked = await dlq.peek()
    probe.fact(
        "peeked",
        sorted(
            (
                {
                    "label": json.loads(message.body).get("label"),
                    "reason": (message.dead_letter_info or {}).get("reason"),
                    "source_is_queue": message.source == queue.arn,
                    "requeued_attempts": message.requeued_attempts,
                    "raw_attribute_names": sorted(message.raw_attributes),
                }
                for message in peeked
            ),
            key=lambda fact: str(fact["label"]),
        ),
    )
    probe.fact("peeked_again", len(await dlq.peek()))
    result = await dlq.redrive()
    probe.fact(
        "redrive",
        {
            "moved": result.moved,
            "destinations": result.destinations,
            "not_moved": [describe_error(failure.error) for failure in result.not_moved],
            "not_deleted": [describe_error(failure.error) for failure in result.not_deleted],
        },
    )
    redriven = await collect(probe, client, queue, 2)
    probe.fact(
        "redriven",
        sorted(
            (
                {"label": label_of(m), "attempt": m.attempt, "raw_attribute_names": sorted(m.raw_attributes)}
                for m in redriven
            ),
            key=lambda fact: str(fact["label"]),
        ),
    )
    probe.fact("dead_letter_queue_counts", await dlq.approximate_count())


@scenario("sqs.message_move_task", targets=("ministack", "localstack", "aws"), timeout=300)
async def sqs_message_move_task(probe: Probe) -> None:
    """SQS's own redrive (``StartMessageMoveTask``) of messages fanferro forwarded to a dead-letter
    queue, which SQS's redrive policy did not move there: to their source, and to a named queue."""
    client = probe.client()
    queue = await dead_letter_two(probe, client, "movetask")
    dlq = queue.dead_letter_queue
    assert dlq is not None
    sqs = probe.target.boto3_client("sqs")

    async def move(key: str, destination: str | None) -> None:
        arguments = {"SourceArn": dlq.arn} | ({"DestinationArn": destination} if destination else {})
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            await asyncio.to_thread(sqs.start_message_move_task, **arguments)
        except (BotoCoreError, ClientError) as error:  # also an emulator without the operation
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            probe.fact(f"{key}.start", {"error": type(error).__name__, "code": code, "info.message": str(error)})
            return
        probe.fact(f"{key}.start", "ok")
        status: dict[str, Any] = {}

        def finished() -> bool:
            tasks = sqs.list_message_move_tasks(SourceArn=dlq.arn, MaxResults=1).get("Results", [])
            status.update(tasks[0] if tasks else {})
            return status.get("Status") not in {"RUNNING", "CANCELLING", None}

        deadline = time.monotonic() + probe.target.delivery_timeout
        while not await asyncio.to_thread(finished) and time.monotonic() < deadline:  # noqa: ASYNC110 - polls AWS
            await asyncio.sleep(1)
        probe.fact(
            f"{key}.task",
            {
                "status": status.get("Status"),
                "moved": status.get("ApproximateNumberOfMessagesMoved"),
                "failure_reason": status.get("FailureReason"),
            },
        )

    await move("to_source", None)
    moved = await collect(probe, client, queue, 2)
    probe.fact(
        "to_source.received",
        sorted(
            (
                {"label": label_of(m), "attempt": m.attempt, "raw_attribute_names": sorted(m.raw_attributes)}
                for m in moved
            ),
            key=lambda fact: str(fact["label"]),
        ),
    )
    probe.fact("to_source.dead_letter_queue_counts", await dlq.approximate_count())
    if moved:
        return
    await move("to_queue", queue.arn)
    moved = await collect(probe, client, queue, 2)
    probe.fact("to_queue.received", sorted(label_of(m) for m in moved))


@scenario("sqs.late_settlement")
async def sqs_late_settlement(probe: Probe) -> None:
    """Settling with the receipt handle of an earlier receive (the message became visible again and
    was received a second time), on a standard and a FIFO queue."""
    for kind, suffix in (("standard", ""), ("fifo", ".fifo")):
        client = probe.client()
        queue = client.queue(probe.name(f"late{suffix}"), attributes=QueueAttributes(visibility_timeout=3))
        held: list[fanferro.Message[Any]] = []

        async def hold(message: fanferro.Message[Any], held: list[fanferro.Message[Any]] = held) -> None:
            held.append(message)

        consumer = client.consumer(
            queue,
            hold,
            wait_time=1,
            auto_ack=False,
            heartbeat=False,
            visibility_timeout=3,
            shutdown=fanferro.ShutdownPolicy(abort_receives=False, release_unfinished=False),
        )
        await queue.send({"label": kind}, group_id="g" if suffix else None)
        await consumer.start()
        try:
            await wait_until(lambda held=held: len(held) >= 2, probe.target.delivery_timeout)
            probe.fact(f"{kind}.receive_counts", [m.receive_count for m in held])
            if len(held) >= 2:
                first, second = held[0], held[1]
                await probe.outcome(f"{kind}.extend_old_handle", first.extend(10))
                await probe.outcome(f"{kind}.ack_old_handle", first.ack())
                probe.fact(f"{kind}.counts_after_old_ack", await queue.approximate_count())
                await probe.outcome(f"{kind}.ack_current_handle", second.ack())
            await asyncio.sleep(probe.target.quiet)
            probe.fact(f"{kind}.counts_after", await queue.approximate_count())
            probe.fact(f"{kind}.receives", len(held))
        finally:
            await consumer.stop()


@scenario("sqs.visibility_limits")
async def sqs_visibility_limits(probe: Probe) -> None:
    """Extending a message's visibility up to and past the 12 hour limit."""
    client = probe.client()
    queue = client.queue(probe.name("extend"))
    done = asyncio.Event()

    async def handle(message: fanferro.Message[Any]) -> None:
        for seconds in (43_100, 43_200, 43_201):
            await probe.outcome(f"extend_{seconds}", message.extend(seconds))
        await probe.outcome("release", message.release())
        done.set()

    consumer = client.consumer(queue, handle, wait_time=1, auto_ack=False)
    await queue.send({"label": "extend"})
    await consumer.start()
    try:
        await asyncio.wait_for(done.wait(), probe.target.delivery_timeout)
    finally:
        await consumer.stop()


@scenario("sqs.purge")
async def sqs_purge(probe: Probe) -> None:
    """Purging a queue, and purging it again right away (SQS allows one purge per minute)."""
    client = probe.client()
    queue = client.queue(probe.name("purge"))
    await queue.send_batch([{"label": f"p{i}"} for i in range(3)])
    await probe.outcome("purge", queue.purge())
    await probe.outcome("purge_again", queue.purge())
    emptied = False
    for _ in range(20):
        if (await queue.approximate_count())["visible"] == 0:
            emptied = True
            break
        await asyncio.sleep(0.5)
    probe.fact("emptied_within_10s", emptied)


@scenario("sqs.deleted_queue")
async def sqs_deleted_queue(probe: Probe) -> None:
    """Using a queue after it was deleted: sends, counts, consumers, with and without provisioning."""
    client = probe.client(provisioning=Provisioning.existing())
    creator = probe.client()
    name = probe.name("deleted")
    await creator.queue(name).resolve()
    queue = client.queue(name)
    await queue.resolve()
    await queue.delete()
    await probe.outcome("send", queue.send({"label": "x"}))
    await probe.outcome("send_batch", queue.send_batch([{"label": "x"}]))
    await probe.outcome("approximate_count", queue.approximate_count())
    await probe.outcome("purge", queue.purge())
    await probe.outcome("delete_again", queue.delete())

    async def handle(message: fanferro.Message[Any]) -> None:
        pass

    consumer = client.consumer(queue, handle, wait_time=1)
    await probe.outcome("consumer_start", consumer.start())
    if consumer.state == "running":
        await consumer.stop()
    recreating = probe.client()
    await probe.outcome("send_with_ensure", recreating.queue(name).send({"label": "x"}))


@scenario("sqs.consumer_stop", timeout=180)
async def sqs_consumer_stop(probe: Probe) -> None:
    """Stopping a consumer during a 20 second long poll, then sending: when does the message arrive?"""
    client = probe.client()
    queue = client.queue(probe.name("stop"), attributes=QueueAttributes(visibility_timeout=8))

    async def handle(message: fanferro.Message[Any]) -> None:
        pass

    consumer = client.consumer(queue, handle, wait_time=20)
    await consumer.start()
    await asyncio.sleep(2)
    started = time.monotonic()
    await consumer.stop()
    probe.fact("stop_took", delay_bucket(time.monotonic() - started))
    sent_at = time.monotonic()
    await queue.send({"label": "after-stop"})
    messages = await collect(probe, client, queue, 1)
    arrived = time.monotonic() - sent_at - probe.target.quiet
    probe.fact("received", [label_of(m) for m in messages])
    probe.fact("receive_counts", [m.receive_count for m in messages])
    probe.fact("info.arrived_after", delay_bucket(arrived))
    probe.fact("arrived_within_visibility_timeout", arrived < 7)


# -- SNS -------------------------------------------------------------------------------------------


@scenario("sns.fanout")
async def sns_fanout(probe: Probe) -> None:
    """A topic delivering to a raw and a wrapped subscription: bodies, attributes, SNS metadata."""
    client = probe.client(verify_md5=True)
    topic = client.topic(probe.name("fanout"))
    raw = client.queue(probe.name("fanout-raw"))
    wrapped = client.queue(probe.name("fanout-wrapped"))
    client.subscription(topic, raw)
    client.subscription(topic, wrapped, raw_message_delivery=False)
    await client.provision()
    early = await probe.outcome("publish_right_after_provisioning", topic.publish({"label": "early"}))
    async with Collector(probe, client, [raw, wrapped]) as collector:
        if early is not None:
            await wait_until(lambda: bool(collector[raw]) and bool(collector[wrapped]), 10)
        probe.fact("info.early_delivered", {"raw": collector.labels(raw), "wrapped": collector.labels(wrapped)})
        await warm_up(probe, topic, collector, raw)
        result = await topic.publish(
            {"label": "main"},
            type="probe.fanout.v1",
            attributes={"region": "eu", "n": 3, "bin": b"\x01\x02"},
            sns_subject="Probe subject",
        )
        probe.fact(
            "publish",
            {"has_transport_id": bool(result.transport_message_id), "sequence_number": result.sequence_number},
        )
        await collector.wait(lambda: "main" in collector.labels(raw) and "main" in collector.labels(wrapped))
    for key, queue in (("raw", raw), ("wrapped", wrapped)):
        main = [m for m in collector[queue] if label_of(m) == "main"]
        probe.fact(f"{key}.received", [describe_message(m) for m in main])
        if main and main[0].delivery is not None and main[0].delivery.sns is not None:
            probe.fact(
                f"{key}.sns_message_id_matches", main[0].delivery.sns["message_id"] == result.transport_message_id
            )
        probe.fact(f"{key}.message_id_matches", [m.id == result.message_id for m in main])


@scenario("sns.filter_policies", timeout=420)
async def sns_filter_policies(probe: Probe) -> None:
    """Which messages SNS delivers for each filter policy operator (attributes and body)."""
    attribute_policies: dict[str, dict[str, Any]] = {
        "exact": {"color": ["red"]},
        "prefix": {"color": [{"prefix": "bl"}]},
        "suffix": {"color": [{"suffix": "ue"}]},
        "ignore_case": {"color": [{"equals-ignore-case": "red"}]},
        "anything_but": {"color": [{"anything-but": ["red", "green"]}]},
        "anything_but_prefix": {"tier": [{"anything-but": {"prefix": "free"}}]},
        "numeric_range": {"price": [{"numeric": [">", 10, "<=", 20]}]},
        "numeric_equals": {"price": [{"numeric": ["=", 15]}]},
        "exact_number": {"price": [15]},
        "exists": {"color": [{"exists": True}]},
        "not_exists": {"color": [{"exists": False}]},
        "wildcard": {"region": [{"wildcard": "eu-*-1"}]},
        "cidr": {"ip": [{"cidr": "10.0.0.0/8"}]},
        "or": {"$or": [{"color": ["blue"]}, {"price": [{"numeric": ["<", 12]}]}]},
        "two_keys": {"color": ["red"], "tier": ["pro"]},
    }
    body_policies: dict[str, dict[str, Any]] = {
        "body_numeric": {"order": {"total": [{"numeric": [">=", 100]}]}},
        "body_ignore_case": {"order": {"status": [{"equals-ignore-case": "paid"}]}},
        "body_array": {"order": {"tags": ["b"]}},
        "body_not_exists": {"order": {"coupon": [{"exists": False}]}},
    }
    red = {"color": "red", "price": 15, "region": "eu-west-1", "ip": "10.1.2.3", "tier": "pro"}
    messages: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
        "red": (red, {"total": 150, "status": "PAID", "tags": ["a", "b"]}),
        "blue": (
            {"color": "blue", "price": 25, "region": "us-east-1", "ip": "192.168.1.1", "tier": "free-trial"},
            {"total": 50, "status": "paid", "tags": ["c"], "coupon": "x"},
        ),
        "green_10": ({"color": "green", "price": 10.0, "tier": "free"}, {"total": 100, "status": "open"}),
        "RED_20": ({"color": "RED", "price": 20, "tier": "pro"}, {"total": 99.5, "status": "Paid"}),
        "no_color": ({"other": "x", "price": 11}, {"total": 0}),
        "array": ({"color": AttributeValue("String.Array", '["red", "yellow"]')}, {"tags": "b"}),
        "price_as_string": ({"price": "15", "color": "teal"}, {"total": "150"}),
        "decimal_price": ({"price": Decimal("15.000"), "color": "blue"}, {"total": 1.5e2}),
    }
    # Logged rather than raised: a target that refuses one policy still runs the others.
    client = probe.client(provisioning=Provisioning.ensure(on_error="log"))
    topic = client.topic(probe.name("filters"))
    canary = client.queue(probe.name("filters-all"))
    subscriptions = {"all": client.subscription(topic, canary)}
    queues: dict[str, fanferro.Queue] = {}
    for key, policy in attribute_policies.items():
        queues[key] = client.queue(probe.name(f"f-{key.replace('_', '-')}"))
        subscriptions[key] = client.subscription(topic, queues[key], filter_policy=policy)
    for key, policy in body_policies.items():
        queues[key] = client.queue(probe.name(f"f-{key.replace('_', '-')}"))
        subscriptions[key] = client.subscription(
            topic, queues[key], filter_policy=policy, filter_policy_scope="MessageBody"
        )
    outcomes = await client.provision()
    probe.fact(
        "provisioning_errors",
        sorted(str(describe_error(o)) for o in outcomes if isinstance(o, BaseException)),
    )
    # The logged errors themselves (the outcomes only say that a subscription is not available).
    probe.fact(
        "provisioning_failures",
        {
            key: describe_report(subscription.last_report)
            for key, subscription in subscriptions.items()
            if subscription.last_report is not None and subscription.last_report.status != "created"
        },
    )
    async with Collector(probe, client, [canary, *queues.values()]) as collector:
        await warm_up(probe, topic, collector, canary)
        for label, (attributes, order) in messages.items():
            await probe.outcome(
                f"publish.{label}", topic.publish({"label": label, "order": order}, attributes=attributes)
            )
        await collector.wait(lambda: set(messages) <= set(collector.labels(canary)), timeout=60)
        if probe.target.real:
            await asyncio.sleep(5)
    probe.fact("all", sorted(set(collector.labels(canary)) - _canaries(collector, canary)))
    for key, queue in queues.items():
        probe.fact(f"matched.{key}", sorted(set(collector.labels(queue)) - _canaries(collector, queue)))


@scenario("sns.invalid_filter_policies")
async def sns_invalid_filter_policies(probe: Probe) -> None:
    """Filter policies SNS refuses: unknown operators, too many keys or combinations, bad values."""
    policies: dict[str, tuple[dict[str, Any], str | None]] = {
        "unknown_operator": ({"color": [{"glob": "r*"}]}, None),
        "six_keys": ({f"k{i}": ["v"] for i in range(6)}, None),
        "151_combinations": ({"color": [f"c{i}" for i in range(151)]}, None),
        "numeric_not_a_number": ({"price": [{"numeric": [">", "ten"]}]}, None),
        "nested_in_attribute_scope": ({"order": {"total": [1]}}, None),
        "empty_values": ({"color": []}, None),
        "exists_not_boolean": ({"color": [{"exists": "yes"}]}, None),
        "body_scope_flat": ({"total": [{"numeric": [">", 1]}]}, "MessageBody"),
    }
    owner = probe.client()
    topic = owner.topic(probe.name("bad-filters"))
    queue = owner.queue(probe.name("bad-filters"))
    await topic.resolve()
    await queue.resolve()
    for key, (policy, scope) in policies.items():
        client = probe.client()
        subscription = client.subscription(
            client.topic(topic.name),
            client.queue(queue.name),
            filter_policy=policy,
            filter_policy_scope=scope,  # type: ignore[arg-type]
        )
        await probe.outcome(key, subscription.resolve())


@scenario("sns.fifo")
async def sns_fifo(probe: Probe) -> None:
    """A FIFO topic delivering to a FIFO and a standard queue: order, deduplication, group ids."""
    client = probe.client()
    topic = client.topic(probe.name("fifo-topic.fifo"))
    fifo_queue = client.queue(probe.name("fifo-sub.fifo"))
    standard_queue = client.queue(probe.name("fifo-sub-std"))
    client.subscription(topic, fifo_queue)
    client.subscription(topic, standard_queue)
    outcomes = await client.provision()
    probe.fact("provisioning_errors", [describe_error(o) for o in outcomes if isinstance(o, BaseException)])
    async with Collector(probe, client, [fifo_queue, standard_queue]) as collector:
        started = time.monotonic()
        rounds = 0
        while not collector[fifo_queue] and time.monotonic() - started < 90:
            rounds += 1
            await topic.publish({"label": f"canary-{rounds}"}, group_id="canary")
            await wait_until(lambda: bool(collector[fifo_queue]), 3.0)
        if probe.target.real:
            await asyncio.sleep(5)
        results = [await topic.publish({"label": f"m{i}"}, group_id="ab"[i % 2], id=f"sns-fifo-{i}") for i in range(6)]
        duplicate = await topic.publish({"label": "m0-again"}, group_id="a", id="sns-fifo-0")
        probe.fact("publish_sequence_numbers", [bool(r.sequence_number) for r in results])
        probe.fact("duplicate_same_transport_id", duplicate.transport_message_id == results[0].transport_message_id)
        await probe.outcome("missing_group_id", topic.publish({"label": "x"}))
        await collector.wait(
            lambda: sum(not label.startswith("canary") for label in collector.labels(fifo_queue)) >= 6, timeout=30
        )
    for key, queue in (("fifo", fifo_queue), ("standard", standard_queue)):
        received = [m for m in collector[queue] if not label_of(m).startswith("canary")]
        by_group: dict[str, list[str]] = {}
        for message in received:
            by_group.setdefault(str(message.group_id), []).append(label_of(message))
        probe.fact(f"{key}.received_by_group", dict(sorted(by_group.items())) if key == "fifo" else None)
        probe.fact(f"{key}.received", sorted(label_of(m) for m in received))
        probe.fact(f"{key}.has_sequence_number", sorted({m.sequence_number is not None for m in received}))


@scenario("sns.batches")
async def sns_batches(probe: Probe) -> None:
    """Publishing 12 messages in batches to a topic, and a batch with an invalid entry."""
    client = probe.client()
    topic = client.topic(probe.name("batch-topic"))
    queue = client.queue(probe.name("batch-sub"))
    client.subscription(topic, queue)
    await client.provision()
    async with Collector(probe, client, [queue]) as collector:
        await warm_up(probe, topic, collector, queue)
        result = await topic.publish_batch([{"label": f"m{i}"} for i in range(12)], attributes={"batch": "yes"})
        probe.fact("result", {"ok": result.ok, "successful": len(result.successful)})
        mixed = [OutgoingMessage({"label": "good"}), OutgoingMessage({"label": "bad"}, attributes={"": "x"})]
        await probe.outcome("invalid_entry", topic.publish_batch(mixed))
        await collector.wait(lambda: len(set(collector.labels(queue)) - _canaries(collector, queue)) >= 12)
    probe.fact("received", sorted(set(collector.labels(queue)) - _canaries(collector, queue)))


@scenario("sns.trace_header")
async def sns_trace_header(probe: Probe) -> None:
    """X-Ray trace headers: sent to a queue, published to a topic with and without active tracing."""
    header = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    client = probe.client()
    queue = client.queue(probe.name("trace"))
    passthrough = client.topic(probe.name("trace-pass"))
    active = client.topic(probe.name("trace-active"), attributes=TopicAttributes(tracing="Active"))
    from_passthrough = client.queue(probe.name("trace-pass-sub"))
    from_active = client.queue(probe.name("trace-active-sub"))
    client.subscription(passthrough, from_passthrough)
    client.subscription(active, from_active)
    await client.provision()
    await queue.send({"label": "direct"}, trace_header=header)
    direct = await collect(probe, client, queue, 1)
    probe.fact("direct", [m.trace_header for m in direct])
    for key, topic, target in (("passthrough", passthrough, from_passthrough), ("active", active, from_active)):
        async with Collector(probe, client, [target]) as collector:
            await warm_up(probe, topic, collector, target)
            await topic.publish({"label": "traced"}, trace_header=header)
            await topic.publish_batch([OutgoingMessage({"label": "traced-batch"}, trace_header=header)])
            await collector.wait(lambda c=collector, t=target: {"traced", "traced-batch"} <= set(c.labels(t)))
        traced = {label_of(m): m.trace_header for m in collector[target] if label_of(m).startswith("traced")}
        probe.fact(
            f"{key}.received", {label: value == header if value else None for label, value in sorted(traced.items())}
        )
        probe.fact(f"info.{key}.headers", traced)


@scenario("sns.size_limits")
async def sns_size_limits(probe: Probe) -> None:
    """Messages exactly at and one byte over the 256 KiB topic limit, delivered raw and wrapped."""
    client = probe.client()
    topic = client.topic(probe.name("size-topic"))
    raw = client.queue(probe.name("size-raw"))
    wrapped = client.queue(probe.name("size-wrapped"))
    client.subscription(topic, raw)
    client.subscription(topic, wrapped, raw_message_delivery=False)
    await client.provision()
    info = await topic.resolve()
    probe.fact("info.topic_limit_attribute", info.attributes.get("MaximumMessageSize"))
    id_attributes = {"fanferro.id": AttributeValue.string("sns-size")}
    overhead = attributes_size(id_attributes)
    async with Collector(probe, client, [raw, wrapped]) as collector:
        await warm_up(probe, topic, collector, raw)
        at_limit = await probe.outcome("at_limit", topic.publish(sized_body(262_144, overhead, "at"), id="sns-size"))
        if at_limit is not None:
            probe.fact("at_limit_size", at_limit.size)
        await probe.outcome("over_limit", topic.publish(sized_body(262_145, overhead, "over"), id="sns-size"))
        await collector.wait(lambda: "at" in collector.labels(raw) and "at" in collector.labels(wrapped), timeout=30)
    probe.fact("raw_received", sorted(set(collector.labels(raw)) - _canaries(collector, raw)))
    probe.fact("wrapped_received", sorted(set(collector.labels(wrapped)) - _canaries(collector, wrapped)))


@scenario("sns.deleted_topic")
async def sns_deleted_topic(probe: Probe) -> None:
    """Publishing to a topic after it was deleted, and deleting it again."""
    client = probe.client(provisioning=Provisioning.existing())
    creator = probe.client()
    name = probe.name("gone-topic")
    await creator.topic(name).resolve()
    topic = client.topic(name)
    await topic.resolve()
    await topic.delete()
    await probe.outcome("publish", topic.publish({"label": "x"}))
    await probe.outcome("publish_batch", topic.publish_batch([{"label": "x"}]))
    await probe.outcome("get_attributes", topic.get_attributes())
    await probe.outcome("delete_again", topic.delete())


# -- large payloads --------------------------------------------------------------------------------


@scenario("payloads.roundtrip", timeout=360)
async def payloads_roundtrip(probe: Probe) -> None:
    """Offloaded payloads with each pointer kind and format, through a queue and through a topic."""
    bucket = probe.bucket("payloads")
    variants: dict[str, fanferro.LargePayloads] = {
        "s3": fanferro.LargePayloads(bucket, threshold=1024, create_bucket=True),
        "url": fanferro.LargePayloads(bucket, threshold=1024, reference="url", create_bucket=True),
        "both": fanferro.LargePayloads(bucket, threshold=1024, reference="both", create_bucket=True),
        "aws_extended": fanferro.LargePayloads(bucket, threshold=1024, format="aws_extended", create_bucket=True),
        "sse_s3": fanferro.LargePayloads(bucket, threshold=1024, encryption="s3", create_bucket=True),
        "sse_kms": fanferro.LargePayloads(
            bucket, threshold=1024, encryption=fanferro.Kms("alias/aws/s3"), create_bucket=True
        ),
    }
    big = {"label": "big", "blob": "x" * 5000}
    for key, settings in variants.items():
        client = probe.client(large_payloads=settings)
        queue = client.queue(probe.name(f"pl-{key.replace('_', '-')}"))
        await client.provision()  # creates the bucket
        sent = await probe.outcome(f"{key}.send", queue.send(big))
        if sent is None:
            continue
        pointer = sent.offloaded
        probe.fact(
            f"{key}.pointer",
            None
            if pointer is None
            else {
                "bucket": pointer.bucket,
                "key": pointer.key,
                "has_url": pointer.url is not None,
                "format": pointer.format,
            },
        )
        if pointer is not None and pointer.url:
            probe.fact(f"info.{key}.url", pointer.url)
        messages = await collect(probe, client, queue, 1)
        probe.fact(f"{key}.received", [m.json() == big for m in messages])
        probe.fact(f"{key}.received_pointer", [m.pointer is not None for m in messages])
    client = probe.client(large_payloads=variants["both"])
    topic = client.topic(probe.name("pl-topic"))
    queue = client.queue(probe.name("pl-topic-sub"))
    client.subscription(topic, queue)
    await client.provision()
    huge = {"label": "huge", "blob": "y" * 300_000}
    async with Collector(probe, client, [queue]) as collector:
        await warm_up(probe, topic, collector, queue)
        published = await probe.outcome("topic.publish", topic.publish(huge))
        if published is not None:
            probe.fact("topic.offloaded", published.offloaded is not None)
        await collector.wait(lambda: "huge" in collector.labels(queue))
    probe.fact("topic.received", [m.json() == huge for m in collector[queue] if label_of(m) == "huge"])


@scenario("payloads.failures", timeout=300)
async def payloads_failures(probe: Probe) -> None:
    """Offloaded payloads that cannot be fetched: deleted objects (by S3 and by URL) and expired URLs."""
    bucket = probe.bucket("payload-fail")
    variants: dict[str, tuple[fanferro.LargePayloads, dict[str, Any], bool]] = {
        "deleted_s3": (fanferro.LargePayloads(bucket, threshold=1024, create_bucket=True), {}, True),
        "deleted_url": (
            fanferro.LargePayloads(bucket, threshold=1024, reference="url", create_bucket=True),
            {},
            True,
        ),
        "deleted_with_delete_offloaded": (
            fanferro.LargePayloads(bucket, threshold=1024, reference="both", create_bucket=True),
            {"delete_offloaded_payloads": True},
            True,
        ),
        "expired_url": (
            fanferro.LargePayloads(bucket, threshold=1024, reference="url", presign_ttl=1, create_bucket=True),
            {},
            False,
        ),
    }
    for key, (settings, options, delete) in variants.items():
        client = probe.client(large_payloads=settings)
        events: list[fanferro.Event] = []
        kinds = ["message.decode_failed", "message.failed", "message.duplicate", "message.dead_lettered"]
        client.on(kinds, events.append)
        queue = client.queue(
            probe.name(f"pf-{key.replace('_', '-')}"),
            dead_letter=fanferro.DeadLetterQueue(probe.name(f"pf-{key.replace('_', '-')}-dlq"), max_receive_count=5),
        )
        await client.provision()  # creates the bucket
        sent = await queue.send({"label": key, "blob": "z" * 5000})
        assert sent.offloaded is not None
        if delete:
            object_key = sent.offloaded.key or object_key_of(sent.offloaded.url or "")
            probe.fact(f"{key}.object_key", object_key)
            core = await client._core()
            await core.delete_payload(bucket, object_key)
        if not delete:
            await asyncio.sleep(3)  # let the presigned URL expire

        async def handle(message: fanferro.Message[Any]) -> None:
            raise AssertionError("must not be decoded")

        consumer = client.consumer(queue, handle, wait_time=1, **options)
        await consumer.start()
        try:
            await wait_until(lambda events=events: bool(events), probe.target.delivery_timeout)
            await asyncio.sleep(probe.target.quiet)
        finally:
            await consumer.stop()
        probe.fact(
            f"{key}.events",
            [
                {
                    "kind": event.kind.value,
                    "reason": event.fields.get("reason"),
                    "error": describe_error(event.exception)
                    if event.exception is not None
                    else dataclasses.asdict(event.error)
                    if event.error is not None
                    else None,
                }
                for event in events
            ],
        )
        probe.fact(f"{key}.counts", await queue.approximate_count())


def object_key_of(url: str) -> str:
    """The object key in a presigned URL (path or virtual-hosted style, or the memory scheme): the
    keys of payloads start with ``LargePayloads.prefix``, ``fanferro/`` by default."""
    path = urllib.parse.unquote(urllib.parse.urlsplit(url).path)
    return path[path.index("/fanferro/") + 1 :]


# -- credentials and errors ----------------------------------------------------------------------


@scenario("errors.invalid_credentials", targets=("ministack", "localstack", "aws"))
async def errors_invalid_credentials(probe: Probe) -> None:
    """Credentials AWS does not know: lookups, sends and payload buckets."""
    fake = fanferro.Credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    client = probe.client(credentials=fake, provisioning=Provisioning.existing(), account_id=None)
    await probe.outcome("account", client.account())
    queue = client.queue(probe.name("denied"))
    await probe.outcome("queue", queue.resolve())
    topic = client.topic(probe.name("denied"))
    await probe.outcome("topic", topic.resolve())
    assumed = probe.client(credentials=fake, provisioning=Provisioning.assume(), account_id=FOREIGN_ACCOUNT)
    await probe.outcome("assumed_send", assumed.queue(probe.name("denied")).send({"label": "x"}))
    await probe.outcome("assumed_publish", assumed.topic(probe.name("denied")).publish({"label": "x"}))
    settings = fanferro.LargePayloads(probe.bucket("denied"), always=True, create_bucket=True)
    bucket = probe.client(credentials=fake, large_payloads=settings, provisioning=Provisioning.assume())
    await probe.outcome("bucket_provisioning", enter(bucket))
    await probe.outcome("bucket_send", bucket.queue(probe.name("denied-q")).send({"label": "x"}))


@scenario("errors.missing_profile", targets=("ministack", "localstack", "aws"))
async def errors_missing_profile(probe: Probe) -> None:
    """A named profile that does not exist (no credentials can be loaded)."""
    client = probe.target.client(profile="ffprobe-no-such-profile", credentials=None, account_id=None)
    probe.clients.append(client)
    await probe.outcome("queue", client.queue(probe.name("noprofile")).resolve(), messages=False)


# -- running ---------------------------------------------------------------------------------------


def start_ministack() -> tuple[str, subprocess.Popen[bytes]]:
    """Starts MiniStack on a free port (like the test suite does)."""
    executable = shutil.which("ministack") or str(Path(sys.executable).with_name("ministack"))
    if not Path(executable).exists():
        raise SystemExit("MiniStack is not installed (uv sync installs it with the test group); pass --endpoint")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])
    log = Path(tempfile.gettempdir()) / f"ffprobe-ministack-{port}.log"
    env = {**os.environ, "GATEWAY_PORT": str(port), "MINISTACK_REGION": "us-east-1"}
    with log.open("wb") as output:
        process = subprocess.Popen([executable], env=env, stdout=output, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"MiniStack exited with code {process.returncode}; see {log}")
        try:
            with urllib.request.urlopen(f"{url}/_ministack/health", timeout=2) as response:
                if response.status == 200:
                    return url, process
        except OSError:
            time.sleep(0.2)
    process.terminate()
    raise SystemExit(f"MiniStack did not become healthy; see {log}")


def emulator_version(target: Target) -> str | None:
    """The version an emulator reports on its health endpoint (MiniStack and LocalStack have one)."""
    if target.endpoint is None:
        return None
    for path in ("/_localstack/health", "/_ministack/health"):
        try:
            with urllib.request.urlopen(f"{target.endpoint}{path}", timeout=5) as response:
                health = json.loads(response.read())
        except (OSError, ValueError):
            continue
        if isinstance(health, dict) and health.get("version"):
            return f"{health.get('edition', '')} {health['version']}".strip()
    return None


async def run_scenario(target: Target, run: str, item: Scenario, normalizer: Normalizer) -> dict[str, Any]:
    """Runs one scenario and returns its record."""
    if target.kind not in item.targets:
        return {"status": "skipped", "doc": item.doc, "facts": {}, "reason": f"not applicable to {target.kind}"}
    probe = Probe(target, run, item.name, normalizer)
    started = time.monotonic()
    status, error = "ok", None
    try:
        await asyncio.wait_for(item.run(probe), item.timeout)
    except (TimeoutError, Exception) as exc:  # noqa: BLE001 - a crashed scenario is a result too
        status = "timeout" if isinstance(exc, TimeoutError) else "crashed"
        error = normalizer.text("".join(traceback.format_exception_only(exc)).strip())
        probe.fact("info.traceback", traceback.format_exc().splitlines()[-12:])
    finally:
        with contextlib.suppress(TimeoutError, Exception):  # best effort: the final sweep deletes the rest
            await asyncio.wait_for(probe.close(), 120)
    return {
        "status": status,
        "doc": item.doc,
        "seconds": round(time.monotonic() - started, 1),
        "error": error,
        "facts": probe.facts,
    }


def sweep(target: Target, prefix: str) -> list[str]:
    """Deletes queues, topics and buckets whose names start with ``prefix`` (AWS-protocol targets)."""
    deleted: list[str] = []
    sqs = target.boto3_client("sqs")
    for url in sqs.list_queues(QueueNamePrefix=prefix, MaxResults=1000).get("QueueUrls", []):
        with contextlib.suppress(Exception):
            sqs.delete_queue(QueueUrl=url)
            deleted.append(url.rsplit("/", 1)[-1])
    sns = target.boto3_client("sns")
    for page in sns.get_paginator("list_topics").paginate():
        for topic in page.get("Topics", []):
            arn = topic["TopicArn"]
            if arn.rsplit(":", 1)[-1].startswith(prefix):
                with contextlib.suppress(Exception):
                    for sub_page in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=arn):
                        for subscription in sub_page.get("Subscriptions", []):
                            if subscription["SubscriptionArn"].startswith("arn:"):
                                sns.unsubscribe(SubscriptionArn=subscription["SubscriptionArn"])
                    sns.delete_topic(TopicArn=arn)
                    deleted.append(arn.rsplit(":", 1)[-1])
    s3 = target.boto3_client("s3")
    with contextlib.suppress(Exception):
        for bucket in s3.list_buckets().get("Buckets", []):
            name = bucket["Name"]
            if not name.startswith(prefix):
                continue
            with contextlib.suppress(Exception):
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name):
                    keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    if keys:
                        s3.delete_objects(Bucket=name, Delete={"Objects": keys})
                s3.delete_bucket(Bucket=name)
                deleted.append(name)
    return deleted


def make_target(args: argparse.Namespace) -> tuple[Target, subprocess.Popen[bytes] | None]:
    """The target the arguments describe (starting MiniStack when needed)."""
    kind = args.target
    process = None
    endpoint = args.endpoint
    region = args.region
    if kind == "aws":
        if endpoint:
            raise SystemExit("--endpoint is for emulators; real AWS uses the regional endpoints")
        region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not region:
            raise SystemExit("pass --region (or set AWS_REGION) for real AWS")
    else:
        region = region or "us-east-1"
    if kind == "ministack" and not endpoint:
        endpoint, process = start_ministack()
    if kind == "localstack" and not endpoint:
        endpoint = "http://localhost:4566"
    broker = fanferro.MemoryBroker(region=region) if kind == "memory" else None
    return Target(kind, region, endpoint.rstrip("/") if endpoint else None, args.profile, broker), process


async def run_command(args: argparse.Namespace) -> int:
    """``run``: probes one target and writes its facts."""
    selected = [s for s in SCENARIOS.values() if not args.only or any(re.search(p, s.name) for p in args.only)]
    if not selected:
        raise SystemExit("no scenario matches --only")
    target, process = make_target(args)
    run = uuid.uuid4().hex[:8]
    normalizer = Normalizer(run, target.region, target.endpoint)
    output = Path(args.out or f"probe-{target.kind}.json")
    record: dict[str, Any] = {
        "probe_version": PROBE_VERSION,
        "target": target.kind,
        "fanferro": fanferro.__version__,
        "python": platform.python_version(),
        "endpoint_version": emulator_version(target),
        "region": target.region,
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenarios": {},
    }
    print(f"probing {target.kind} ({target.region}{', ' + target.endpoint if target.endpoint else ''}), run {run}")
    print(f"resources are named {PREFIX}{run}-*; results go to {output}")
    semaphore = asyncio.Semaphore(max(1, args.jobs))

    async def one(item: Scenario) -> None:
        async with semaphore:
            result = await run_scenario(target, run, item, normalizer)
        record["scenarios"][item.name] = result
        mark = {"ok": "ok", "skipped": "--"}.get(result["status"], "!!")
        detail = f"  {result['error']}" if result.get("error") else ""
        print(f"[{mark}] {item.name:42} {result.get('seconds', 0):6.1f}s{detail}", flush=True)
        record["scenarios"] = dict(sorted(record["scenarios"].items()))
        text = json.dumps(record, indent=2, ensure_ascii=False) + "\n"
        await asyncio.to_thread(output.write_text, text, encoding="utf-8")

    try:
        await asyncio.gather(*(one(item) for item in selected))
    finally:
        if target.aws_protocol and not args.keep:
            deleted = await asyncio.to_thread(sweep, target, f"{PREFIX}{run}")
            if deleted:
                print(f"cleanup deleted {len(deleted)} leftover resources: {', '.join(sorted(deleted))}")
        if process is not None:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10)
    failed = [name for name, result in record["scenarios"].items() if result["status"] in {"crashed", "timeout"}]
    print(f"wrote {output}: {len(record['scenarios'])} scenarios, {len(failed)} crashed or timed out")
    return 1 if failed else 0


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flattens nested dicts into dotted keys (lists stay whole)."""
    if isinstance(value, dict) and value and not set(value) <= {"bytes"}:
        out: dict[str, Any] = {}
        for key, item in value.items():
            out.update(flatten(item, f"{prefix}.{key}" if prefix else str(key)))
        return out
    return {prefix: value}


def without_info(value: Any) -> Any:
    """``value`` without its ``info.`` entries, which are never compared."""
    if isinstance(value, dict):
        return {key: without_info(item) for key, item in value.items() if not str(key).startswith("info.")}
    if isinstance(value, list):
        return [without_info(item) for item in value]
    return value


def is_error(value: Any) -> bool:
    """Whether a fact describes an exception (see :func:`describe_error`)."""
    return isinstance(value, dict) and "type" in value and "message" in value


def render(value: Any) -> str:
    """A fact on one line: an exception as its class, classification and message."""
    if not is_error(value):
        return json.dumps(value, ensure_ascii=False)
    error = value
    report = error.get("report") or {}
    if report.get("error"):
        error = report["error"]  # what failed underneath a ProvisioningError
    details = ", ".join(f"{key}={error[key]}" for key in ("kind", "code", "status", "retryable") if key in error)
    prefix = f"{value['type']}(report={report.get('status')}) <- " if error is not value else ""
    return f"{prefix}{error['type']}({details}): {value['message']}"


def compare_command(args: argparse.Namespace) -> int:
    """``compare``: prints the facts that differ from the first file."""
    records = [json.loads(Path(path).read_text(encoding="utf-8")) for path in args.files]
    names = [f"{r['target']}" for r in records]
    if len(set(names)) != len(names):
        names = [f"{r['target']}#{i}" for i, r in enumerate(records)]
    width = max(len(name) for name in names) + 1
    reference = names[0]
    print(f"reference: {reference} (fanferro {records[0]['fanferro']}, {records[0]['region']})")
    print(
        "compared:  "
        + ", ".join(f"{n} (fanferro {r['fanferro']})" for n, r in zip(names[1:], records[1:], strict=True))
    )
    scenario_names = sorted({name for record in records for name in record["scenarios"]})
    differences: dict[str, int] = dict.fromkeys(names[1:], 0)
    total = 0

    def rows(values: list[Any]) -> list[str]:
        return [f"      {name:{width}} {render(value)}" for name, value in zip(names, values, strict=True)]

    for scenario_name in scenario_names:
        results = [record["scenarios"].get(scenario_name) for record in records]
        lines: list[str] = []
        statuses = [r["status"] if r else "missing" for r in results]
        if len(set(statuses)) > 1 or statuses[0] not in {"ok", "skipped"}:
            lines.append("  status: " + ", ".join(f"{n}={s}" for n, s in zip(names, statuses, strict=True)))
            for name, result in zip(names, results, strict=True):
                if result and result.get("error"):
                    lines.append(f"    {name}: {result['error']}")
        facts = [r["facts"] if r and r["status"] != "skipped" else None for r in results]
        # A scenario that crashed or timed out stopped early: the facts it did not get to are not
        # differences.
        absent = ["<absent>" if r and r["status"] == "ok" else "<skipped>" for r in results]
        keys = list(dict.fromkeys(key for f in facts if f for key in f))
        for key in keys:
            info = key.startswith("info.")
            if info and not args.all:
                continue
            values = [
                f.get(key, missing) if f is not None else "<skipped>" for f, missing in zip(facts, absent, strict=True)
            ]
            compared = [without_info(v) for v in values]
            present = [v for v in compared if v != "<skipped>"]
            same = all(v == present[0] for v in present)
            if not info:
                total += 1
                if compared[0] != "<skipped>":
                    for name, value in zip(names[1:], compared[1:], strict=True):
                        if value not in ("<skipped>", compared[0]):
                            differences[name] += 1
            if same and not args.all:
                continue
            marker = "  " if same or info else "≠ "
            if not same and all(isinstance(v, dict) and not is_error(v) for v in present):
                flat = [flatten(v) if v != "<skipped>" else None for v in compared]
                for sub in dict.fromkeys(k for f in flat if f is not None for k in f):
                    sub_values = [f.get(sub, "<absent>") if f is not None else "<skipped>" for f in flat]
                    shown = [v for v in sub_values if v != "<skipped>"]
                    if any(v != shown[0] for v in shown):
                        lines.append(f"  {marker}{key}.{sub}")
                        lines.extend(rows(sub_values))
                continue
            lines.append(f"  {marker}{key}")
            lines.extend(rows(compared if not args.all else values))
        if lines:
            doc = next((r["doc"] for r in results if r), "")
            print(f"\n== {scenario_name} ==  {doc}")
            print("\n".join(lines))
    print(
        f"\n{total} facts compared; differences from {reference}: "
        + ", ".join(f"{n} {c}" for n, c in differences.items())
    )
    return 0


def iam_policy(account: str) -> dict[str, Any]:
    """The least privilege a run on real AWS needs: the probe's own resources, cleanup listings, and
    the AWS managed KMS keys through SQS, SNS and S3."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ProbeQueues",
                "Effect": "Allow",
                "Action": "sqs:*",
                "Resource": f"arn:aws:sqs:*:{account}:{PREFIX}*",
            },
            {
                "Sid": "ProbeTopics",
                "Effect": "Allow",
                "Action": "sns:*",
                "Resource": f"arn:aws:sns:*:{account}:{PREFIX}*",
            },
            {
                "Sid": "ProbeBuckets",
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": [f"arn:aws:s3:::{PREFIX}*", f"arn:aws:s3:::{PREFIX}*/*"],
            },
            {
                "Sid": "Cleanup",
                "Effect": "Allow",
                "Action": ["sqs:ListQueues", "sns:ListTopics", "s3:ListAllMyBuckets"],
                "Resource": "*",
            },
            {
                "Sid": "ManagedKeys",
                "Effect": "Allow",
                "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
                "Resource": "*",
                "Condition": {
                    "StringLike": {
                        "kms:ViaService": ["sqs.*.amazonaws.com", "sns.*.amazonaws.com", "s3.*.amazonaws.com"]
                    }
                },
            },
        ],
    }


def iam_policy_command(args: argparse.Namespace) -> int:
    """``iam-policy``: prints the IAM policy a run on real AWS needs."""
    if not re.fullmatch(r"\d{12}", args.account):
        raise SystemExit("--account takes a 12 digit AWS account id")
    print(json.dumps(iam_policy(args.account), indent=2))
    return 0


def list_command(_args: argparse.Namespace) -> int:
    """``list``: prints the scenarios."""
    for item in SCENARIOS.values():
        only = "" if item.targets == TARGETS else f" [{', '.join(item.targets)}]"
        print(f"{item.name:36} {item.doc}{only}")
    return 0


async def cleanup_command(args: argparse.Namespace) -> int:
    """``cleanup``: deletes every probe resource (of all runs) on an AWS-protocol target."""
    if args.target in {"memory", "ministack"} and not args.endpoint:
        raise SystemExit("nothing to clean up: the in-memory broker and a MiniStack started by run are gone")
    target, _ = make_target(args)
    deleted = await asyncio.to_thread(sweep, target, PREFIX)
    print(f"deleted {len(deleted)} resources" + (": " + ", ".join(sorted(deleted)) if deleted else ""))
    return 0


def main() -> int:
    """Command line entry point."""
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # facts are Unicode, consoles may not be
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "cleanup"):
        command = commands.add_parser(name)
        command.add_argument("--target", choices=TARGETS, required=True)
        command.add_argument("--endpoint", help="emulator endpoint (default: start MiniStack; LocalStack on :4566)")
        command.add_argument("--region", help="AWS region (default: AWS_REGION for aws, us-east-1 for emulators)")
        command.add_argument("--profile", help="AWS profile (default: the AWS SDK's provider chain)")
        if name == "run":
            command.add_argument("--out", help="result file (default: probe-<target>.json)")
            command.add_argument("--only", action="append", help="run scenarios matching this regex (repeatable)")
            command.add_argument("--jobs", type=int, default=1, help="scenarios run concurrently (default 1)")
            command.add_argument("--keep", action="store_true", help="skip the final cleanup sweep")
    compare = commands.add_parser("compare")
    compare.add_argument("files", nargs="+", help="result files; the first one is the reference (real AWS)")
    compare.add_argument("--all", action="store_true", help="also print facts that are the same everywhere")
    commands.add_parser("list")
    policy = commands.add_parser("iam-policy")
    policy.add_argument("--account", required=True, help="the AWS account id of the test account")
    args = parser.parse_args()
    if args.command == "run":
        return asyncio.run(run_command(args))
    if args.command == "cleanup":
        return asyncio.run(cleanup_command(args))
    if args.command == "compare":
        return compare_command(args)
    if args.command == "iam-policy":
        return iam_policy_command(args)
    return list_command(args)


if __name__ == "__main__":
    sys.exit(main())

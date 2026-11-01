"""Invalid options raise ``ConfigurationError``: a ``FanferroError`` that is also a ``ValueError``."""

from __future__ import annotations

import pickle
from collections.abc import Callable
from typing import Any

import pytest

import fanferro
from fanferro import aws_lambda
from fanferro.idempotency import InMemoryIdempotencyStore
from fanferro.outbox import InMemoryOutbox, OutboxRelay
from fanferro.testing import RecordingSettler, build_message, memory_client


async def _noop(message: fanferro.Message) -> None: ...


def _consumer(**options: Any) -> Callable[[], object]:
    return lambda: memory_client().consumer("invalid", _noop, **options)


INVALID: dict[str, Callable[[], object]] = {
    "consumer concurrency": _consumer(concurrency=0),
    "consumer prefetch": _consumer(prefetch=-1),
    "consumer batch_size": _consumer(batch_size=11),
    "consumer min_receive_batch": _consumer(min_receive_batch=0),
    "consumer wait_time": _consumer(wait_time=21),
    "consumer visibility_timeout": _consumer(visibility_timeout=-3),
    "consumer handler_timeout": _consumer(handler_timeout=0),
    "consumer pollers": _consumer(pollers=0),
    "consumer max_pollers": _consumer(pollers=2, max_pollers=1),
    "consumer settle_linger": _consumer(settle_linger=2),
    "consumer max_receive_failures": _consumer(max_receive_failures=0),
    "Backoff": lambda: fanferro.Backoff(multiplier=0.5),
    "RetryPolicy": lambda: fanferro.RetryPolicy(max_attempts=0),
    "ShutdownPolicy": lambda: fanferro.ShutdownPolicy(progress_interval=0),
    "RateLimit per_second": lambda: fanferro.RateLimit(0),
    "RateLimit burst": lambda: fanferro.RateLimit(10, burst=0),
    "Heartbeat": lambda: fanferro.Heartbeat(interval=-1),
    "DeadLetterQueue": lambda: fanferro.DeadLetterQueue("dlq", max_receive_count=0),
    "LargePayloads bucket": lambda: fanferro.LargePayloads(""),
    "LargePayloads threshold": lambda: fanferro.LargePayloads("bucket", threshold=0),
    "AwsRetry max_attempts": lambda: fanferro.AwsRetry(max_attempts=0),
    "AwsRetry backoff": lambda: fanferro.AwsRetry(initial_backoff=-1),
    "Timeouts": lambda: fanferro.Timeouts(read=-1),
    "Client prefix": lambda: fanferro.Client(queue_prefix="a.fifo"),
    "message_type": lambda: fanferro.message_type(""),
    "event selector": lambda: memory_client().events.subscribe("no.such.*", lambda event: None),
    "sqs_handler timeout_margin": lambda: aws_lambda.sqs_handler(memory_client(), _noop, timeout_margin=61),
    "InMemoryIdempotencyStore": lambda: InMemoryIdempotencyStore(max_entries=0),
    "OutboxRelay": lambda: OutboxRelay(memory_client(), InMemoryOutbox(), max_attempts=0),
    "OutgoingMessage delay": lambda: fanferro.OutgoingMessage({}, delay=-1),
    "codec name": lambda: memory_client().queue("invalid", codec="jsn"),
}


@pytest.mark.parametrize("create", INVALID.values(), ids=INVALID.keys())
def test_invalid_options_raise_configuration_error(create: Callable[[], object]) -> None:
    with pytest.raises(fanferro.ConfigurationError) as caught:
        create()
    assert isinstance(caught.value, fanferro.FanferroError)
    assert isinstance(caught.value, ValueError), "`except ValueError` keeps catching invalid options"
    assert caught.value.retryable is False


def test_configuration_error_is_a_value_error_that_pickles() -> None:
    assert issubclass(fanferro.ConfigurationError, fanferro.FanferroError)
    assert issubclass(fanferro.ConfigurationError, ValueError)
    assert fanferro.ConfigurationError.__module__ == "fanferro.errors"
    error = fanferro.ConfigurationError("bad option")
    copied = pickle.loads(pickle.dumps(error))
    assert type(copied) is fanferro.ConfigurationError
    assert copied.args == ("bad option",)


async def test_core_configuration_errors_are_value_errors() -> None:
    # Raised by the Rust core: an app name the AWS SDK refuses (checked before any network call).
    client = fanferro.Client(
        region="us-east-1",
        endpoint_url="http://127.0.0.1:9",
        credentials=fanferro.Credentials("test", "test"),
        app_name="not an app name",
    )
    with pytest.raises(ValueError, match="invalid app_name") as caught:
        await client._core()  # creates the native client
    assert type(caught.value) is fanferro.ConfigurationError


def test_unknown_codec_names_list_the_registered_codecs() -> None:
    client = memory_client()
    with pytest.raises(fanferro.ConfigurationError, match=r"no codec named 'jsn'; the registered codecs are .*'json'"):
        client.topic("orders", codec="jsn")
    assert client.queue("orders", codec="json") is not None


async def test_settling_twice_raises_lease_error() -> None:
    message = build_message({"n": 1}, settler=RecordingSettler())
    await message.ack()
    with pytest.raises(fanferro.LeaseError, match="already settled") as caught:
        await message.retry()
    assert isinstance(caught.value, RuntimeError), "`except RuntimeError` keeps catching it"
    assert issubclass(fanferro.LeaseError, fanferro.FanferroError)
    with pytest.raises(fanferro.LeaseError):
        await message.extend(30)

"""Errors cross process boundaries (pickle) and copy as themselves, with their attributes."""

from __future__ import annotations

import copy
import pickle
from dataclasses import dataclass
from typing import Any

import pytest

import fanferro
import fanferro.errors
from fanferro import testing
from fanferro.messages import BatchFailure, BatchPublishResult, OutgoingMessage, PublishResult
from fanferro.provisioning import ProvisioningReport

ALL = sorted(
    (
        value
        for name in fanferro.errors.__all__
        if isinstance(value := getattr(fanferro.errors, name), type) and issubclass(value, BaseException)
    ),
    key=lambda value: value.__name__,
)


def make(error_type: type[Exception]) -> Exception:
    if error_type is fanferro.ProvisioningError:
        report = ProvisioningReport(kind="queue", name="orders", status="missing", resource=None)
        return fanferro.ProvisioningError("boom", report)
    if error_type is fanferro.BatchPublishError:
        return fanferro.BatchPublishError("boom", BatchPublishResult((PublishResult("1", "t1", "orders"),)))
    return error_type("boom")


def round_trip(error: BaseException) -> Any:
    return pickle.loads(pickle.dumps(error))


def make_local_type() -> type:
    @dataclass
    class Local:
        n: int

    return Local


class TwoArgumentError(Exception):
    def __init__(self, message: str, detail: str) -> None:
        super().__init__(message)
        self.detail = detail


@pytest.mark.parametrize("error_type", ALL, ids=lambda error_type: error_type.__name__)
def test_every_error_pickles_and_copies_with_its_attributes(error_type: type[Exception]) -> None:
    # Unpickling does not call the constructor, which may take more than the message: a class with a
    # second required argument pickled, and then failed to unpickle in the process that received it.
    error = make(error_type)
    error.extra = {"n": 1}  # pyrefly: ignore[missing-attribute]
    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        copied = pickle.loads(pickle.dumps(error, protocol))
        assert (type(copied), copied.args, vars(copied)) == (error_type, ("boom",), vars(error))
    for copied in (copy.copy(error), copy.deepcopy(error)):
        assert (type(copied), copied.args, vars(copied)) == (error_type, ("boom",), vars(error))


async def test_errors_of_calls_keep_their_attributes() -> None:
    async with testing.memory_client(provisioning=fanferro.Provisioning.existing()) as client:
        with pytest.raises(fanferro.ProvisioningError) as missing:
            await client.queue("missing").send({"x": 1})
        queue = client.queue("orders", provisioning=fanferro.Provisioning.ensure())
        messages = [
            OutgoingMessage(body={"n": 1}),
            OutgoingMessage(body={"n": 2}, attributes={"bad name": "x", "tenant": fanferro.AttributeValue.string("a")}),
        ]
        with pytest.raises(fanferro.BatchPublishError) as failed:
            await queue.send_batch(messages)
    provisioning = round_trip(missing.value)
    assert (type(provisioning), str(provisioning)) == (fanferro.ProvisioningError, str(missing.value))
    assert provisioning.report == missing.value.report
    assert provisioning.report.status == "missing"
    batch = round_trip(failed.value)
    assert (type(batch), str(batch), batch.retryable) == (fanferro.BatchPublishError, str(failed.value), False)
    assert batch.result.successful == failed.value.result.successful
    [failure] = batch.result.failed
    assert (failure.index, failure.message) == (1, messages[1])
    assert type(failure.error) is fanferro.ValidationError
    assert vars(failure.error) == vars(failed.value.result.failed[0].error)


def test_values_that_do_not_pickle_are_left_out() -> None:
    # Only the values that do not pickle (or unpickle) are left out: a failed message keeps its id,
    # type and attributes when its body is an instance of a class defined in a function.
    local = make_local_type()
    message = OutgoingMessage(
        body=local(1), type="local", attributes={"tenant": "a"}, envelope=fanferro.envelopes.CloudEvents()
    )
    sent = OutgoingMessage(body={"n": 2})
    failure = BatchFailure(0, message, fanferro.ThrottledError("slow down"))
    foreign = BatchFailure(2, sent, TwoArgumentError("no", "detail"))
    result = BatchPublishResult((failure, PublishResult("2", "t2", "orders"), foreign))
    error = fanferro.BatchPublishError("2 of 3 messages failed", result)
    error.hook = lambda: None  # pyrefly: ignore[missing-attribute]
    with pytest.raises((pickle.PicklingError, AttributeError, TypeError)):
        pickle.dumps(dict(vars(error)))
    copied = round_trip(error)
    assert (type(copied), str(copied), copied.hook) == (fanferro.BatchPublishError, str(error), None)
    first, published, second = copied.result.results
    assert (first.index, first.message.body, first.message.envelope) == (0, None, None)
    assert (first.message.id, first.message.type, first.message.attributes) == (message.id, "local", {"tenant": "a"})
    assert (type(first.error), str(first.error)) == (fanferro.ThrottledError, "slow down")
    assert published == result.results[1]
    assert (second.index, second.message, second.error) == (2, sent, None)


def test_copies_keep_every_attribute() -> None:
    local = make_local_type()
    message = OutgoingMessage(body=local(1))
    error = fanferro.BatchPublishError("failed", BatchPublishResult((BatchFailure(0, message, ValueError("x")),)))
    error.hook = print  # pyrefly: ignore[missing-attribute]
    shallow, deep = copy.copy(error), copy.deepcopy(error)
    for copied in (shallow, deep):
        assert (type(copied), copied.args, vars(copied)["hook"]) == (fanferro.BatchPublishError, ("failed",), print)
        copied_message = copied.result.failed[0].message
        assert isinstance(copied_message, OutgoingMessage)
        assert copied_message.body == local(1)
    assert shallow.result is error.result
    assert deep.result is not error.result

"""Lambda: records of asynchronous invocations and events sent to a dead-letter queue."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from fanferro.aws._document import AwsDocument, _at
from fanferro.message_types import message_type

if TYPE_CHECKING:
    from fanferro.messages import Message


@dataclass(frozen=True, slots=True, kw_only=True)
class LambdaRequestContext(AwsDocument):
    """The invocation a record is about."""

    request_id: str | None = None
    function_arn: str | None = None
    condition: str | None = None
    """``Success``, ``RetriesExhausted``, ``EventAgeExceeded``... (``RetryAttemptsExhausted`` and
    others for event source mappings)."""
    approximate_invoke_count: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class LambdaResponseContext(AwsDocument):
    """The function's response."""

    status_code: int | None = None
    executed_version: str | None = None
    function_error: str | None = None
    """``Handled`` or ``Unhandled`` when the function failed."""


@message_type("aws.lambda.invocation_failure", "aws.lambda.invocation_success")
@dataclass(frozen=True, slots=True, kw_only=True)
class LambdaInvocationRecord(AwsDocument):
    """The record of an asynchronous invocation sent to a destination (SQS, SNS or EventBridge).

    Message types ``aws.lambda.invocation_failure`` and ``aws.lambda.invocation_success``. The
    on-failure destinations of event source mappings (Kinesis, DynamoDB, Kafka) send records of
    the same shape, with the batch in :attr:`batch_info`.
    """

    aws_message_type: ClassVar[str | None] = "aws.lambda.invocation_failure"

    request_context: LambdaRequestContext
    timestamp: datetime | None = None
    version: str | None = None
    request_payload: Any = None
    """The event the function was invoked with, as JSON data."""
    response_context: LambdaResponseContext | None = None
    response_payload: Any = None
    """What the function returned, or its error (``errorMessage``, ``errorType``, ``stackTrace``)."""
    kinesis_batch_info: Any = field(default=None, metadata=_at("KinesisBatchInfo"))
    ddb_stream_batch_info: Any = field(default=None, metadata=_at("DDBStreamBatchInfo"))

    @property
    def succeeded(self) -> bool:
        """Whether the invocation succeeded."""
        return self.request_context.condition == "Success"

    @property
    def error_message(self) -> str | None:
        """The function's error message, when it failed with one."""
        payload = self.response_payload
        message = payload.get("errorMessage") if isinstance(payload, dict) else None
        return message if isinstance(message, str) else None


def _integer(value: object) -> int | None:
    """An attribute's number as an integer (``None`` if it is not one)."""
    try:
        return int(value) if isinstance(value, int | float | Decimal | str) and not isinstance(value, bool) else None
    except ValueError:
        return None


@dataclass(frozen=True, slots=True, kw_only=True)
class LambdaDeadLetter:
    """Why an event reached a Lambda function's dead-letter queue (message type ``aws.lambda.dead_letter``).

    The message's body is the event the function failed to process, decoded into the class its
    handler is annotated with; this reads the attributes Lambda adds::

        @consumer.route("aws.lambda.dead_letter")
        async def on_failed(message: fanferro.Message[OrderPlaced]) -> None:
            failure = aws.LambdaDeadLetter.of(message)
    """

    request_id: str | None
    error_code: int | None
    """The HTTP status code of the invocation."""
    error_message: str | None
    """The first kilobyte of the error message."""

    @classmethod
    def of(cls, message: Message[Any]) -> LambdaDeadLetter | None:
        """The failure ``message`` records (``None`` if it is not a Lambda dead letter)."""
        if message.type != "aws.lambda.dead_letter":
            return None
        request_id, error_code, error_message = (
            message.attribute("RequestID"),
            message.attribute("ErrorCode"),
            message.attribute("ErrorMessage"),
        )
        return cls(
            request_id=request_id if isinstance(request_id, str) else None,
            error_code=_integer(error_code),
            error_message=error_message if isinstance(error_message, str) else None,
        )

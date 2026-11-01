"""fanferro: fast, robust SNS + SQS messaging for Python, powered by a Rust core.

Quick start::

    import fanferro

    client = fanferro.Client(region="eu-west-1", source="urn:service:orders")
    orders = client.topic("orders", provisioning=fanferro.Provisioning.ensure())
    worker = client.queue(
        "orders-worker",
        dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"),
        provisioning=fanferro.Provisioning.ensure(),
    )
    client.subscription(orders, worker)
    consumer = client.consumer(worker, concurrency=20)


    @consumer.route(OrderCreated)
    async def on_created(order: OrderCreated) -> None: ...


    async def main() -> None:
        async with client:
            await orders.publish(OrderCreated(id=1))
            await consumer.run()

Read ``fanferro guide`` (or ``python -m fanferro guide``) for the full usage guide.
"""

# Public submodules: `from x import m as m` (or `__all__`) makes type checkers see `fanferro.errors`
# and the others after `import fanferro`; `from fanferro.errors import ...` alone does not.
from fanferro import actions as actions
from fanferro import aws_lambda as aws_lambda
from fanferro import client as client
from fanferro import codecs, envelopes, idempotency, matchers, outbox, testing
from fanferro import config as config
from fanferro import consumer as consumer
from fanferro import errors as errors
from fanferro import events as events
from fanferro import message_types as message_types
from fanferro import messages as messages
from fanferro import middleware as middleware
from fanferro import payloads as payloads
from fanferro import policies as policies
from fanferro import provisioning as provisioning
from fanferro import resources as resources
from fanferro import routing as routing
from fanferro import telemetry as telemetry
from fanferro._core import (
    AttributeValue,
    MemoryBroker,
    Payload,
    WireMessage,
    new_message_id,
)
from fanferro._core import __version__ as __version__
from fanferro.actions import Discard, HandlerAction, Reject, Requeue, Retry
from fanferro.client import Client
from fanferro.codecs import Codec, CodecRegistry
from fanferro.config import AwsRetry, Credentials, Timeouts
from fanferro.consumer import Consumer
from fanferro.envelopes import Envelope
from fanferro.errors import (
    AccessDeniedError,
    AwsError,
    BatchItemFailuresError,
    BatchPublishError,
    CodecError,
    ConfigurationError,
    ConsumerStateError,
    DecodeError,
    EncodeError,
    EnvelopeError,
    FanferroError,
    HandlerTimeoutError,
    LeaseError,
    MessageTooLargeError,
    PayloadError,
    PayloadIntegrityError,
    ProvisioningError,
    ResourceNotFoundError,
    ResourceUnavailableError,
    RoutingError,
    ShutdownError,
    ThrottledError,
    TransportError,
    ValidationError,
)
from fanferro.events import ErrorInfo, Event, EventHook, EventKind, LoggingConfig
from fanferro.message_types import MessageTypes, message_type
from fanferro.messages import (
    BatchFailure,
    BatchPublishResult,
    Message,
    OutgoingMessage,
    PayloadPointer,
    PreparedMessage,
    PublishResult,
    Settlement,
)
from fanferro.middleware import ConsumeMiddleware, HandlerNext, PrepareNext, PublishContext, PublishMiddleware
from fanferro.payloads import LargePayloads, PayloadFetch
from fanferro.policies import Backoff, DeadLetterPolicy, Heartbeat, RateLimit, RetryPolicy, ShutdownPolicy
from fanferro.provisioning import (
    DeadLetterQueue,
    Drift,
    Fallback,
    Kms,
    Provisioning,
    ProvisioningFailure,
    ProvisioningReport,
    QueueAttributes,
    QueueInfo,
    SqsManagedEncryption,
    SubscriptionInfo,
    TopicAttributes,
    TopicInfo,
)
from fanferro.resources import Queue, Subscription, Topic
from fanferro.routing import Route, Router
from fanferro.telemetry import Carrier, Propagation, Telemetry

__all__ = [
    "AccessDeniedError",
    "AttributeValue",
    "AwsError",
    "AwsRetry",
    "Backoff",
    "BatchFailure",
    "BatchItemFailuresError",
    "BatchPublishError",
    "BatchPublishResult",
    "Carrier",
    "Client",
    "Codec",
    "CodecError",
    "CodecRegistry",
    "ConfigurationError",
    "ConsumeMiddleware",
    "Consumer",
    "ConsumerStateError",
    "Credentials",
    "DeadLetterPolicy",
    "DeadLetterQueue",
    "DecodeError",
    "Discard",
    "Drift",
    "EncodeError",
    "Envelope",
    "EnvelopeError",
    "ErrorInfo",
    "Event",
    "EventHook",
    "EventKind",
    "Fallback",
    "FanferroError",
    "HandlerAction",
    "HandlerNext",
    "HandlerTimeoutError",
    "Heartbeat",
    "Kms",
    "LargePayloads",
    "LeaseError",
    "LoggingConfig",
    "MemoryBroker",
    "Message",
    "MessageTooLargeError",
    "MessageTypes",
    "OutgoingMessage",
    "Payload",
    "PayloadError",
    "PayloadFetch",
    "PayloadIntegrityError",
    "PayloadPointer",
    "PrepareNext",
    "PreparedMessage",
    "Propagation",
    "Provisioning",
    "ProvisioningError",
    "ProvisioningFailure",
    "ProvisioningReport",
    "PublishContext",
    "PublishMiddleware",
    "PublishResult",
    "Queue",
    "QueueAttributes",
    "QueueInfo",
    "RateLimit",
    "Reject",
    "Requeue",
    "ResourceNotFoundError",
    "ResourceUnavailableError",
    "Retry",
    "RetryPolicy",
    "Route",
    "Router",
    "RoutingError",
    "Settlement",
    "ShutdownError",
    "ShutdownPolicy",
    "SqsManagedEncryption",
    "Subscription",
    "SubscriptionInfo",
    "Telemetry",
    "ThrottledError",
    "Timeouts",
    "Topic",
    "TopicAttributes",
    "TopicInfo",
    "TransportError",
    "ValidationError",
    "WireMessage",
    "__version__",
    "codecs",
    "envelopes",
    "idempotency",
    "matchers",
    "message_type",
    "new_message_id",
    "outbox",
    "testing",
]

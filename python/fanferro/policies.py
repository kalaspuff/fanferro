"""Consumer policies: retries, dead-lettering, shutdown, heartbeats and rate limits.

Failure handling, in order of precedence for a handler error:

1. A handler action exception (:class:`fanferro.Retry`, :class:`fanferro.Requeue`,
   :class:`fanferro.Reject`, :class:`fanferro.Discard`) decides explicitly.
2. :attr:`RetryPolicy.dead_letter_on` exceptions are dead-lettered immediately.
3. Otherwise, if the attempt number reached :attr:`RetryPolicy.max_attempts`, the message is
   dead-lettered ("exhausted"); else it is retried after the :class:`Backoff` delay, either by
   changing its visibility (``strategy="visibility"``: cheap, keeps the SQS receive count, delays up
   to 12 hours) or by sending a copy with an attempt counter (``strategy="requeue"``: resets SQS'
   receive count so a queue ``RedrivePolicy`` never cuts a retry sequence short; delays up to 15
   minutes on standard queues, none on FIFO queues).
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from fanferro._utils import Duration, seconds

__all__ = [
    "Backoff",
    "DeadLetterPolicy",
    "Heartbeat",
    "RateLimit",
    "RetryPolicy",
    "ShutdownPolicy",
]


@dataclass(frozen=True, slots=True)
class Backoff:
    """Retry delays: ``initial * multiplier ** (attempt - 1)``, capped at ``maximum``.

    Attributes:
        initial: Delay before the first retry (seconds).
        maximum: Maximum delay (seconds; visibility retries are capped at 12 hours, requeues with a
            delay at 15 minutes).
        multiplier: Growth factor per attempt (``1.0`` = constant delay).
        jitter: Randomize delays between 50% and 100% of the computed value (avoids synchronized
            retries after an outage).
    """

    initial: float = 1.0
    maximum: float = 300.0
    multiplier: float = 2.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.initial < 0 or self.maximum < 0 or self.multiplier < 1:
            raise ValueError("backoff delays must be non-negative and the multiplier at least 1")

    @classmethod
    def exponential(cls, initial: Duration = 1.0, maximum: Duration = 300.0, multiplier: float = 2.0) -> Backoff:
        """Exponential backoff with jitter."""
        return cls(seconds(initial, "initial"), seconds(maximum, "maximum"), multiplier)

    @classmethod
    def constant(cls, delay: Duration) -> Backoff:
        """The same delay for every retry."""
        value = seconds(delay, "delay")
        return cls(value, value, 1.0, jitter=False)

    @classmethod
    def immediate(cls) -> Backoff:
        """Retry as soon as possible (the message becomes visible again immediately)."""
        return cls(0.0, 0.0, 1.0, jitter=False)

    def delay(self, attempt: int) -> float:
        """The delay before retrying after attempt number ``attempt`` (1-based)."""
        base = min(self.maximum, self.initial * self.multiplier ** max(0, attempt - 1))
        if self.jitter and base > 0:
            return base * (0.5 + random.random() / 2)
        return base


type ErrorClassifier = Callable[[BaseException], Literal["retry", "dead_letter", "discard"] | None]
"""Custom error classification; ``None`` falls through to the default rules."""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How failed messages are retried.

    Attributes:
        max_attempts: Processing attempts before a message is dead-lettered (``None`` = retry until
            SQS' own redrive policy moves it). Counts SQS receives plus requeues.
        backoff: Delays between attempts.
        strategy: ``"visibility"`` changes the message visibility (cheap, keeps SQS receive count) -
            ``"requeue"`` sends a copy with ``fanferro.attempt`` and deletes the original.
        retry_on: Exceptions considered transient (others are dead-lettered immediately when
            ``dead_letter_unknown`` is set; by default every exception is retried).
        dead_letter_on: Exceptions that are never retried.
        classify: Custom classification hook evaluated first.
        dead_letter_unknown: Dead-letter exceptions not listed in ``retry_on`` immediately.
    """

    max_attempts: int | None = 5
    backoff: Backoff = field(default_factory=Backoff)
    strategy: Literal["visibility", "requeue"] = "visibility"
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    dead_letter_on: tuple[type[BaseException], ...] = ()
    classify: ErrorClassifier | None = None
    dead_letter_unknown: bool = False

    def __post_init__(self) -> None:
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1 (or None)")

    def decide(self, error: BaseException) -> Literal["retry", "dead_letter", "discard"]:
        """Classifies an error raised by a handler."""
        if self.classify is not None:
            decision = self.classify(error)
            if decision is not None:
                return decision
        if self.dead_letter_on and isinstance(error, self.dead_letter_on):
            return "dead_letter"
        if isinstance(error, self.retry_on):
            return "retry"
        return "dead_letter" if self.dead_letter_unknown else "retry"

    def exhausted(self, attempt: int) -> bool:
        """Whether ``attempt`` was the last allowed attempt."""
        return self.max_attempts is not None and attempt >= self.max_attempts


type FailureAction = Literal["dead_letter", "retry", "discard", "leave"]
"""What to do with a message: forward to the dead-letter queue, retry with backoff, delete it, or
leave it (its visibility times out and SQS redrive applies)."""


@dataclass(frozen=True, slots=True)
class DeadLetterPolicy:
    """Where dead-lettered messages go and what happens to messages that cannot be processed.

    Attributes:
        queue: Dead-letter queue name. Defaults to the queue's ``DeadLetterQueue`` declaration or the
            target of its ``RedrivePolicy``.
        on_exhausted: Action when retries are exhausted (``"leave"`` lets SQS redrive do it).
        on_decode_error: Action for messages that cannot be decoded (bad envelope or body).
        on_unroutable: Action for messages no route matches.
        on_missing_queue: Action when dead-lettering is requested but no dead-letter queue is known.
        include_error: Add the exception type and message to the ``fanferro.dead-letter`` attribute.
    """

    queue: str | None = None
    on_exhausted: FailureAction = "dead_letter"
    on_decode_error: FailureAction = "dead_letter"
    on_unroutable: FailureAction = "dead_letter"
    on_missing_queue: Literal["retry", "leave", "discard"] = "leave"
    include_error: bool = True


@dataclass(frozen=True, slots=True)
class ShutdownPolicy:
    """Graceful shutdown of a consumer.

    1. Receiving stops. A long poll that is in flight is allowed to finish (at most the consumer's
       ``wait_time``, overlapping with step 2) and whatever it returns is released immediately. With
       ``abort_receives=True`` it is aborted instead: shutdown is faster, but SQS keeps serving the
       abandoned poll and may hide a message in it until that message's visibility timeout expires.
       Prefetched messages that have not started are released (made visible) when
       ``release_prefetched`` is set, otherwise they are processed as part of the drain.
    2. In-flight handlers get ``drain_timeout`` seconds to finish (``None`` waits forever); a
       ``consumer.draining`` event reports progress every ``progress_interval`` seconds.
    3. Still-running handlers are cancelled and get ``cancel_timeout`` seconds to unwind.
    4. Unfinished messages are released (``release_unfinished``) or left to time out.
    """

    drain_timeout: float | None = 30.0
    cancel_timeout: float = 10.0
    progress_interval: float = 5.0
    release_unfinished: bool = True
    release_prefetched: bool = True
    abort_receives: bool = False

    def __post_init__(self) -> None:
        if self.drain_timeout is not None and self.drain_timeout < 0:
            raise ValueError("drain_timeout must be non-negative or None")
        if self.cancel_timeout < 0 or self.progress_interval <= 0:
            raise ValueError("cancel_timeout must be non-negative and progress_interval positive")


@dataclass(frozen=True, slots=True)
class Heartbeat:
    """Visibility heartbeats that keep messages invisible while they are processed.

    Attributes:
        interval: Seconds between extensions (default: a third of the visibility timeout).
        extend_by: Visibility timeout set by each extension (default: the visibility timeout).
        max_duration: Stop extending after this long (default and maximum: 12 hours after receipt);
            the message then becomes visible again when its visibility runs out.
    """

    interval: Duration | None = None
    extend_by: Duration | None = None
    max_duration: Duration | None = None

    def to_core(self) -> dict[str, object]:
        """The core representation."""
        return {
            "enabled": True,
            "interval": seconds(self.interval, "heartbeat.interval") if self.interval is not None else None,
            "extend_by": seconds(self.extend_by, "heartbeat.extend_by") if self.extend_by is not None else None,
            "max_lease": seconds(self.max_duration, "heartbeat.max_duration")
            if self.max_duration is not None
            else None,
        }


@dataclass(frozen=True, slots=True)
class RateLimit:
    """Limits how many messages per second a consumer receives.

    Attributes:
        per_second: Sustained rate.
        burst: Bucket size (defaults to ``per_second`` rounded up).
    """

    per_second: float
    burst: int | None = None

    def __post_init__(self) -> None:
        if self.per_second <= 0:
            raise ValueError("per_second must be positive")
        if self.burst is not None and self.burst < 1:
            raise ValueError("burst must be at least 1")

"""Matchers: predicates that route messages to handlers.

Matchers look at a message before its body is decoded: attributes, envelope headers, the message
type, the SNS topic, or fields of a JSON body. Build them from fields and combine them with ``&``,
``|`` and ``~``::

    from fanferro import matchers as m

    router.route(OrderV2, when=m.header("version") >= 2)
    router.route(when=m.attribute("tenant").is_in("acme", "globex") & ~m.body("test").exists())
    router.route(when=(m.topic() == "orders") | m.message_type().startswith("orders."))

Parenthesize comparisons when combining them: ``&`` and ``|`` bind tighter than ``==``.
Any callable ``(Message) -> bool`` is accepted wherever a matcher is expected.
"""

from __future__ import annotations

import operator
import re
from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from fanferro.errors import DecodeError

if TYPE_CHECKING:
    from fanferro.messages import Message

__all__ = [
    "MISSING",
    "Field",
    "Matcher",
    "all_of",
    "any_of",
    "as_matcher",
    "attribute",
    "body",
    "content_type",
    "envelope",
    "group_id",
    "header",
    "message_type",
    "predicate",
    "source",
    "subject",
    "topic",
]


class _Missing:
    def __repr__(self) -> str:
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Any = _Missing()
"""Returned by field getters when the field is absent."""

type MessagePredicate = Callable[[Message[Any]], bool]


class Matcher:
    """A named predicate over a message. Combine with ``&``, ``|`` and ``~``."""

    __slots__ = ("_description", "_predicate")

    def __init__(self, predicate: MessagePredicate, description: str) -> None:
        self._predicate = predicate
        self._description = description

    def __call__(self, message: Message[Any]) -> bool:
        """Evaluates the matcher."""
        return bool(self._predicate(message))

    def __and__(self, other: Matcher | MessagePredicate) -> Matcher:
        right = as_matcher(other)
        return Matcher(lambda message: self(message) and right(message), f"({self} & {right})")

    def __or__(self, other: Matcher | MessagePredicate) -> Matcher:
        right = as_matcher(other)
        return Matcher(lambda message: self(message) or right(message), f"({self} | {right})")

    def __invert__(self) -> Matcher:
        return Matcher(lambda message: not self(message), f"~{self}")

    def __repr__(self) -> str:
        return self._description


def as_matcher(value: Matcher | MessagePredicate) -> Matcher:
    """Wraps a plain callable into a :class:`Matcher`."""
    if isinstance(value, Matcher):
        return value
    if not callable(value):
        raise TypeError(f"expected a matcher or a callable, got {type(value).__name__}")
    return Matcher(value, getattr(value, "__qualname__", repr(value)))


def _coerce_equal(left: Any, right: Any) -> bool:
    if left is MISSING:
        return False
    if left == right:
        return True
    numeric = (int, float, Decimal)
    if (
        isinstance(left, numeric)
        and isinstance(right, numeric)
        and not isinstance(left, bool)
        and not isinstance(right, bool)
    ):
        # By value, as the ordering comparisons: Number attributes are Decimals, and
        # Decimal("0.1") == 0.1 is false in Python (the float is not exactly 0.1).
        return bool(_number(left) == _number(right))
    if isinstance(left, str) and isinstance(right, numeric) and not isinstance(right, bool):
        return left == str(right)
    if isinstance(right, str) and isinstance(left, numeric) and not isinstance(left, bool):
        return str(left) == right
    if isinstance(left, bool) or isinstance(right, bool):
        return str(left).lower() == str(right).lower()
    return False


def _number(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return Decimal(value)
        except ArithmeticError:
            return value
    if isinstance(value, float):
        return Decimal(repr(value))
    return value


def _compare(op: Callable[[Any, Any], bool]) -> Callable[[Any, Any], bool]:
    def compare(left: Any, right: Any) -> bool:
        if left is MISSING or left is None:
            return False
        try:
            return bool(op(_number(left), _number(right)))
        except (TypeError, ArithmeticError):  # incomparable types, NaN (decimal.InvalidOperation)
            return False

    return compare


class Field:
    """A message field; comparisons produce :class:`Matcher` objects."""

    __slots__ = ("_description", "_getter")
    __hash__ = None  # type: ignore[assignment] - == builds matchers

    def __init__(self, getter: Callable[[Message[Any]], Any], description: str) -> None:
        self._getter = getter
        self._description = description

    def value(self, message: Message[Any]) -> Any:
        """The field value (or :data:`MISSING`)."""
        try:
            return self._getter(message)
        except (KeyError, IndexError, TypeError, ValueError, DecodeError):
            return MISSING

    def _matcher(self, test: Callable[[Any], bool], description: str) -> Matcher:
        return Matcher(lambda message: test(self.value(message)), f"{self._description} {description}")

    def __eq__(self, other: object) -> Matcher:  # type: ignore[override]
        return self._matcher(lambda value: _coerce_equal(value, other), f"== {other!r}")

    def __ne__(self, other: object) -> Matcher:  # type: ignore[override]
        return self._matcher(lambda value: value is not MISSING and not _coerce_equal(value, other), f"!= {other!r}")

    def __lt__(self, other: Any) -> Matcher:
        return self._matcher(lambda value: _compare(operator.lt)(value, other), f"< {other!r}")

    def __le__(self, other: Any) -> Matcher:
        return self._matcher(lambda value: _compare(operator.le)(value, other), f"<= {other!r}")

    def __gt__(self, other: Any) -> Matcher:
        return self._matcher(lambda value: _compare(operator.gt)(value, other), f"> {other!r}")

    def __ge__(self, other: Any) -> Matcher:
        return self._matcher(lambda value: _compare(operator.ge)(value, other), f">= {other!r}")

    def is_in(self, *values: Any) -> Matcher:
        """The value equals one of ``values``."""
        return self._matcher(lambda value: any(_coerce_equal(value, v) for v in values), f"in {values!r}")

    def exists(self) -> Matcher:
        """The field is present."""
        return self._matcher(lambda value: value is not MISSING and value is not None, "exists")

    def missing(self) -> Matcher:
        """The field is absent."""
        return self._matcher(lambda value: value is MISSING or value is None, "is missing")

    def startswith(self, prefix: str) -> Matcher:
        """The (text) value starts with ``prefix``."""
        return self._matcher(
            lambda value: isinstance(value, str) and value.startswith(prefix), f"startswith {prefix!r}"
        )

    def endswith(self, suffix: str) -> Matcher:
        """The (text) value ends with ``suffix``."""
        return self._matcher(lambda value: isinstance(value, str) and value.endswith(suffix), f"endswith {suffix!r}")

    def contains(self, item: Any) -> Matcher:
        """The value (text, list or mapping) contains ``item``."""

        def test(value: Any) -> bool:
            try:
                return value is not MISSING and value is not None and item in value
            except TypeError:
                return False

        return self._matcher(test, f"contains {item!r}")

    def matches(self, pattern: str | re.Pattern[str]) -> Matcher:
        """The (text) value matches the regular expression (``re.search``)."""
        compiled = re.compile(pattern) if isinstance(pattern, str) else pattern
        return self._matcher(
            lambda value: isinstance(value, str) and compiled.search(value) is not None, f"matches {compiled.pattern!r}"
        )

    def test(self, function: Callable[[Any], bool], description: str = "satisfies predicate") -> Matcher:
        """A custom test on the value."""
        return self._matcher(lambda value: value is not MISSING and bool(function(value)), description)

    def __repr__(self) -> str:
        return self._description


def attribute(name: str) -> Field:
    """A message attribute (``str``, ``int``/``Decimal`` for numbers, ``bytes`` for binary)."""
    return Field(lambda message: message.attribute(name, MISSING), f"attribute({name!r})")


def header(name: str) -> Field:
    """An envelope header (CloudEvents extension), falling back to the attribute of the same name."""

    def get(message: Message[Any]) -> Any:
        headers = message.headers
        if name in headers:
            return headers[name]
        return message.attribute(name, MISSING)

    return Field(get, f"header({name!r})")


def body(path: str) -> Field:
    """A field of a JSON body by dotted path (``"order.items.0.sku"``); ``""`` is the whole body."""
    parts = [part for part in path.split(".") if part]

    def get(message: Message[Any]) -> Any:
        value: Any = message.json()
        for part in parts:
            if isinstance(value, list):
                value = value[int(part)]
            elif isinstance(value, dict):
                if part not in value:
                    return MISSING
                value = value[part]
            else:
                return MISSING
        return value

    return Field(get, f"body({path!r})")


def message_type() -> Field:
    """The message type (envelope ``type`` or the ``fanferro.type`` attribute)."""
    return Field(lambda message: message.type if message.type is not None else MISSING, "message_type()")


def content_type() -> Field:
    """The body media type without parameters (``application/json``)."""

    def get(message: Message[Any]) -> Any:
        value = message.content_type
        return value.split(";", 1)[0].strip().lower() if value else MISSING

    return Field(get, "content_type()")


def source() -> Field:
    """The CloudEvents ``source``."""
    return Field(lambda message: message.source if message.source is not None else MISSING, "source()")


def subject() -> Field:
    """The CloudEvents ``subject``."""
    return Field(lambda message: message.subject if message.subject is not None else MISSING, "subject()")


def envelope() -> Field:
    """The envelope name (``cloudevents``, ``cloudevents-protobuf``, ``raw`` or a custom one)."""
    return Field(lambda message: message.envelope, "envelope()")


def group_id() -> Field:
    """The FIFO message group id."""
    return Field(lambda message: message.group_id if message.group_id is not None else MISSING, "group_id()")


class _TopicField(Field):
    """Topic ARN; ``==`` also accepts the bare topic name."""

    def __eq__(self, other: object) -> Matcher:  # type: ignore[override]
        def test(value: Any) -> bool:
            if not isinstance(value, str):
                return False
            return value == other or value.rsplit(":", 1)[-1] == other

        return self._matcher(test, f"== {other!r}")


def topic() -> Field:
    """The SNS topic ARN the message was published to.

    ``== "name"`` matches the topic's AWS name too, which includes a ``topic_prefix``: compare with
    ``topic.name`` of the declared topic.
    """
    return _TopicField(lambda message: message.topic_arn if message.topic_arn is not None else MISSING, "topic()")


def predicate(function: MessagePredicate, description: str | None = None) -> Matcher:
    """A matcher from any ``(Message) -> bool`` function."""
    return Matcher(function, description or getattr(function, "__qualname__", "predicate"))


def any_of(*matchers: Matcher | MessagePredicate) -> Matcher:
    """True when any matcher is true."""
    items = [as_matcher(m) for m in matchers]
    return Matcher(lambda message: any(m(message) for m in items), f"any_of({', '.join(map(repr, items))})")


def all_of(*matchers: Matcher | MessagePredicate) -> Matcher:
    """True when every matcher is true."""
    items = [as_matcher(m) for m in matchers]
    return Matcher(lambda message: all(m(message) for m in items), f"all_of({', '.join(map(repr, items))})")

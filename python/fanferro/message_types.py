"""Message type names: the logical name of a message class on the wire.

The message type travels in the envelope (CloudEvents ``type``) and, by default, in the
``fanferro.type`` attribute so routers and SNS filter policies can use it. Name resolution for
a class, in order:

1. names registered with :meth:`MessageTypes.register`,
2. the ``@message_type("...")`` decorator (``__fanferro_type__``),
3. a codec-provided canonical name (protobuf: ``DESCRIPTOR.full_name``),
4. the naming function (default: the class ``__name__``).

Prefer explicit, versioned names for anything that crosses service boundaries::

    @fanferro.message_type("orders.order_created.v1")
    class OrderCreated(BaseModel): ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from fanferro.codecs import CodecRegistry

__all__ = ["MessageTypes", "message_type"]

_ATTRIBUTE = "__fanferro_type__"
_ALIASES = "__fanferro_type_aliases__"


def message_type[C: type](name: str, *aliases: str) -> Callable[[C], C]:
    """Class decorator assigning a message type name (and optional accepted aliases)."""
    if not name:
        raise ValueError("message type names must not be empty")

    def decorate(cls: C) -> C:
        setattr(cls, _ATTRIBUTE, name)
        setattr(cls, _ALIASES, tuple(aliases))
        return cls

    return decorate


def _default_naming(cls: type) -> str:
    return cls.__name__


class MessageTypes:
    """Maps message classes to type names and back.

    Args:
        codecs: Used to look up codec-provided names (protobuf full names).
        naming: Fallback naming function for classes without an explicit name.
    """

    def __init__(self, codecs: CodecRegistry | None = None, naming: Callable[[type], str] | None = None) -> None:
        self._codecs = codecs
        self._naming = naming or _default_naming
        self._names: dict[type, str] = {}
        self._classes: dict[str, type] = {}

    def register(self, cls: type, name: str, *aliases: str) -> None:
        """Registers ``cls`` under ``name`` (and ``aliases`` for decoding)."""
        self._names[cls] = name
        for alias in (name, *aliases):
            self._classes[alias] = cls

    def name_of(self, target: Any) -> str:
        """The type name of a class or instance."""
        cls = target if isinstance(target, type) else type(target)
        if cls in self._names:
            return self._names[cls]
        explicit = cls.__dict__.get(_ATTRIBUTE)
        if isinstance(explicit, str):
            return explicit
        if self._codecs is not None:
            name = self._codecs.type_name(cls)
            if name:
                return name
        return self._naming(cls)

    def names_of(self, cls: type) -> tuple[str, ...]:
        """The primary name and every alias accepted for ``cls``."""
        primary = self.name_of(cls)
        aliases = tuple(name for name, registered in self._classes.items() if registered is cls and name != primary)
        decorated = cls.__dict__.get(_ALIASES, ())
        return (primary, *aliases, *(alias for alias in decorated if alias not in aliases))

    def class_for(self, name: str) -> type | None:
        """The class registered for ``name``."""
        return self._classes.get(name)

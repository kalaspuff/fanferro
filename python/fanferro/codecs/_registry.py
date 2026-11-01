"""Ordered codec lookup."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fanferro.codecs._base import Codec, EncodedBody, media_type
from fanferro.codecs._dataclass import DataclassCodec
from fanferro.codecs._json import JsonCodec
from fanferro.codecs._msgspec import MsgspecCodec
from fanferro.codecs._protobuf import ProtobufCodec
from fanferro.codecs._pydantic import PydanticCodec
from fanferro.codecs._raw import BytesCodec, TextCodec
from fanferro.errors import ConfigurationError, DecodeError, EncodeError

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["CodecRegistry", "default_codecs"]


_BUILTIN_CODECS: frozenset[type] = frozenset(
    {ProtobufCodec, PydanticCodec, MsgspecCodec, DataclassCodec, BytesCodec, TextCodec, JsonCodec}
)


def default_codecs() -> list[Codec]:
    """The built-in codecs in lookup order.

    Library-specific codecs (protobuf, pydantic, msgspec) are always present but only match when
    the library is imported, so fanferro has no required dependencies.
    """
    return [ProtobufCodec(), PydanticCodec(), MsgspecCodec(), DataclassCodec(), BytesCodec(), TextCodec(), JsonCodec()]


class CodecRegistry:
    """Finds the codec for a value (encoding) or a target type (decoding).

    Args:
        codecs: Codecs in lookup order; defaults to :func:`default_codecs`.

    Example::

        registry = CodecRegistry()
        registry.register(MyAvroCodec())  # tried before the built-ins
    """

    def __init__(self, codecs: Iterable[Codec] | None = None) -> None:
        self._codecs: list[Codec] = list(codecs) if codecs is not None else default_codecs()
        self._decoders: dict[tuple[Any, str | None], Codec] = {}
        self._encoders: dict[type, Codec] | None = {} if self._type_based() else None
        self._generation = 0

    def _type_based(self) -> bool:
        """Whether every codec decides ``can_encode`` from the value's type alone.

        The built-in codecs do, so the encoder found for a type is reused for every value of
        that type; a custom codec may look at values, so it disables the cache.
        """
        return all(type(codec) in _BUILTIN_CODECS for codec in self._codecs)

    @property
    def generation(self) -> int:
        """Incremented whenever a codec is registered (lets dependent caches refresh)."""
        return self._generation

    @property
    def codecs(self) -> tuple[Codec, ...]:
        """Registered codecs in lookup order."""
        return tuple(self._codecs)

    def register(self, codec: Codec, *, first: bool = True) -> None:
        """Adds a codec (before the existing ones unless ``first=False``)."""
        if first:
            self._codecs.insert(0, codec)
        else:
            self._codecs.append(codec)
        self._decoders.clear()
        self._encoders = {} if self._type_based() else None
        self._generation += 1

    def copy(self) -> CodecRegistry:
        """An independent copy."""
        return CodecRegistry(self._codecs)

    def by_name(self, name: str) -> Codec:
        """The registered codec called ``name``.

        Raises:
            ConfigurationError: No codec of that name is registered (the message lists those that are).
        """
        for codec in self._codecs:
            if codec.name == name:
                return codec
        names = ", ".join(repr(codec.name) for codec in self._codecs)
        raise ConfigurationError(f"no codec named {name!r}; the registered codecs are {names}")

    def encoder_for(self, value: object) -> Codec:
        """The first codec that can encode ``value``."""
        encoders = self._encoders
        if encoders is not None:
            cached = encoders.get(type(value))
            if cached is not None:
                return cached
        for codec in self._codecs:
            if codec.can_encode(value):
                if encoders is not None:
                    encoders[type(value)] = codec
                return codec
        raise EncodeError(f"no codec can encode values of type {type(value).__name__}; register a codec for it")

    def encode(self, value: object, codec: Codec | None = None) -> EncodedBody:
        """Encodes ``value`` with ``codec`` or the first matching codec."""
        return (codec or self.encoder_for(value)).encode(value)

    def decoder_for(self, target: Any, content_type: str | None) -> Codec:
        """The first codec that can decode ``content_type`` data into ``target``."""
        key = (target, media_type(content_type))
        try:
            cached = self._decoders.get(key)
        except TypeError:  # unhashable typing construct
            cached = None
            key = None  # type: ignore[assignment]
        if cached is not None:
            return cached
        for codec in self._codecs:
            if codec.can_decode(target, content_type):
                if key is not None:
                    self._decoders[key] = codec
                return codec
        raise DecodeError(
            f"no codec can decode {content_type or 'untyped'} data into {getattr(target, '__name__', target)}"
        )

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        """Decodes ``data`` into ``target``."""
        return self.decoder_for(target, content_type).decode(data, target, content_type)

    def __repr__(self) -> str:
        return f"CodecRegistry({[codec.name for codec in self._codecs]!r})"

    def type_name(self, target: Any) -> str | None:
        """A canonical message type name for ``target`` provided by a codec, if any."""
        for codec in self._codecs:
            name = codec.type_name(target)
            if name:
                return name
        return None

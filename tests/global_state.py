"""Process-wide state that tests must leave as they found it (#168).

OpenTelemetry lets a process set its tracer and meter providers once, and every client created
later traces, records metrics and propagates context through them; the root logger's level and
handlers decide what every later test logs. OpenTelemetry has no public way to read or reset its
global providers, so this reads and writes its private module state.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from opentelemetry import trace
from opentelemetry.metrics import _internal as metrics


def _own_handlers(logger: logging.Logger) -> list[logging.Handler]:
    """The handlers of ``logger`` that pytest did not add (it adds its own around each phase)."""
    return [handler for handler in logger.handlers if not type(handler).__module__.startswith("_pytest")]


@dataclass(frozen=True)
class GlobalState:
    """A snapshot of the state tests share."""

    tracer_provider: Any
    tracer_provider_set: bool
    meter_provider: Any
    meter_provider_set: bool
    root_level: int
    root_handlers: tuple[logging.Handler, ...]

    @classmethod
    def capture(cls) -> GlobalState:
        root = logging.getLogger()
        return cls(
            tracer_provider=trace._TRACER_PROVIDER,
            tracer_provider_set=trace._TRACER_PROVIDER_SET_ONCE._done,
            meter_provider=metrics._METER_PROVIDER,
            meter_provider_set=metrics._METER_PROVIDER_SET_ONCE._done,
            root_level=root.level,
            root_handlers=tuple(_own_handlers(root)),
        )

    def changes(self, other: GlobalState) -> list[str]:
        """What differs in ``other`` (empty if nothing)."""
        changed = []
        if (other.tracer_provider, other.tracer_provider_set) != (self.tracer_provider, self.tracer_provider_set):
            changed.append(f"the global tracer provider was set to {other.tracer_provider!r}")
        if (other.meter_provider, other.meter_provider_set) != (self.meter_provider, self.meter_provider_set):
            changed.append(f"the global meter provider was set to {other.meter_provider!r}")
        if other.root_level != self.root_level:
            changed.append(f"the root logger's level changed to {logging.getLevelName(other.root_level)}")
        if other.root_handlers != self.root_handlers:
            changed.append(f"the root logger's handlers changed to {list(other.root_handlers)!r}")
        return changed

    def restore(self) -> None:
        trace._TRACER_PROVIDER = self.tracer_provider
        trace._TRACER_PROVIDER_SET_ONCE._done = self.tracer_provider_set
        metrics._METER_PROVIDER = self.meter_provider
        metrics._METER_PROVIDER_SET_ONCE._done = self.meter_provider_set
        root = logging.getLogger()
        root.setLevel(self.root_level)
        for handler in _own_handlers(root):
            if handler not in self.root_handlers:
                root.removeHandler(handler)
                handler.close()
        for handler in self.root_handlers:
            if handler not in root.handlers:
                root.addHandler(handler)


@contextlib.contextmanager
def restored_global_state() -> Iterator[None]:
    """Runs code that may change the shared state (an example script) and restores it after."""
    state = GlobalState.capture()
    try:
        yield
    finally:
        state.restore()

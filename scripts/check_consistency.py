"""Checks that code, stubs and documentation agree. Run by CI and ``make check``.

    uv run python scripts/check_consistency.py

Each check prints what to fix. Add a check here whenever a new "remember to also update X" rule
appears, so agents and humans cannot forget it.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import pkgutil
import re
import sys
import tomllib
from collections.abc import Callable, Iterator
from pathlib import Path

import fanferro
from fanferro import _core, errors
from fanferro.client import Client
from fanferro.consumer import Consumer
from fanferro.events import EventKind

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import generate_reference  # noqa: E402 - after sys.path setup

Check = Callable[[], Iterator[str]]
CHECKS: list[Check] = []


def check(function: Check) -> Check:
    CHECKS.append(function)
    return function


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


@check
def generated_references() -> Iterator[str]:
    if read("docs/reference/events.md") != generate_reference.render_events():
        yield "docs/reference/events.md is stale: run `uv run python scripts/generate_reference.py`"
    if read("docs/reference/attributes.md") != generate_reference.render_attributes():
        yield "docs/reference/attributes.md is stale: run `uv run python scripts/generate_reference.py`"


@check
def event_kinds_match() -> Iterator[str]:
    rust = [name for name, _, _ in _core.event_kinds()]
    python = [kind.value for kind in EventKind]
    if rust != python:
        missing = sorted(set(rust) - set(python))
        extra = sorted(set(python) - set(rust))
        yield (
            "EventKind differs between crates/fanferro-core/src/events.rs and python/fanferro/events.py "
            f"(order matters): missing in Python {missing}, missing in Rust {extra}"
        )


@check
def stub_matches_native_module() -> Iterator[str]:
    tree = ast.parse(read("python/fanferro/_core.pyi"))
    names = {node.name for node in tree.body if isinstance(node, ast.ClassDef | ast.FunctionDef)}
    names |= {
        node.target.id for node in tree.body if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    names -= {"AttributeInput", "HeaderValue"}
    runtime = {name for name in dir(_core) if not name.startswith("__") or name == "__version__"}
    for name in sorted(runtime - names):
        yield f"python/fanferro/_core.pyi is missing `{name}` (exported by crates/fanferro-py)"
    for name in sorted(names - runtime):
        yield f"python/fanferro/_core.pyi declares `{name}`, which the native module does not export"
    for class_name in sorted(names & runtime):
        runtime_object = getattr(_core, class_name)
        if not isinstance(runtime_object, type) or issubclass(runtime_object, BaseException):
            continue
        stub_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
        stub_members = {n.name for n in stub_class.body if isinstance(n, ast.FunctionDef)}
        runtime_members = {m for m in vars(runtime_object) if not m.startswith("_")}
        for member in sorted(runtime_members - stub_members):
            yield f"python/fanferro/_core.pyi: `{class_name}.{member}` is missing"
        for member in sorted(stub_members - runtime_members - {"__init__", "__eq__", "__hash__"}):
            yield f"python/fanferro/_core.pyi: `{class_name}.{member}` does not exist at runtime"


def sections(markdown: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    current = ""
    for line in markdown.splitlines():
        if line.startswith("## "):
            current = line[3:].strip()
            parts[current] = ""
        else:
            parts[current] = parts.get(current, "") + line + "\n"
    return parts


def parameters(function: Callable[..., object], skip: set[str]) -> list[str]:
    return [name for name in inspect.signature(function).parameters if name not in skip | {"self"}]


@check
def configuration_reference() -> Iterator[str]:
    docs = sections(read("docs/reference/configuration.md"))
    expectations = {
        "`Client(...)`": parameters(Client.__init__, set()),
        "`client.topic(...)`": parameters(Client.topic, set()),
        "`client.queue(...)`": parameters(Client.queue, set()),
        "`client.subscription(topic, queue, ...)`": parameters(Client.subscription, {"topic", "queue"}),
        "`client.consumer(queue, handler_or_router, ...)`": parameters(
            Consumer.__init__, {"client", "queue", "router"}
        ),
    }
    for section, names in expectations.items():
        text = docs.get(section)
        if text is None:
            yield f"docs/reference/configuration.md has no section {section}"
            continue
        documented = set(re.findall(r"^\| `([a-z0-9_]+)` \|", text, re.MULTILINE))
        for name in names:
            if name not in documented:
                yield f"docs/reference/configuration.md section {section} does not document `{name}`"
        for name in sorted(documented - set(names)):
            yield f"docs/reference/configuration.md section {section} documents unknown parameter `{name}`"
    policies = docs.get("Policies", "")
    for cls in (
        fanferro.RetryPolicy,
        fanferro.Backoff,
        fanferro.DeadLetterPolicy,
        fanferro.DeadLetterQueue,
        fanferro.ShutdownPolicy,
        fanferro.Heartbeat,
        fanferro.RateLimit,
        fanferro.Provisioning,
        fanferro.LargePayloads,
        fanferro.PayloadFetch,
        fanferro.Telemetry,
        fanferro.Propagation,
        fanferro.LoggingConfig,
        fanferro.AwsRetry,
        fanferro.Timeouts,
    ):
        paragraph = re.search(rf"^`{cls.__name__}`:(.*?)(?:\n\n|\Z)", policies, re.MULTILINE | re.DOTALL)
        if paragraph is None:
            yield f"docs/reference/configuration.md (Policies) does not describe `{cls.__name__}`"
            continue
        for field in dataclasses.fields(cls):
            if f"`{field.name}`" not in paragraph.group(1):
                yield f"docs/reference/configuration.md (Policies): `{cls.__name__}.{field.name}` is not documented"


@check
def error_reference() -> Iterator[str]:
    text = read("docs/reference/errors.md")
    for name in errors.__all__:
        if name == "FanferroError":
            continue
        if f"`{name}`" not in text:
            yield f"docs/reference/errors.md does not document `{name}`"


@check
def submodules_typed() -> Iterator[str]:
    # At runtime every imported submodule is an attribute of the package; type checkers (pyright and
    # Pylance) only see the ones `__init__.py` imports as `from fanferro import m as m` or lists in
    # `__all__`, and a module-level `__getattr__` would make them type every unknown name as `Any`.
    tree = ast.parse(read("python/fanferro/__init__.py"))
    bound = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "fanferro"
        for alias in node.names
        if alias.asname == alias.name or alias.name in fanferro.__all__
    }
    for module in sorted(pkgutil.iter_modules([str(ROOT / "python" / "fanferro")])):
        if not module.name.startswith("_") and module.name not in bound:
            yield f"python/fanferro/__init__.py must import `from fanferro import {module.name} as {module.name}`"
    if any(isinstance(node, ast.FunctionDef) and node.name == "__getattr__" for node in tree.body):
        yield "python/fanferro/__init__.py must define __getattr__ under `if not TYPE_CHECKING:`"


@check
def examples_listed() -> Iterator[str]:
    readme = read("examples/README.md")
    for path in sorted((ROOT / "examples").glob("*.py")):
        if not path.stem.startswith("_") and f"{path.name}" not in readme:
            yield f"examples/README.md does not list {path.name}"


@check
def guides_linked() -> Iterator[str]:
    index = read("docs/index.md")
    for path in sorted((ROOT / "docs" / "guides").glob("*.md")):
        if f"guides/{path.name}" not in index:
            yield f"docs/index.md does not link docs/guides/{path.name}"


@check
def versions_agree() -> Iterator[str]:
    cargo = tomllib.loads(read("Cargo.toml"))
    version = cargo["workspace"]["package"]["version"]
    if fanferro.__version__ != version:
        yield f"installed fanferro {fanferro.__version__} != Cargo.toml {version} (rebuild: `uv sync`)"
    core_dependency = cargo["workspace"]["dependencies"]["fanferro-core"]["version"]
    if core_dependency != version:
        yield f"Cargo.toml workspace dependency fanferro-core = {core_dependency}, expected {version}"
    changelog = read("CHANGELOG.md")
    if "## [Unreleased]" not in changelog and f"## [{version}]" not in changelog:
        yield f"CHANGELOG.md has neither an [Unreleased] section nor a [{version}] section"


@check
def agent_guide_packaged() -> Iterator[str]:
    guide = read("python/fanferro/AGENT_GUIDE.md")
    if len(guide) < 2000:
        yield "python/fanferro/AGENT_GUIDE.md looks empty; it is printed by `fanferro guide`"
    skill = read("skills/fanferro/SKILL.md")
    if "fanferro guide" not in skill:
        yield "skills/fanferro/SKILL.md must point agents to `fanferro guide`"


def main() -> int:
    problems = [problem for function in CHECKS for problem in function()]
    for problem in problems:
        print(f"✗ {problem}")
    if problems:
        print(f"\n{len(problems)} consistency problem(s). See AGENTS.md for the rules.")
        return 1
    print(f"✓ {len(CHECKS)} consistency checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

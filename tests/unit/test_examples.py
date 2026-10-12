"""Every example must keep working (they double as documentation)."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

EXAMPLES = sorted(
    path.stem
    for path in (Path(__file__).resolve().parents[2] / "examples").glob("*.py")
    if not path.stem.startswith("_")
)


@pytest.mark.parametrize("example", EXAMPLES)
async def test_example_runs(example: str) -> None:
    module = importlib.import_module(f"examples.{example}")
    await module.main()


def test_examples_are_listed_in_the_readme() -> None:
    readme = (Path(__file__).resolve().parents[2] / "examples" / "README.md").read_text()
    for example in EXAMPLES:
        assert f"{example}.py" in readme, f"examples/README.md does not mention {example}.py"

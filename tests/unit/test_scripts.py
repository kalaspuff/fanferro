"""The repository scripts (consistency checks, release helper, wheel smoke test) keep working."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"scripts_{name}", SCRIPTS / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_code_and_docs_are_consistent(capsys: pytest.CaptureFixture[str]) -> None:
    check_consistency = load("check_consistency")
    assert check_consistency.main() == 0, capsys.readouterr().out


@pytest.mark.parametrize(
    ("current", "requested", "expected"),
    [
        ("0.1.0", "patch", "0.1.1"),
        ("0.1.3", "minor", "0.2.0"),
        ("0.4.2", "major", "0.5.0"),
        ("1.4.2", "major", "2.0.0"),
        ("1.4.2", "minor", "1.5.0"),
        ("0.1.0", "0.3.0", "0.3.0"),
    ],
)
def test_release_version_bumps(current: str, requested: str, expected: str) -> None:
    assert load("release").next_version(current, requested) == expected


def test_release_rejects_invalid_versions() -> None:
    with pytest.raises(SystemExit, match="invalid version"):
        load("release").next_version("0.1.0", "v0.2")


def test_wheel_smoke_test_passes() -> None:
    result = subprocess.run(
        [sys.executable, "-I", str(SCRIPTS / "smoke_test.py")], capture_output=True, text=True, timeout=120, check=False
    )
    assert result.returncode == 0, result.stderr
    assert " OK on " in result.stdout


CHANGELOG = """# Changelog

Intro.

## [Unreleased]

{unreleased}
## [0.2.0] - Unreleased

First release.

## [0.1.0] - 2026-01-01

Old.
"""


@pytest.mark.parametrize("unreleased", ["", "Hand-written note.\n"])
def test_release_dates_a_hand_written_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unreleased: str) -> None:
    release = load("release")
    changelog = tmp_path / "CHANGELOG.md"
    monkeypatch.setattr(release, "CHANGELOG", changelog)
    changelog.write_text(CHANGELOG.format(unreleased=unreleased))
    release.update_changelog("0.2.0", generated="unused")
    text = changelog.read_text()
    assert "## [0.2.0] - Unreleased" not in text
    assert "## [Unreleased]\n\n## [0.2.0] - 2" in text
    notes = load("release_notes").section(text, "0.2.0")
    assert notes == ("Hand-written note.\n\nFirst release." if unreleased else "First release.")


@pytest.mark.parametrize("unreleased", ["", "Hand-written note.\n"])
def test_release_inserts_the_generated_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unreleased: str
) -> None:
    release = load("release")
    changelog = tmp_path / "CHANGELOG.md"
    monkeypatch.setattr(release, "CHANGELOG", changelog)
    changelog.write_text(CHANGELOG.format(unreleased=unreleased).replace("0.2.0", "0.1.1"))
    release.update_changelog("0.3.0", generated="## [0.3.0] - 2026-10-11\n\n### Features\n\n- Thing")
    text = changelog.read_text()
    assert text.index("## [Unreleased]") < text.index("## [0.3.0] - 2026-10-11") < text.index("## [0.1.1]")
    notes = load("release_notes").section(text, "0.3.0")
    expected = "### Features\n\n- Thing"
    assert notes == (f"Hand-written note.\n\n{expected}" if unreleased else expected)
    assert load("release_notes").section(text, "Unreleased") == ""
    with pytest.raises(SystemExit, match="already has a section"):
        release.update_changelog("0.3.0", generated="")


def test_release_notes_of_the_current_version_exist() -> None:
    version = tomllib.loads((SCRIPTS.parent / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    notes = load("release_notes").section((SCRIPTS.parent / "CHANGELOG.md").read_text(), version)
    assert notes, f"CHANGELOG.md needs a section for {version}"

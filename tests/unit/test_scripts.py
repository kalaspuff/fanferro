"""The repository scripts (consistency checks, release helper, wheel smoke test, benchmark) keep working."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"scripts_{name}", SCRIPTS / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look up their module there (Python 3.12)
    spec.loader.exec_module(module)
    return module


def test_code_and_docs_are_consistent(capsys: pytest.CaptureFixture[str]) -> None:
    check_consistency = load("check_consistency")
    assert check_consistency.main() == 0, capsys.readouterr().out


@pytest.mark.parametrize(
    ("edit", "problem"),
    [
        (
            lambda init: init.replace("from fanferro import errors as errors\n", ""),
            "must import `from fanferro import errors as errors`",
        ),
        (
            lambda init: init + "\n\ndef __getattr__(name: str) -> object:\n    raise AttributeError(name)\n",
            "must define __getattr__ under `if not TYPE_CHECKING:`",
        ),
    ],
)
def test_submodules_the_type_checkers_cannot_see_fail_the_checks(
    monkeypatch: pytest.MonkeyPatch, edit: Callable[[str], str], problem: str
) -> None:
    check_consistency = load("check_consistency")
    read = check_consistency.read
    init = edit(read("python/fanferro/__init__.py"))
    assert init != read("python/fanferro/__init__.py")
    monkeypatch.setattr(check_consistency, "read", lambda path: init if path.endswith("__init__.py") else read(path))
    assert [problem in found for found in check_consistency.submodules_typed()] == [True]


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


def test_benchmark_runs() -> None:
    result = subprocess.run(
        [sys.executable, "-I", str(SCRIPTS / "benchmark.py"), "-n", "30", "--json"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert [row["scenario"] for row in rows] == list(load("benchmark").SCENARIOS)
    assert all(row["messages"] == 30 and row["wall_us_per_message"] > 0 for row in rows)


def test_wheel_smoke_test_passes() -> None:
    result = subprocess.run(
        [sys.executable, "-I", str(SCRIPTS / "smoke_test.py")], capture_output=True, text=True, timeout=120, check=False
    )
    assert result.returncode == 0, result.stderr
    assert " OK on " in result.stdout


def test_backend_probe_runs_and_compares(tmp_path: Path) -> None:
    probe = str(SCRIPTS / "probe_backends.py")
    scenarios = ["provision.queue_defaults", "provision.missing_resources", "sqs.roundtrip"]
    only = [argument for name in scenarios for argument in ("--only", f"^{name}$")]
    outputs = [tmp_path / "first.json", tmp_path / "second.json"]
    for output in outputs:
        command = [sys.executable, "-I", probe, "run", "--target", "memory", "--out", str(output), *only]
        result = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
        assert result.returncode == 0, result.stdout + result.stderr
    record = json.loads(outputs[0].read_text(encoding="utf-8"))
    assert sorted(record["scenarios"]) == sorted(scenarios)
    assert {result["status"] for result in record["scenarios"].values()} == {"ok"}
    text = outputs[0].read_text(encoding="utf-8")
    assert "000000000000" not in text, "account ids are normalised"
    assert "ffprobe-" not in text, "resource names are normalised"
    compared = subprocess.run(
        [sys.executable, "-I", probe, "compare", *map(str, outputs)],
        capture_output=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )
    assert compared.returncode == 0, compared.stderr
    assert compared.stdout.rstrip().endswith("differences from memory#0: memory#1 0"), compared.stdout
    policy = load("probe_backends").iam_policy("123456789012")
    assert all("123456789012" in json.dumps(s) for s in policy["Statement"][:2])


@pytest.mark.skipif(sys.platform == "win32" or shutil.which("bash") is None, reason="needs bash")
def test_aws_cli_probe_is_valid_shell() -> None:
    script = str(SCRIPTS / "probe_aws_cli.sh")
    result = subprocess.run(["bash", "-n", script], capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    if shutil.which("shellcheck"):
        result = subprocess.run(["shellcheck", script], capture_output=True, text=True, timeout=60, check=False)
        assert result.returncode == 0, result.stdout


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


def test_release_merges_hand_written_entries_with_generated_ones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Hand-written entries mention their pull requests: git-cliff lists those again, and a group of
    # both (two "Bug fixes" headings) would read badly in the release notes.
    release = load("release")
    changelog = tmp_path / "CHANGELOG.md"
    monkeypatch.setattr(release, "CHANGELOG", changelog)
    unreleased = (
        "### Behaviour changes\n\n- Changed.\n\n### Bug fixes\n\n"
        "- **consumer:** Fixed, as described by hand (#8)\n- Also fixed by hand (#9)\n"
    )
    changelog.write_text(CHANGELOG.format(unreleased=unreleased).replace("0.2.0", "0.1.1"))
    generated = (
        "## [0.3.0] - 2026-10-11\n\n### Features\n\n- Thing (#7)\n\n### Bug fixes\n\n"
        "- **consumer:** Fixed (#8)\n- Also fixed (#9)\n- Another fix (#10) (#10)\n\n"
        "### Documentation\n\n- Docs for the fix (#9)"
    )
    release.update_changelog("0.3.0", generated=generated)
    notes = load("release_notes").section(changelog.read_text(), "0.3.0")
    assert notes == (
        "### Behaviour changes\n\n- Changed.\n\n### Bug fixes\n\n"
        "- **consumer:** Fixed, as described by hand (#8)\n- Also fixed by hand (#9)\n- Another fix (#10) (#10)\n\n"
        "### Features\n\n- Thing (#7)"
    )


def _cliff_commit(message: str, group: str, pr_number: int | None) -> dict[str, object]:
    remote = {
        "username": None,
        "pr_author": None,
        "pr_title": None,
        "pr_number": pr_number,
        "pr_numbers": [pr_number] if pr_number else [],
        "pr_labels": [],
        "is_first_time": False,
    }
    no_remote = {**remote, "pr_number": None, "pr_numbers": []}
    person = {"name": "a", "email": "a@example.com", "timestamp": 0}
    return {
        "id": message,
        "message": message,
        "group": group,
        "scope": None,
        "breaking": False,
        "links": [],
        "conventional": True,
        "merge_commit": False,
        "author": person,
        "committer": person,
        "remote": remote,
        "github": remote,
        **dict.fromkeys(("gitlab", "gitea", "bitbucket", "azure_devops"), no_remote),
    }


@pytest.mark.skipif(shutil.which("uvx") is None, reason="needs uvx to run git-cliff")
def test_changelog_template_lists_each_pull_request_once(tmp_path: Path) -> None:
    # With a GitHub token (as in the "Prepare release" workflow) git-cliff knows each commit's pull
    # request, and the subjects of squash merges end with its number already.
    context = [
        {
            "version": "v0.3.0",
            "timestamp": 0,
            "commits": [
                _cliff_commit("fix the thing (#5)", "<!-- 1 -->Bug fixes", 5),
                _cliff_commit("add the other thing", "<!-- 0 -->Features", 7),
                _cliff_commit("pushed directly", "<!-- 0 -->Features", None),
            ],
            "submodule_commits": {},
            **{key: {"contributors": []} for key in ("github", "gitlab", "gitea", "bitbucket", "azure_devops")},
        }
    ]
    path = tmp_path / "context.json"
    path.write_text(json.dumps(context))
    result = subprocess.run(
        ["uvx", load("release").GIT_CLIFF, "--config", "cliff.toml", "--from-context", str(path), "--strip", "all"],
        cwd=SCRIPTS.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip().partition("\n")[2].strip() == (
        "### Features\n\n- Add the other thing (#7)\n- Pushed directly\n\n### Bug fixes\n\n- Fix the thing (#5)"
    )


def test_release_notes_of_the_current_version_exist() -> None:
    version = tomllib.loads((SCRIPTS.parent / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    notes = load("release_notes").section((SCRIPTS.parent / "CHANGELOG.md").read_text(), version)
    assert notes, f"CHANGELOG.md needs a section for {version}"

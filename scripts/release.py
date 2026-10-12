"""Prepares a release: bumps the workspace version, refreshes Cargo.lock, updates CHANGELOG.md.

    uv run --no-project scripts/release.py 0.2.0
    uv run --no-project scripts/release.py minor      # or patch / major

While the version is 0.x, ``major`` bumps the minor version (0.4.2 → 0.5.0); pass ``1.0.0``
explicitly to leave 0.x.

The changelog section for the new version is generated from conventional commits since the last
tag with git-cliff (``cliff.toml``); notes written by hand under ``## [Unreleased]`` move into it.
If CHANGELOG.md already has a ``## [<version>] - Unreleased`` section (hand-written, e.g. for the
first release), it is dated instead. ``scripts/release_notes.py`` turns the section into the
GitHub release notes, so polish it in the release PR.
"""

from __future__ import annotations

import argparse
import datetime
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CARGO = ROOT / "Cargo.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
GIT_CLIFF = "git-cliff@2.14.2"


def current_version() -> str:
    return tomllib.loads(CARGO.read_text(encoding="utf-8"))["workspace"]["package"]["version"]


def next_version(current: str, requested: str) -> str:
    if re.fullmatch(r"\d+\.\d+\.\d+", requested):
        return requested
    major, minor, patch = (int(part) for part in current.split("."))
    if requested == "major":
        return f"{major + 1}.0.0" if major > 0 else f"0.{minor + 1}.0"
    if requested == "minor":
        return f"{major}.{minor + 1}.0"
    if requested == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise SystemExit(f"invalid version {requested!r}: use X.Y.Z, patch, minor or major")


def bump_cargo(old: str, new: str) -> None:
    text = CARGO.read_text(encoding="utf-8")
    text, count = re.subn(
        r'(\[workspace\.package\][^\[]*?\nversion = ")' + re.escape(old) + '"', rf'\g<1>{new}"', text, count=1
    )
    text, dep_count = re.subn(
        r'(fanferro-core = \{ path = "crates/fanferro-core", version = ")' + re.escape(old) + '"', rf'\g<1>{new}"', text
    )
    if count != 1 or dep_count != 1:
        raise SystemExit("could not update the versions in Cargo.toml")
    CARGO.write_text(text, encoding="utf-8")
    subprocess.run(["cargo", "update", "--workspace"], cwd=ROOT, check=True)


def generated_section(version: str) -> str:
    result = subprocess.run(
        ["uvx", GIT_CLIFF, "--config", "cliff.toml", "--unreleased", "--tag", f"v{version}", "--strip", "all"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def update_changelog(version: str, generated: str | None = None) -> None:
    """Dates the ``[version] - Unreleased`` section or inserts one below ``[Unreleased]``.

    Hand-written notes under ``[Unreleased]`` move to the top of the new section.
    """
    text = CHANGELOG.read_text(encoding="utf-8")
    today = datetime.date.today().isoformat()
    unreleased = re.search(r"^## \[Unreleased\][^\n]*\n(.*?)(?=^## \[|\Z)", text, re.MULTILINE | re.DOTALL)
    if unreleased is None:
        raise SystemExit("CHANGELOG.md needs a '## [Unreleased]' heading")
    manual = unreleased.group(1).strip()
    placeholder = re.search(rf"^## \[{re.escape(version)}\] - Unreleased\n", text, re.MULTILINE)
    if placeholder is not None:
        dated = f"## [{version}] - {today}\n" + (f"\n{manual}\n" if manual else "")
        text = text[: placeholder.start()] + dated + text[placeholder.end() :]
    else:
        if f"## [{version}]" in text:
            raise SystemExit(f"CHANGELOG.md already has a section for {version}")
        generated = generated if generated is not None else generated_section(version)
        heading, _, body = (generated or f"## [{version}] - {today}").partition("\n")
        body = "\n\n".join(part for part in (manual, body.strip()) if part) or "No user-facing changes."
        section = f"{heading}\n\n{body}\n\n"
        text = text[: unreleased.end()] + section + text[unreleased.end() :]
    # The [Unreleased] section is empty again.
    text = re.sub(
        r"^## \[Unreleased\][^\n]*\n.*?(?=^## \[)", "## [Unreleased]\n\n", text, count=1, flags=re.MULTILINE | re.DOTALL
    )
    CHANGELOG.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("version", help="X.Y.Z, patch, minor or major")
    parser.add_argument("--github-output", type=Path, help="append version=... for GitHub Actions")
    parser.add_argument("--skip-changelog", action="store_true")
    args = parser.parse_args()
    old = current_version()
    new = next_version(old, args.version)
    if new != old:
        bump_cargo(old, new)
    if not args.skip_changelog:
        update_changelog(new)
    if args.github_output:
        with args.github_output.open("a") as output:
            output.write(f"version={new}\n")
    print(f"prepared release {new} (was {old})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Prints the CHANGELOG.md section of a version (the GitHub release notes). Fails if it is missing.

    python3 scripts/release_notes.py 0.2.0 > RELEASE_NOTES.md

Standard library only: the release workflow runs it without installing anything.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parents[1] / "CHANGELOG.md"


def section(changelog: str, version: str) -> str | None:
    """Returns the body of the ``## [version] - date`` section, or None if there is none."""
    match = re.search(rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|\Z)", changelog, re.MULTILINE | re.DOTALL)
    return match.group(1).strip() if match else None


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: release_notes.py VERSION", file=sys.stderr)
        return 2
    version = sys.argv[1].removeprefix("v")
    body = section(CHANGELOG.read_text(encoding="utf-8"), version)
    if body is None:
        print(f"CHANGELOG.md has no '## [{version}]' section", file=sys.stderr)
        return 1
    print(body or "No user-facing changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

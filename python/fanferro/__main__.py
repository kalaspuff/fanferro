"""Command line helpers: ``fanferro guide | version | events | attributes``."""

from __future__ import annotations

import argparse
import sys
from importlib import resources

from fanferro import __version__
from fanferro._core import event_kinds, reserved_attributes


def _guide() -> str:
    return resources.files("fanferro").joinpath("AGENT_GUIDE.md").read_text(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Entry point of the ``fanferro`` command."""
    parser = argparse.ArgumentParser(prog="fanferro", description="fanferro: SNS + SQS messaging for Python.")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("guide", help="print the usage guide (written for developers and AI agents)")
    commands.add_parser("version", help="print the installed version")
    commands.add_parser("events", help="list event kinds with their default log levels")
    commands.add_parser("attributes", help="list message attributes fanferro reads or writes")
    args = parser.parse_args(argv)
    if args.command == "guide":
        sys.stdout.write(_guide())
    elif args.command == "version":
        print(__version__)
    elif args.command == "events":
        for name, level, description in event_kinds():
            print(f"{name:<30} {level:<8} {description}")
    elif args.command == "attributes":
        for name in reserved_attributes():
            print(name)
    else:
        parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

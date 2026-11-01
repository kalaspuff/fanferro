"""Command line helpers: ``fanferro guide | version | events | attributes | dlq``."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
from importlib import resources
from typing import TYPE_CHECKING, Any

from fanferro import __version__
from fanferro._core import event_kinds, reserved_attributes

if TYPE_CHECKING:
    from fanferro._core import AttributeValue
    from fanferro.client import Client
    from fanferro.redrive import PeekedMessage, RedriveFailure

_PREVIEW = 200


def _guide() -> str:
    return resources.files("fanferro").joinpath("AGENT_GUIDE.md").read_text(encoding="utf-8")


def _add_dlq_commands(commands: Any) -> None:
    dlq = commands.add_parser(
        "dlq",
        help="inspect or redrive a dead-letter queue",
        description="Inspect a dead-letter queue, or move its messages back to the queues they came from.",
    )
    actions = dlq.add_subparsers(dest="action", required=True)
    connection = argparse.ArgumentParser(add_help=False)
    connection.add_argument("--queue-url", required=True, help="URL of the dead-letter queue")
    connection.add_argument("--region", help="AWS region (default: from the environment)")
    connection.add_argument("--endpoint-url", help="endpoint of an AWS-compatible service (e.g. an emulator)")
    connection.add_argument("--profile", help="AWS profile")
    connection.add_argument("--json", action="store_true", help="print JSON")
    actions.add_parser(
        "stats", parents=[connection], help="approximate numbers of visible, in-flight and delayed messages"
    )
    peek = actions.add_parser(
        "peek",
        parents=[connection],
        help="show messages with why they were dead-lettered (they stay in the queue)",
        description="Show messages with why they were dead-lettered. They are received and made visible again: "
        "each peek counts as a receive of the messages.",
    )
    peek.add_argument("--limit", type=int, default=10, help="most messages to show (default: 10)")
    redrive = actions.add_parser(
        "redrive",
        parents=[connection],
        help="move messages back to the queue they came from (or to --to-url)",
        description="Move messages back to the queue they were dead-lettered from (or to --to-url), without "
        "fanferro's dead-letter metadata and, unless --keep-attempts, with a fresh retry budget.",
    )
    redrive.add_argument("--to-url", help="move every message to this queue instead of its source")
    redrive.add_argument("--limit", type=int, help="most messages to move (default: all)")
    redrive.add_argument("--max-per-second", type=float, help="most messages to move per second (default: no limit)")
    redrive.add_argument(
        "--keep-attempts",
        action="store_true",
        help="keep fanferro.attempt (requeue retries) so the moved messages do not get a fresh retry budget",
    )


def _client(args: argparse.Namespace) -> Client:
    """The client the ``dlq`` commands use (replaced in tests)."""
    from fanferro.client import Client

    return Client(region=args.region, endpoint_url=args.endpoint_url, profile=args.profile)


def _attribute_json(value: AttributeValue) -> Any:
    raw = value.value
    if isinstance(raw, bytes):
        return {"type": value.data_type, "base64": base64.b64encode(raw).decode()}
    return {"type": value.data_type, "value": raw}


def _peeked_json(message: PeekedMessage) -> dict[str, Any]:
    return {
        "sqs_message_id": message.sqs_message_id,
        "sent_at": message.sent_at.isoformat() if message.sent_at else None,
        "receive_count": message.receive_count,
        "group_id": message.group_id,
        "source": message.source,
        "dead_letter_info": message.dead_letter_info,
        "attributes": {name: _attribute_json(value) for name, value in message.raw_attributes.items()},
        "body": message.body,
    }


def _print_peeked(message: PeekedMessage) -> None:
    info = message.dead_letter_info or {}
    print(message.sqs_message_id)
    sent = message.sent_at.isoformat() if message.sent_at else "?"
    print(f"  sent at:       {sent} (received {message.receive_count} times)")
    if message.group_id:
        print(f"  group:         {message.group_id}")
    print(f"  source:        {message.source or '(unknown)'}")
    if info:
        print(f"  reason:        {info.get('reason', '?')}")
        if info.get("error_type"):
            print(f"  error:         {info['error_type']}: {info.get('error_message', '')}")
    else:
        print("  reason:        (not dead-lettered by fanferro)")
    if message.requeued_attempts:
        print(f"  requeued:      {message.requeued_attempts} attempts")
    preview = message.body if len(message.body) <= _PREVIEW else message.body[:_PREVIEW] + "..."
    print(f"  body:          {preview}")


def _failure_json(failure: RedriveFailure) -> dict[str, Any]:
    return {
        "sqs_message_id": failure.sqs_message_id,
        "destination": failure.destination,
        "error_type": type(failure.error).__name__,
        "error": str(failure.error),
    }


async def _dlq(args: argparse.Namespace) -> int:
    from fanferro.provisioning import Provisioning

    client = _client(args)
    try:
        queue = client.queue(url=args.queue_url, provisioning=Provisioning.existing())
        if args.action == "stats":
            counts = await queue.approximate_count()
            if args.json:
                print(json.dumps(counts))
            else:
                for name, count in counts.items():
                    print(f"{name:<10} {count}")
            return 0
        if args.action == "peek":
            messages = await queue.peek(args.limit)
            if args.json:
                print(json.dumps([_peeked_json(message) for message in messages], indent=2))
            elif not messages:
                print("no visible messages")
            else:
                for message in messages:
                    _print_peeked(message)
            return 0
        target = client.queue(url=args.to_url, provisioning=Provisioning.existing()) if args.to_url else None
        result = await queue.redrive(
            target,
            limit=args.limit,
            max_per_second=args.max_per_second,
            reset_attempts=not args.keep_attempts,
        )
        if args.json:
            print(
                json.dumps(
                    {
                        "moved": result.moved,
                        "destinations": result.destinations,
                        "not_moved": [_failure_json(failure) for failure in result.not_moved],
                        "not_deleted": [_failure_json(failure) for failure in result.not_deleted],
                    },
                    indent=2,
                )
            )
        else:
            print(f"moved {result.moved} message(s)")
            for url, count in result.destinations.items():
                print(f"  {count:>6} to {url}")
            for failure in result.not_moved:
                print(f"not moved: {failure.sqs_message_id}: {type(failure.error).__name__}: {failure.error}")
            for failure in result.not_deleted:
                print(
                    f"moved but not deleted (now in both queues): {failure.sqs_message_id}: "
                    f"{type(failure.error).__name__}: {failure.error}"
                )
        return 0 if result.ok else 1
    finally:
        await client.close()


def main(argv: list[str] | None = None) -> int:
    """Entry point of the ``fanferro`` command."""
    parser = argparse.ArgumentParser(prog="fanferro", description="fanferro: SNS + SQS messaging for Python.")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("guide", help="print the usage guide (written for developers and AI agents)")
    commands.add_parser("version", help="print the installed version")
    commands.add_parser("events", help="list event kinds with their default log levels")
    commands.add_parser("attributes", help="list message attributes fanferro reads or writes")
    _add_dlq_commands(commands)
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
    elif args.command == "dlq":
        from fanferro.errors import FanferroError

        try:
            return asyncio.run(_dlq(args))
        except FanferroError as error:
            print(f"fanferro: {type(error).__name__}: {error}", file=sys.stderr)
            return 2
    else:
        parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

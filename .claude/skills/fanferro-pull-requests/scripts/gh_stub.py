#!/usr/bin/env python3
"""A stub of the GitHub CLI (`gh`) for rehearsing publish kits: pull requests live in a JSON file.

It implements what publish.sh uses (`auth status`; `pr list`, `create`, `view`, `edit`,
`comment`) and `pr merge --squash [--delete-branch]`, which merges like GitHub's "Squash and
merge": one commit on the base branch, titled "TITLE (#N)", with the description as its body.
With --delete-branch the head branch is deleted, and the open pull requests based on it are
retargeted to the merged one's base, as GitHub does. `--jq` filters run through the real `jq`.

Environment: GH_STUB_DB (the JSON file), GH_STUB_REMOTE (the bare repository playing GitHub),
GH_STUB_LOG (optional: a file that records every call), GH_STUB_FAIL_CREATE (a head branch whose
`pr create` fails), GH_STUB_UNAUTHENTICATED (set: `auth status` fails), GH_STUB_HOOK and
GH_STUB_HOOK_MATCH (a shell command run once, at the first call containing the text: someone else
pushing while publish.sh runs), GH_STUB_FIRST_NUMBER (the number of the first pull request,
default 101).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, NoReturn

VALUE_OPTIONS = {"--base", "--body", "--body-file", "--head", "--jq", "--json", "--limit", "--state", "--title"}
FLAG_OPTIONS = {"--delete-branch", "--squash"}


def fail(message: str, code: int = 1) -> NoReturn:
    print(f"gh (stub): {message}", file=sys.stderr)
    raise SystemExit(code)


def parse(args: list[str]) -> tuple[list[str], dict[str, str]]:
    positional: list[str] = []
    options: dict[str, str] = {}
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in VALUE_OPTIONS:
            if index + 1 >= len(args):
                fail(f"{arg} needs a value", 2)
            options[arg] = args[index + 1]
            index += 2
        elif arg in FLAG_OPTIONS:
            options[arg] = "true"
            index += 1
        elif arg.startswith("-"):
            fail(f"unsupported option {arg} (extend gh_stub.py if publish.sh uses it)", 2)
        else:
            positional.append(arg)
            index += 1
    return positional, options


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "--git-dir", os.environ["GH_STUB_REMOTE"], *args], capture_output=True, text=True, check=check
    )


def tip(branch: str) -> str | None:
    result = git("rev-parse", "--verify", "--quiet", f"refs/heads/{branch}^{{commit}}", check=False)
    return result.stdout.strip() or None


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        if path.exists():
            self.data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        else:
            self.data = {"next": int(os.environ.get("GH_STUB_FIRST_NUMBER", "101")), "prs": []}

    @property
    def prs(self) -> list[dict[str, Any]]:
        return self.data["prs"]

    def save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=1) + "\n", encoding="utf-8")

    def find(self, target: str) -> dict[str, Any]:
        if target.isdigit():
            for pr in self.prs:
                if pr["number"] == int(target):
                    return pr
            fail(f"no pull request #{target}")
        matches = sorted((pr for pr in self.prs if pr["head"] == target), key=lambda pr: pr["number"], reverse=True)
        if not matches:
            fail(f"no pull requests found for branch {target!r}")
        return next((pr for pr in matches if pr["state"] == "OPEN"), matches[0])


def fields(pr: dict[str, Any]) -> dict[str, Any]:
    return {
        "number": pr["number"],
        "state": pr["state"],
        "title": pr["title"],
        "body": pr["body"],
        "url": f"https://github.example/stub/pull/{pr['number']}",
        "baseRefName": pr["base"],
        "headRefName": pr["head"],
        "headRefOid": pr["headRefOid"] if pr["state"] != "OPEN" else tip(pr["head"]),
        "comments": [{"body": body} for body in pr["comments"]],
        "mergeCommit": {"oid": pr["mergeCommit"]} if pr.get("mergeCommit") else None,
    }


def output(value: Any, options: dict[str, str]) -> None:
    requested = options.get("--json")
    if requested is None:
        fail("only --json output is supported", 2)
    names = requested.split(",")

    def select(item: dict[str, Any]) -> dict[str, Any]:
        return {name: item[name] for name in names}

    shaped = [select(item) for item in value] if isinstance(value, list) else select(value)
    document = json.dumps(shaped)
    if "--jq" in options:
        result = subprocess.run(
            ["jq", "-r", options["--jq"]], input=document, capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            fail(f"jq: {result.stderr.strip()}", 2)
        sys.stdout.write(result.stdout)
    else:
        print(document)


def read_body(options: dict[str, str]) -> str | None:
    if "--body-file" in options:
        return Path(options["--body-file"]).read_text(encoding="utf-8")
    return options.get("--body")


def merge(store: Store, pr: dict[str, Any], options: dict[str, str]) -> None:
    if "--squash" not in options:
        fail("the stub merges with --squash only", 2)
    if pr["state"] != "OPEN":
        fail(f"#{pr['number']} is {pr['state'].lower()}")
    base_tip, head_tip = tip(pr["base"]), tip(pr["head"])
    if base_tip is None or head_tip is None:
        fail(f"#{pr['number']}: its base or head branch does not exist")
    merged = git("merge-tree", "--write-tree", base_tip, head_tip, check=False)
    if merged.returncode != 0:
        fail(f"#{pr['number']} is not mergeable: the merge conflicts")
    tree = merged.stdout.split()[0]
    message = f"{pr['title']} (#{pr['number']})\n\n{pr['body']}"
    environment = {
        **os.environ,
        "GIT_AUTHOR_NAME": "GitHub (stub)",
        "GIT_AUTHOR_EMAIL": "noreply@github.example",
        "GIT_COMMITTER_NAME": "GitHub (stub)",
        "GIT_COMMITTER_EMAIL": "noreply@github.example",
    }
    commit = subprocess.run(
        ["git", "--git-dir", os.environ["GH_STUB_REMOTE"], "commit-tree", tree, "-p", base_tip, "-F", "-"],
        input=message,
        capture_output=True,
        text=True,
        check=True,
        env=environment,
    ).stdout.strip()
    git("update-ref", f"refs/heads/{pr['base']}", commit, base_tip)
    git("update-ref", f"refs/pull/{pr['number']}/head", head_tip)  # GitHub keeps every head
    pr.update(state="MERGED", headRefOid=head_tip, mergeCommit=commit)
    if "--delete-branch" in options:
        git("update-ref", "-d", f"refs/heads/{pr['head']}")
        for other in store.prs:
            if other["state"] == "OPEN" and other["base"] == pr["head"]:
                other["base"] = pr["base"]
    store.save()
    print(f"✓ Squashed and merged pull request #{pr['number']} ({pr['title']})")


def run_hook(argv: list[str]) -> None:
    """Runs GH_STUB_HOOK once, at the first call that contains GH_STUB_HOOK_MATCH (for races)."""
    hook, match = os.environ.get("GH_STUB_HOOK"), os.environ.get("GH_STUB_HOOK_MATCH")
    if not hook or not match or match not in " ".join(argv):
        return
    done = Path(os.environ["GH_STUB_DB"] + ".hook-done")
    if not done.exists():
        done.touch()
        subprocess.run(hook, shell=True, check=True)


def main(argv: list[str]) -> None:
    if log := os.environ.get("GH_STUB_LOG"):
        with open(log, "a", encoding="utf-8") as file:
            file.write(json.dumps(argv) + "\n")
    run_hook(argv)
    if argv[:2] == ["auth", "status"]:
        if os.environ.get("GH_STUB_UNAUTHENTICATED"):
            fail("You are not logged into any GitHub hosts. To log in, run: gh auth login")
        return
    if len(argv) < 2 or argv[0] != "pr":
        fail(f"unsupported command: {' '.join(argv)}", 2)
    command = argv[1]
    positional, options = parse(argv[2:])
    store = Store(Path(os.environ["GH_STUB_DB"]))
    if command == "list":
        state = options.get("--state", "open").upper()
        found = [
            pr
            for pr in sorted(store.prs, key=lambda pr: pr["number"], reverse=True)
            if pr["head"] == options.get("--head", pr["head"]) and state in ("ALL", pr["state"])
        ]
        output([fields(pr) for pr in found], options)
    elif command == "create":
        head, base, title = options.get("--head"), options.get("--base"), options.get("--title")
        body = read_body(options)
        if head is None or base is None or title is None or body is None:
            fail("pr create needs --head, --base, --title and --body-file", 2)
        if os.environ.get("GH_STUB_FAIL_CREATE") == head:
            fail(f"creating the pull request for {head} failed (injected)")
        if tip(head) is None:
            fail(f"{head} is not pushed")
        if tip(base) is None:
            fail(f"the base branch {base} does not exist")
        if any(pr["head"] == head and pr["state"] == "OPEN" for pr in store.prs):
            fail(f"a pull request for branch {head!r} already exists")
        number = store.data["next"]
        store.data["next"] += 1
        store.prs.append(
            {
                "number": number,
                "head": head,
                "base": base,
                "title": title,
                "body": body,
                "state": "OPEN",
                "headRefOid": None,
                "comments": [],
                "mergeCommit": None,
            }
        )
        store.save()
        print(f"https://github.example/stub/pull/{number}")
    elif command in ("view", "edit", "comment", "merge"):
        if len(positional) != 1:
            fail(f"pr {command} needs one pull request number or branch", 2)
        pr = store.find(positional[0])
        if command == "view":
            output(fields(pr), options)
        elif command == "edit":
            if "--base" in options:
                if tip(options["--base"]) is None:
                    fail(f"the base branch {options['--base']} does not exist")
                pr["base"] = options["--base"]
            if "--title" in options:
                pr["title"] = options["--title"]
            if (body := read_body(options)) is not None:
                pr["body"] = body
            store.save()
            print(f"https://github.example/stub/pull/{pr['number']}")
        elif command == "comment":
            body = read_body(options)
            if body is None:
                fail("pr comment needs --body-file or --body", 2)
            pr["comments"].append(body)
            store.save()
            print(f"https://github.example/stub/pull/{pr['number']}#issuecomment-{len(pr['comments'])}")
        else:
            merge(store, pr, options)
    else:
        fail(f"unsupported command: pr {command}", 2)


if __name__ == "__main__":
    main(sys.argv[1:])

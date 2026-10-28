#!/usr/bin/env python3
"""Builds and checks publish kits: what an agent without GitHub credentials hands over.

A publish kit is a directory (by convention `.stacks/<topic>/` in the repository; it ignores
itself) holding one or more stacked pull requests as patches, with their commit messages,
descriptions and tested trees, and `publish.sh`, which the user runs to create the branches, push
them, open the pull requests, restack after merges and apply fixes. See kits.md.

    kit.py build --repo SCRATCH --base BASE --tip TIP --prefix PREFIX --names a,b,c KIT
    kit.py fix --repo SCRATCH --name NAME --commit FIX KIT
    kit.py check [--repo SCRATCH] KIT

build: one pull request per commit of BASE..TIP in the scratch repository; a commit's message is
the commit message on the branch and its first line the pull request's title. The descriptions are
KIT/prs/N.md (or --bodies DIR/N.md). The stages become refs/kits/<topic>/stage-N in the scratch
repository. With --workspace DIR, every file the pull requests change must be in the last stage as
it is in DIR (which is only read); other differences, such as a newer main, are reported.

fix: FIX is a commit on top of one stage of a published kit. The stages after it are rebased onto
it as `publish.sh fix NAME` will do; the kit records the patch, the message and the trees before
and after, and its patches and trees become those of the fixed stack.

check: validates the files of a kit; with --repo, also rebuilds the stages from the patches.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Self

SCRIPTS = Path(__file__).resolve().parent
DEFAULT_TYPES = ("feat", "fix", "perf", "refactor", "docs", "test", "ci", "build", "chore", "revert")
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
WIDTH = 72  # GitHub wraps longer lines of a description at this width when it builds the squash commit
BLOCK_START = re.compile(r"\s*(?:$|```|~~~|\||#|>|[-*+]\s|\d+[.)]\s)")  # a line that starts a new block
IDENTITY = {
    "GIT_AUTHOR_NAME": "publish kit",
    "GIT_AUTHOR_EMAIL": "kit@localhost",
    "GIT_COMMITTER_NAME": "publish kit",
    "GIT_COMMITTER_EMAIL": "kit@localhost",
}
MANAGED = ("patches", "messages", "fixes", "published", "stack.conf", "publish.sh", "README.md", ".gitignore")


class KitError(Exception):
    """A problem that stops the command (the message says what to change)."""


def run_git(cwd: Path, *args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        input=stdin,
        capture_output=True,
        text=True,
        env={**os.environ, **IDENTITY},
        check=False,
    )


def git(cwd: Path, *args: str, stdin: str | None = None) -> str:
    result = run_git(cwd, *args, stdin=stdin)
    if result.returncode != 0:
        raise KitError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def diff_to(repo: Path, old: str, new: str, path: Path) -> None:
    """Writes the binary-safe diff between two commits to a file, byte for byte."""
    with path.open("wb") as file:
        subprocess.run(["git", "-C", str(repo), "diff", "--binary", "--full-index", old, new], stdout=file, check=True)
    if path.stat().st_size == 0:
        raise KitError(f"{old[:12]}..{new[:12]} changes nothing: every pull request must change something")


def resolve(repo: Path, revision: str) -> str:
    sha = run_git(repo, "rev-parse", "--verify", "--quiet", f"{revision}^{{commit}}").stdout.strip()
    if not sha:
        raise KitError(f"{revision} is not a commit in {repo}")
    return sha


def tree_of(repo: Path, revision: str) -> str:
    return git(repo, "rev-parse", f"{revision}^{{tree}}").strip()


def message_of(repo: Path, commit: str) -> str:
    return git(repo, "log", "-1", "--format=%B", commit).rstrip() + "\n"


def subject_of(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[0] if lines else ""


@dataclass
class Conf:
    topic: str
    base: str
    prefix: str
    names: list[str]
    trees: list[str]
    fixes: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.names)


def read_conf(path: Path) -> Conf:
    if not path.is_file():
        raise KitError(f"{path} is missing: is this a publish kit?")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            values[key] = value
    missing = [key for key in ("TOPIC", "BASE_COMMIT", "PREFIX", "NAMES", "TREES") if key not in values]
    if missing:
        raise KitError(f"{path} lacks {', '.join(missing)}")
    return Conf(
        topic=values["TOPIC"],
        base=values["BASE_COMMIT"],
        prefix=values["PREFIX"],
        names=values["NAMES"].split(),
        trees=values["TREES"].split(),
        fixes=values.get("FIXES", "").split(),
    )


def write_conf(path: Path, conf: Conf) -> None:
    path.write_text(
        f"# Publish kit {conf.topic}: written by kit.py and read (never sourced) by publish.sh.\n"
        f"TOPIC={conf.topic}\n"
        f"BASE_COMMIT={conf.base}\n"
        f"PREFIX={conf.prefix}\n"
        f"NAMES={' '.join(conf.names)}\n"
        f"TREES={' '.join(conf.trees)}\n"
        f"FIXES={' '.join(conf.fixes)}\n",
        encoding="utf-8",
    )


def toplevel(path: Path) -> Path | None:
    result = run_git(path, "rev-parse", "--show-toplevel")
    return Path(result.stdout.strip()).resolve() if result.returncode == 0 else None


def display_path(kit: Path) -> str:
    """The kit as the user runs it: relative to the top of its repository, if it is in one."""
    root = toplevel(kit)
    if root is not None and kit.resolve().is_relative_to(root):
        return str(kit.resolve().relative_to(root))
    return str(kit.resolve())


def conventional_types(kit: Path) -> tuple[str, ...]:
    """The types the PR title check accepts (.github/workflows/pr-title.yml), else the defaults."""
    for start in (kit, SCRIPTS):
        root = toplevel(start)
        workflow = root / ".github/workflows/pr-title.yml" if root else None
        if workflow is not None and workflow.is_file():
            match = re.search(r"types:\s*\|\n((?:[ \t]+[a-z]+\n)+)", workflow.read_text(encoding="utf-8"))
            if match:
                return tuple(match.group(1).split())
    return DEFAULT_TYPES


class Worktree:
    """A temporary detached worktree of a repository, for rebuilding stages."""

    def __init__(self, repo: Path, commit: str) -> None:
        self.repo = repo
        self.path = Path(tempfile.mkdtemp(prefix="publish-kit."))
        git(repo, "worktree", "add", "--quiet", "--detach", str(self.path), commit)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        run_git(self.repo, "worktree", "remove", "--force", str(self.path))
        shutil.rmtree(self.path, ignore_errors=True)
        run_git(self.repo, "worktree", "prune")

    def apply(self, patch: Path) -> None:
        """Applies a patch like publish.sh does: exactly, else with a 3-way merge."""
        if run_git(self.path, "apply", "--index", "--whitespace=nowarn", str(patch)).returncode != 0:
            git(self.path, "apply", "--index", "--3way", "--whitespace=nowarn", str(patch))

    def commit(self, message: str) -> str:
        git(self.path, "commit", "--quiet", "--no-verify", "--cleanup=whitespace", "-F", "-", stdin=message)
        return git(self.path, "rev-parse", "HEAD").strip()


def rebuild(kit: Path, conf: Conf, patches: Path, worktree: Worktree) -> list[str]:
    """Rebuilds the stage commits from a kit's patches on its base; returns the commits."""
    commits = []
    for index in range(1, conf.count + 1):
        try:
            worktree.apply(patches / f"{index}.patch")
        except KitError as error:
            raise KitError(f"patch {index} does not apply to the stage before it: {error}") from error
        commits.append(worktree.commit((kit / "messages" / f"{index}.txt").read_text(encoding="utf-8")))
    return commits


def workspace_tree(repo: Path, tip: str, workspace: Path) -> str:
    """The tree git would commit from a directory's files on top of TIP; nothing is written there."""
    with tempfile.TemporaryDirectory(prefix="publish-kit-index.") as scratch:
        environment = {**os.environ, "GIT_INDEX_FILE": str(Path(scratch) / "index")}
        for args in (["read-tree", tip], [f"--work-tree={workspace}", "add", "--all"], ["write-tree"]):
            result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=environment)
            if result.returncode != 0:
                raise KitError(f"reading the files of {workspace} failed: {result.stderr.strip()}")
        return result.stdout.strip()


def check_workspace(repo: Path, base: str, tip: str, workspace: Path) -> None:
    """The files the pull requests change must be the workspace's; other differences are reported."""
    found = workspace_tree(repo, tip, workspace)
    changed = set(git(repo, "diff", "--name-only", "--no-renames", "-z", base, tip).split("\0")) - {""}
    fields = git(repo, "diff", "--name-status", "--no-renames", "-z", tree_of(repo, tip), found).split("\0")
    rows = list(zip(fields[0::2], fields[1::2], strict=False))
    mismatched = [f"{status}\t{path}" for status, path in rows if path in changed]
    others = [path for _, path in rows if path not in changed]
    if mismatched:
        raise KitError(f"the pull requests' files are not those of {workspace}:\n" + "\n".join(mismatched))
    print(f"the files the pull requests change are exactly those of {workspace}")
    if others:
        shown = ", ".join(others[:8]) + (", ..." if len(others) > 8 else "")
        print(f"warning: {workspace} also differs in {len(others)} file(s) that no pull request changes: {shown}")
        print("         (is origin/main newer than the workspace, or is a change missing from the stack?)")


def set_stage_refs(repo: Path, topic: str, commits: dict[int, str], count: int) -> None:
    for index, commit in commits.items():
        git(repo, "update-ref", f"refs/kits/{topic}/stage-{index}", commit)
    for ref in git(repo, "for-each-ref", "--format=%(refname)", f"refs/kits/{topic}/").split():
        if int(ref.rsplit("-", 1)[1]) > count:
            git(repo, "update-ref", "-d", ref)


# ---- README -----------------------------------------------------------------------------------


def render_readme(kit: Path, conf: Conf) -> str:
    where = display_path(kit)
    titles = [subject_of(kit / "messages" / f"{index}.txt") for index in range(1, conf.count + 1)]
    stacked = conf.count > 1
    lines = [f"# Publish kit: {conf.topic}", ""]
    if stacked:
        lines.append(f"{conf.count} stacked pull requests (each one based on the one before it), built and tested")
    else:
        lines.append("One pull request, built and tested")
    lines += [
        "by an agent that cannot push. `publish.sh` builds the commits from `origin/main` in a temporary",
        "worktree with your git identity and pushes only when every branch is exactly the tree that was",
        "tested. Your checkout and working tree are never touched.",
        "",
        "| # | Branch | Pull request |",
        "| --- | --- | --- |",
    ]
    for index, (name, title) in enumerate(zip(conf.names, titles, strict=True), 1):
        lines.append(f"| {index} | `{conf.prefix}{name}` | {title} |")
    plural, plural_es = ("s", "es") if stacked else ("", "")
    lines += [
        "",
        "```bash",
        f"{where}/publish.sh --dry-run   # build the commits and verify them; push nothing",
        f"{where}/publish.sh             # create the branch{plural_es}, push, open the pull request{plural}",
        f"{where}/publish.sh status",
        "```",
        "",
    ]
    if stacked:
        lines += [
            'Merge the pull requests in order with "Squash and merge". After each merge, run',
            f"`{where}/publish.sh restack`: it rebases the remaining branches onto main (dropping the",
            "merged commit), force-pushes them with a lease and points the next pull request at main.",
            "Review commits pushed to a branch are kept. If a rebase conflicts, nothing is pushed and the",
            "script prints the commands to resolve it; `restack` then continues with the branches after it.",
            "",
        ]
    else:
        lines += ['Merge the pull request with "Squash and merge".', ""]
    if conf.fixes:
        lines += [
            "## Fixes",
            "",
            "Each fix is a new commit on one branch; the branches after it are rebased onto it and",
            "force-pushed with a lease, after every tree is compared with the tested one. Run them in this",
            "order, each after `--dry-run`; running one again does nothing.",
            "",
            "| Command | Branch | Commit |",
            "| --- | --- | --- |",
        ]
        for name in conf.fixes:
            stage = int((kit / "fixes" / f"{name}.trees").read_text(encoding="utf-8").split()[0])
            subject = subject_of(kit / "fixes" / f"{name}.txt")
            lines.append(f"| `{where}/publish.sh fix {name}` | `{conf.prefix}{conf.names[stage - 1]}` | {subject} |")
        lines += ["", "The patches in `patches/` include the fixes, so a first `publish` builds the fixed stack.", ""]
    lines += [
        "Files: `stack.conf` (the base commit, branch names and tested trees), `patches/N.patch` (each",
        "pull request's diff against the one before it), `messages/N.txt` (commit messages; the first",
        "lines are the titles), `prs/N.md` (descriptions), `fixes/` (fixes after publishing), `forks`",
        "(written by `publish.sh`: the commit each branch was built on). The directory ignores itself;",
        "delete it once everything is merged.",
    ]
    return "\n".join(lines) + "\n"


# ---- checks -----------------------------------------------------------------------------------


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def describe(numbers: list[int]) -> str:
    shown = ", ".join(str(number) for number in numbers[:5])
    return (
        f"line {shown} is"
        if len(numbers) == 1
        else f"{len(numbers)} lines ({shown}{', ...' if len(numbers) > 5 else ''}) are"
    )


def ragged_prose_lines(text: str) -> list[int]:
    """Prose lines longer than WIDTH that GitHub would wrap into ragged lines (outside code and tables)."""
    # Text wrapped at a greater width: the paragraph goes on in the next line. A paragraph or a list
    # item on a single line is fine, GitHub wraps it evenly.
    found, fenced = [], False
    lines = text.splitlines()
    for number, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith(("```", "~~~")):
            fenced = not fenced
        elif (
            not fenced
            and len(line) > WIDTH
            and not stripped.startswith("|")
            and " " in stripped
            and number < len(lines)
            and not BLOCK_START.match(lines[number])
        ):
            found.append(number)
    return found


def check_kit(kit: Path, repo: Path | None) -> Report:
    report = Report()
    conf = read_conf(kit / "stack.conf")
    if not NAME.match(conf.topic):
        report.errors.append(f"TOPIC {conf.topic!r} must be a simple name")
    if not HEX40.match(conf.base):
        report.errors.append("BASE_COMMIT must be a full commit id")
    if not conf.names or len(conf.trees) != conf.count:
        report.errors.append("stack.conf needs one tree per name")
    types = conventional_types(kit)
    title = re.compile(rf"(?:{'|'.join(types)})(?:\([a-z0-9][a-z0-9._/-]*\))?!?: \S")
    for index, name in enumerate(conf.names, 1):
        if not NAME.match(name):
            report.errors.append(f"branch name {conf.prefix}{name}: names are letters, digits, '.', '_' and '-'")
        for part in (f"patches/{index}.patch", f"messages/{index}.txt", f"prs/{index}.md"):
            if not (kit / part).is_file() or (kit / part).stat().st_size == 0:
                report.errors.append(f"{part} is missing or empty")
        message_path, body_path = kit / f"messages/{index}.txt", kit / f"prs/{index}.md"
        if message_path.is_file():
            message = message_path.read_text(encoding="utf-8").splitlines() or [""]
            if not title.match(message[0]):
                report.errors.append(
                    f"messages/{index}.txt: {message[0]!r} is not a conventional title ({', '.join(types)})"
                )
            long = [number for number, line in enumerate(message[1:], 2) if len(line) > WIDTH and " " in line]
            if long:
                report.warnings.append(f"messages/{index}.txt: {describe(long)} longer than {WIDTH} characters")
        if body_path.is_file():
            body = body_path.read_text(encoding="utf-8")
            if "{{" in body:
                report.warnings.append(f"prs/{index}.md has a '{{{{' placeholder; descriptions are used as they are")
            if ragged := ragged_prose_lines(body):
                report.warnings.append(
                    f"prs/{index}.md: {describe(ragged)} longer than {WIDTH} characters and wrapped;"
                    " GitHub wraps them again in the squash commit: write each paragraph on one line"
                )
    ignore = kit / ".gitignore"
    if not ignore.is_file() or ignore.read_text(encoding="utf-8") != "*\n":
        report.errors.append(".gitignore must be '*', so that the kit never shows up in git status")
    if not (kit / "publish.sh").is_file():
        report.errors.append("publish.sh is missing")
    elif (kit / "publish.sh").read_bytes() != (SCRIPTS / "publish.sh").read_bytes():
        report.warnings.append(f"publish.sh differs from {SCRIPTS / 'publish.sh'}")
    check_fixes(kit, conf, title, report)
    if repo is not None and not report.errors:
        check_rebuild(kit, repo, conf, report)
    return report


def check_fixes(kit: Path, conf: Conf, title: re.Pattern[str], report: Report) -> None:
    """The fixes, applied in order to the trees as published, must lead to TREES."""
    if not conf.fixes:
        return
    if not (kit / "published" / "stack.conf").is_file():
        report.errors.append("a kit with fixes needs published/ (the stack as it was published)")
        return
    trees = read_conf(kit / "published" / "stack.conf").trees
    for name in conf.fixes:
        files = [kit / "fixes" / f"{name}.{suffix}" for suffix in ("patch", "txt", "trees")]
        if not all(file.is_file() for file in files):
            report.errors.append(f"fix {name}: fixes/{name}.patch, .txt and .trees are needed")
            return
        if not title.match(subject_of(files[1])):
            report.errors.append(f"fixes/{name}.txt: {subject_of(files[1])!r} is not a conventional title")
        for row in (line.split() for line in files[2].read_text(encoding="utf-8").splitlines() if line.strip()):
            if len(row) != 3 or not row[0].isdigit() or not 1 <= int(row[0]) <= conf.count:
                report.errors.append(f"fixes/{name}.trees: {' '.join(row)!r} is not 'N BEFORE AFTER'")
                return
            stage = int(row[0])
            if trees[stage - 1] != row[1]:
                report.errors.append(
                    f"fix {name}: branch {stage} is {trees[stage - 1][:12]} before it, not {row[1][:12]}"
                )
            trees[stage - 1] = row[2]
    if trees != conf.trees:
        report.errors.append("the fixes do not lead to the trees in stack.conf")


def check_rebuild(kit: Path, repo: Path, conf: Conf, report: Report) -> None:
    sets = [(kit / "patches", conf)]
    if conf.fixes:
        sets.append((kit / "published" / "patches", read_conf(kit / "published" / "stack.conf")))
    for patches, expected in sets:
        with Worktree(repo, resolve(repo, conf.base)) as worktree:
            try:
                commits = rebuild(kit, expected, patches, worktree)
            except KitError as error:
                report.errors.append(f"{patches.relative_to(kit)}: {error}")
                continue
            for index, commit in enumerate(commits, 1):
                if tree_of(repo, commit) != expected.trees[index - 1]:
                    report.errors.append(
                        f"{patches.relative_to(kit)}/{index}.patch does not give the tree in stack.conf"
                    )


def print_report(report: Report) -> int:
    for warning in report.warnings:
        print(f"warning: {warning}")
    for error in report.errors:
        print(f"error: {error}")
    if report.errors:
        return 1
    print("✓ the kit is consistent")
    return 0


# ---- commands ---------------------------------------------------------------------------------


def build(args: argparse.Namespace) -> int:
    repo, kit = Path(args.repo).resolve(), Path(args.kit).resolve()
    if (kit / "forks").exists() and not args.force:
        raise KitError(f"{kit} was published (it has forks): add fixes with `kit.py fix`, or rebuild with --force")
    base, tip = resolve(repo, args.base), resolve(repo, args.tip)
    if run_git(repo, "merge-base", "--is-ancestor", base, tip).returncode != 0:
        raise KitError(f"{args.tip} does not contain {args.base}")
    if git(repo, "rev-list", "--merges", f"{base}..{tip}").strip():
        raise KitError(f"{args.base}..{args.tip} has merge commits; a kit needs a linear history")
    commits = git(repo, "rev-list", "--reverse", f"{base}..{tip}").split()
    names = [name for name in args.names.split(",") if name]
    if not commits or len(commits) != len(names):
        raise KitError(
            f"{args.base}..{args.tip} has {len(commits)} commits and --names {len(names)}: one commit per pull request"
        )
    for name in names:
        branch = f"{args.prefix}{name}"
        if not NAME.match(name) or run_git(repo, "check-ref-format", "--branch", branch).returncode != 0:
            raise KitError(f"{branch!r} is not a good branch name")
    topic = args.topic or kit.name
    if not NAME.match(topic):
        raise KitError(f"the topic {topic!r} must be a simple name (--topic)")
    if args.workspace:
        check_workspace(repo, base, tip, Path(args.workspace).resolve())
    bodies_dir = Path(args.bodies).resolve() if args.bodies else kit / "prs"
    bodies = {}
    for index in range(1, len(commits) + 1):
        body = bodies_dir / f"{index}.md"
        if not body.is_file():
            raise KitError(f"{body} is missing: write the description of pull request {index} first")
        bodies[index] = body.read_text(encoding="utf-8")
    kit.mkdir(parents=True, exist_ok=True)
    for managed in MANAGED:
        target = kit / managed
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
    for directory in ("patches", "messages", "prs"):
        (kit / directory).mkdir(exist_ok=True)
    for stale in (kit / "prs").glob("*.md"):
        if not stale.stem.isdigit() or int(stale.stem) > len(commits):
            stale.unlink()
    trees = []
    for index, commit in enumerate(commits, 1):
        diff_to(repo, f"{commit}^", commit, kit / "patches" / f"{index}.patch")
        (kit / "messages" / f"{index}.txt").write_text(message_of(repo, commit), encoding="utf-8")
        (kit / "prs" / f"{index}.md").write_text(bodies[index], encoding="utf-8")
        trees.append(tree_of(repo, commit))
    conf = Conf(topic=topic, base=base, prefix=args.prefix, names=names, trees=trees)
    write_conf(kit / "stack.conf", conf)
    shutil.copyfile(SCRIPTS / "publish.sh", kit / "publish.sh")
    (kit / "publish.sh").chmod(0o755)
    (kit / ".gitignore").write_text("*\n", encoding="utf-8")
    (kit / "README.md").write_text(render_readme(kit, conf), encoding="utf-8")
    set_stage_refs(repo, topic, dict(enumerate(commits, 1)), len(commits))
    print(f"built {display_path(kit)}: {len(commits)} pull request(s) on {base[:12]}")
    for index, name in enumerate(names, 1):
        print(
            f"  {index}. {args.prefix}{name}  {subject_of(kit / 'messages' / f'{index}.txt')}"
            f"  (refs/kits/{topic}/stage-{index})"
        )
    return print_report(check_kit(kit, repo))


def apply_fix(
    worktree: Worktree, conf: Conf, stages: list[str], stage: int, patch: Path, message: str
) -> dict[int, str]:
    """Commits the fix onto a stage and rebases the stages after it, as `publish.sh fix` does."""
    git(worktree.path, "checkout", "--quiet", "--detach", stages[stage - 1])
    worktree.apply(patch)
    new = {stage: worktree.commit(message)}
    if stage == conf.count:
        return new
    git(worktree.path, "checkout", "--quiet", "--detach", stages[-1])
    rebased = run_git(worktree.path, "rebase", "--quiet", "--no-verify", "--onto", new[stage], stages[stage - 1])
    if rebased.returncode != 0:
        run_git(worktree.path, "rebase", "--abort")
        raise KitError(
            f"the fix conflicts with a branch after {conf.prefix}{conf.names[stage - 1]},"
            " so `publish.sh fix` would stop"
            f" too:\n{rebased.stdout.strip()}\n{rebased.stderr.strip()}"
        )
    later = git(worktree.path, "rev-list", "--reverse", f"{new[stage]}..HEAD").split()
    if len(later) != conf.count - stage:
        raise KitError("rebasing the branches after the fix dropped a commit: the fix makes a later stage empty")
    new.update({stage + offset: commit for offset, commit in enumerate(later, 1)})
    return new


def fix(args: argparse.Namespace) -> int:
    repo, kit = Path(args.repo).resolve(), Path(args.kit).resolve()
    conf = read_conf(kit / "stack.conf")
    name = args.name
    if not NAME.match(name) or name in conf.fixes:
        raise KitError(f"fix name {name!r}: a simple name this kit does not use yet ({' '.join(conf.fixes) or 'none'})")
    commit = resolve(repo, args.commit)
    parents = git(repo, "rev-list", "--parents", "-n1", commit).split()[1:]
    if len(parents) != 1:
        raise KitError(f"{args.commit} must be a commit with one parent (the stage it fixes)")
    parent_tree = tree_of(repo, parents[0])
    if parent_tree not in conf.trees:
        raise KitError(
            f"{args.commit} must be on top of a stage of the kit (refs/kits/{conf.topic}/stage-N),"
            " but its parent's tree"
            f" {parent_tree[:12]} is none of the kit's trees"
        )
    stage = conf.trees.index(parent_tree) + 1
    message = message_of(repo, commit)
    with (
        tempfile.TemporaryDirectory(prefix="publish-kit-fix.") as scratch,
        Worktree(repo, resolve(repo, conf.base)) as worktree,
    ):
        patch = Path(scratch) / "fix.patch"
        diff_to(repo, parents[0], commit, patch)
        stages = rebuild(kit, conf, kit / "patches", worktree)
        if [tree_of(repo, built) for built in stages] != conf.trees:
            raise KitError("the kit's patches do not give the trees in stack.conf; run kit.py check")
        new = apply_fix(worktree, conf, stages, stage, patch, message)
        if tree_of(repo, new[stage]) != tree_of(repo, commit):
            raise KitError("applying the fix to its stage gave another tree than the fix commit")
        # Everything is known: write the fix, and make the kit's patches those of the fixed stack.
        if not conf.fixes and not (kit / "published").exists():
            (kit / "published").mkdir()
            shutil.copyfile(kit / "stack.conf", kit / "published" / "stack.conf")
            shutil.copytree(kit / "patches", kit / "published" / "patches")
        (kit / "fixes").mkdir(exist_ok=True)
        shutil.copyfile(patch, kit / "fixes" / f"{name}.patch")
        (kit / "fixes" / f"{name}.txt").write_text(message, encoding="utf-8")
        rows = [
            f"{index} {conf.trees[index - 1]} {tree_of(repo, new[index])}" for index in range(stage, conf.count + 1)
        ]
        (kit / "fixes" / f"{name}.trees").write_text("\n".join(rows) + "\n", encoding="utf-8")
        for index in range(stage, conf.count + 1):
            previous = new[index - 1] if index > stage else (stages[index - 2] if index > 1 else conf.base)
            diff_to(repo, previous, new[index], kit / "patches" / f"{index}.patch")
            conf.trees[index - 1] = tree_of(repo, new[index])
    conf.fixes.append(name)
    write_conf(kit / "stack.conf", conf)
    (kit / "README.md").write_text(render_readme(kit, conf), encoding="utf-8")
    set_stage_refs(repo, conf.topic, new, conf.count)
    branch = f"{conf.prefix}{conf.names[stage - 1]}"
    print(
        f"added fix {name} to {display_path(kit)}: a commit on {branch}, then {conf.count - stage} branch(es) rebased"
    )
    for index in range(stage, conf.count + 1):
        print(
            f"  {index}. {conf.prefix}{conf.names[index - 1]}  tree {conf.trees[index - 1][:12]}"
            f"  (refs/kits/{conf.topic}/stage-{index})"
        )
    return print_report(check_kit(kit, repo))


def check(args: argparse.Namespace) -> int:
    return print_report(check_kit(Path(args.kit).resolve(), Path(args.repo).resolve() if args.repo else None))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    build_parser = commands.add_parser("build", help="make a kit of the commits BASE..TIP of a scratch repository")
    build_parser.add_argument("--repo", required=True, help="the scratch repository with the commits")
    build_parser.add_argument("--base", required=True, help="the commit on main the stack is built on")
    build_parser.add_argument("--tip", required=True, help="the last commit of the stack")
    build_parser.add_argument("--prefix", required=True, help="branch prefix, for example 'docs/' or 'models-stack/'")
    build_parser.add_argument("--names", required=True, help="comma-separated branch names, one per commit")
    build_parser.add_argument("--topic", help="the kit's name (default: the directory's name)")
    build_parser.add_argument("--bodies", help="a directory with the descriptions 1.md, 2.md, ... (default: KIT/prs)")
    build_parser.add_argument("--workspace", help="a directory with the pull requests' files (only read)")
    build_parser.add_argument("--force", action="store_true", help="rebuild a kit that was published")
    build_parser.add_argument("kit", help="the kit directory, for example .stacks/<topic>")
    build_parser.set_defaults(handler=build)
    fix_parser = commands.add_parser("fix", help="add a fix (a commit on top of a stage) to a published kit")
    fix_parser.add_argument("--repo", required=True, help="the scratch repository with the fix commit")
    fix_parser.add_argument("--name", required=True, help="the fix's name, as in `publish.sh fix NAME`")
    fix_parser.add_argument("--commit", required=True, help="the fix commit, on top of refs/kits/<topic>/stage-N")
    fix_parser.add_argument("kit")
    fix_parser.set_defaults(handler=fix)
    check_parser = commands.add_parser("check", help="validate a kit")
    check_parser.add_argument("--repo", help="a repository with the base commit, to rebuild the stages")
    check_parser.add_argument("kit")
    check_parser.set_defaults(handler=check)
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except KitError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

---
name: fanferro-pull-requests
description: Commit, push and open pull requests for fanferro - conventional titles, descriptions that become the squash commit, stacked pull requests (splitting work, verifying every stage, keeping the branches up to date after reviews and merges), and fixing failing checks and review comments. Use when committing, opening, updating or merging pull requests, splitting a change into a stack, or handling a failing CI check. An agent that cannot push to GitHub hands its work over as a publish kit instead (kits.md).
---

# Pull requests and stacks

Every change reaches `main` as a squash-merged pull request. An agent that can push to GitHub does
all of it itself, with `git` and an authenticated `gh`: it branches, commits, pushes, opens the pull
request, follows its checks and answers reviews. There is no publish kit, scratch clone, rehearsal
or hand-over in that path. Only an agent that cannot push hands its work over as a publish kit
(section 6). The commands below run from the top of the repository, with
`SKILL=.claude/skills/fanferro-pull-requests/scripts`.

## 1. How a pull request lands

- **Squash merge.** The commit on main is `<PR title> (#N)`, with the PR description as its body
  and the `Co-authored-by` trailers of the branch's commits. The branch's own commit messages are
  dropped. In the body, GitHub wraps every line longer than 72 characters at 72, except in code
  blocks. Merged branches are deleted.
- **Changelog.** git-cliff (`cliff.toml`) builds the release notes from the subjects on main: the
  type picks the group, the scope becomes a bold prefix, and `!` or `BREAKING CHANGE:` marks a
  breaking change. The PR title is the changelog line, so write it for users of fanferro. What a
  title cannot say goes into `CHANGELOG.md` under `[Unreleased]`, in the same PR.
- **Title check.** `.github/workflows/pr-title.yml` accepts `feat`, `fix`, `perf`, `refactor`,
  `docs`, `test`, `ci`, `build`, `chore` and `revert`, with an optional scope and `!`. Editing the
  title runs the check again.
- **CI** (`ci.yml`) runs on every pull request, also on one based on another branch: lint, types
  and docs consistency (`scripts/check_consistency.py`, actionlint); Rust tests and the Rust
  end-to-end tests on MiniStack; Python 3.12-3.15 on Linux x86_64 and arm64, with the integration
  tests on both backends and the e2e tests; macOS and Windows on 3.12 and 3.15 without the
  emulator (`-m "not e2e" -k "not ministack"`); packaging (sdist and a zig cross-built wheel).
  CodeQL runs too.

## 2. One pull request

1. **Branch.** Work on the branch your harness gave you, or create one from the current main:
   `git switch -c <type>/<topic> origin/main`, for example `docs/aws-credentials-region`.
2. **Commit** as you go, with conventional subjects and bodies that say why, ending with the
   attribution trailers your harness gives you. The squash merge drops these messages, so the
   number of commits does not matter. Never amend, rebase or force-push a commit once it is pushed.
3. **Title**: the conventional subject of the whole change. **Description**: what changes and why,
   a `## Breaking change` section with the migration when there is one, and how it was tested; end
   it with the attribution lines or session link your harness asks for. Write each paragraph and
   list item on a single line: GitHub shows the description as written, and the squash commit gets
   the same text wrapped at 72 columns. Text wrapped at another width comes out ragged on main (see
   the v0.4.0 and v0.5.0 release commits). Leave out what does not belong in main's history:
   instructions for whoever merges, local paths, the order of a stack.
4. **Check** with `make check` before you open it.
5. **Push and open it** yourself: `git push -u origin <branch>`, then `gh pr create --base main
   --title "<title>" --body-file <description>`. Push later commits the same way, and keep the title
   and description true to what the pull request does: `gh pr edit <n> --title "<title>"
   --body-file <description>`.

## 3. Stacked pull requests

Split a change into a stack when its steps can be reviewed on their own: fixes the feature needs,
refactors, the feature, and a breaking change last. Fixes go first, so that they can be merged
while the feature is still in review.

- Each pull request passes CI on its own and has its own tests, docs and changelog lines.
- Pull request N is based on branch N-1, the first one on main. Branches are named
  `<topic>-stack/<n>-<name>`, for example `fast-defaults-stack/2-free-slots`.
- #2-#7 were such a stack: a consumer fix first (#2), then performance work and a feature
  (#3-#5), the breaking change of the defaults (#6) and a last performance change (#7).
- The order of the stack goes in a comment on each pull request, not into the descriptions, which
  become commit messages.

**Build the branches** in your checkout, each from the one before it:

```bash
git switch -c <topic>-stack/1-<name> origin/main   # commit the first change
git switch -c <topic>-stack/2-<name>               # branches off stack/1: commit the second change
```

When the work already exists as one change, split it: commit each stage's files in order, editing a
file to its intermediate state when a stage has only part of it. The last branch must hold exactly
the work: `git diff <the work> <topic>-stack/<last>` shows nothing.

**Test the branches against each other:**

- Every stage passes the CI checks on its own:
  `$SKILL/verify_stages.sh . <topic>-stack/1-<name> <topic>-stack/2-<name> ...` checks the stages
  out one after another in a clone of its own (under `~/.cache/fanferro-stages`), so your checkout
  stays as it is. `--quick` skips the emulator; `--python "3.12 3.15"` (the default) picks the
  Pythons.
- Each pull request contains only its own change: read `git diff <branch N-1> <branch N>` for every
  stage.

**Open them** bottom up, each based on the branch before it, then comment the order on each:

```bash
git push -u origin <topic>-stack/1-<name> <topic>-stack/2-<name>
gh pr create --base main --head <topic>-stack/1-<name> --title "<title 1>" --body-file <1.md>
gh pr create --base <topic>-stack/1-<name> --head <topic>-stack/2-<name> --title "<title 2>" --body-file <2.md>
gh pr comment <n> --body-file <order.md>   # on each: the pull requests in merge order, one per line
```

The CI of each pull request then tests exactly its stage: its branch merged into the branch before
it.

**Merging** is up to the maintainer. GitHub's stack support merges a whole stack at once and lands
every pull request on main as its own squash commit, in order (#42-#46). A pull request merged on
its own goes into its base branch: #28-#38 were merged into theirs and reached main only inside
#27's commit. One at a time, merge only the pull request whose base is main, and update the next
branch (section 4) before merging it.

## 4. After opening: reviews, merges, a moving main

Bring changes into a pushed branch with new commits and merges, never with a rebase or a force-push:
the pull request, its reviews and its CI runs belong to the pushed commits, and the squash merge
drops the merge commits anyway.

- **A review comment or a failed check** becomes a new commit on the branch that needs it. Then
  merge each branch into the next one up the stack and push them:
  `git switch <topic>-stack/<n+1>-<name> && git merge <topic>-stack/<n>-<name>`.
- **After a pull request of a stack is merged on its own**, GitHub deletes its branch and bases the
  next pull request on main. That pull request's diff shows the merged change again, because main
  has the squash commit instead of the branch's commits: merge main into its branch
  (`git fetch origin && git switch <next branch> && git merge origin/main`), push, and carry it up
  the stack. `gh pr diff <n>` then shows only the pull request's own change.
- **When main moves** under an open pull request (other merges, conflicts in `CHANGELOG.md`), merge
  `origin/main` into its branch, the bottom one of a stack, and resolve the conflicts there; CI then
  tests the new combination.

## 5. Fixing a failing check

1. **Find what failed**: `gh pr checks <n>`, then `gh run view <run-id> --log-failed` (or
   `gh run view --job <job-id> --log-failed`).
2. **Reproduce it on the exact commit**, the pull request's branch (`gh pr checkout <n>`), with the
   failing job's command and the same Python:
   `UV_PROJECT_ENVIRONMENT=~/.cache/fanferro-venvs/3.12 uv run -p 3.12 ...` (a plain
   `uv run -p 3.12` replaces `.venv`, and later `uv run`s stay on 3.12). A failure only on MiniStack,
   or only in `cargo test --test aws_emulator`, is usually a protocol subtlety rather than
   flakiness: see `fanferro-e2e-testing` and ADR 0007. For a platform you cannot run (Windows,
   macOS), work out what differs there: clock resolution (Windows' clock advances in steps of
   about 15.6 ms, so two timestamps can be equal and short timeouts round), path separators, line
   endings, the event loop (the proactor loop on Windows), signals, case-insensitive or locked
   files. Emulate it so that the test fails first and passes after the fix, for example with a
   frozen or coarse clock in a fixture.
3. **Fix it on the earliest branch that has the cause** (`git log -S`, `git blame`), as a commit of
   its own (`test: ...`, `fix(<scope>): ...`).
4. **Verify that stage and every stage after it** again, push the fix and merge it up the stack
   (section 4).

## 6. Without push access: publish kits

An agent without GitHub credentials, or one working in someone's own checkout where it must not
commit, builds the pull requests in a scratch clone and hands over a **publish kit**: a tested
script that the user runs to push them, restack after merges and apply fixes. [kits.md](kits.md)
describes building, verifying and handing over a kit, following up on its pull requests, and the
rules and self-tests for these scripts. Everything else in this skill applies to kits too.

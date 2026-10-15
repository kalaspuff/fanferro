---
name: fanferro-pull-requests
description: Commit and open pull requests for fanferro - conventional titles, descriptions that become the squash commit, stacked pull requests (splitting work, verifying every stage, restacking after squash merges), fixing failing checks and review comments on a stack, and publish kits (a tested script the user runs) when the agent cannot push to GitHub. Use when committing, opening, updating or merging pull requests, splitting a change into a stack, handling a failing CI check, or handing work over from a machine without GitHub credentials.
---

# Pull requests, stacks and publish kits

Every change reaches `main` as a squash-merged pull request. The tools in `scripts/` build, test
and publish pull requests, also from a machine that cannot push: `kit.py` builds and checks
publish kits, `publish.sh` is what the user runs, `verify_stages.sh` runs the CI checks on every
stage, `rehearse.sh` rehearses a kit against a local origin with a stub `gh`, and `selftest.sh`
tests the tools. [kits.md](kits.md) describes kits and the rules for such scripts. The commands
below run from the top of the repository, with
`SKILL=.claude/skills/fanferro-pull-requests/scripts`.

## 1. How a pull request lands

- **Squash merge.** The commit on main is `<PR title> (#N)`, with the PR description as its body
  (GitHub rewraps prose lines longer than 72 characters, but not code blocks) and the
  `Co-authored-by` trailers of the branch's commits. The branch's own commit messages are dropped.
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

1. **Branch** from the current main: `<type>/<topic>`, for example `docs/aws-credentials-region`.
2. **Commit** with a conventional subject and a body, wrapped at 72 columns, that says why. End
   it with the attribution trailers your harness gives you (Claude Code: `Co-Authored-By:` and
   `Claude-Session:`).
3. **Title** = the commit subject. **Description**: what changes and why, a `## Breaking change`
   section with the migration when there is one, and how it was tested. Wrap prose at 72 columns
   so that the squash commit reads as written (code blocks may be longer), and end with the
   attribution lines. Leave out what does not belong in main's history: instructions for whoever
   merges, local paths, script names, the order of a stack.
4. **Check** with `make check` before opening it. If the workspace is the user's own checkout,
   mounted into your machine, build and test in a scratch clone instead: `$SKILL/verify_stages.sh`
   runs the checks in a clone of its own.
5. **Publish** once the user agrees, since pushing is outward-facing. With credentials:
   `git push -u origin <branch>` and `gh pr create --base main --title "<title>" --body-file
   <description>`. Without them, hand over a publish kit (section 6); a kit is the tested path
   with credentials too.

## 3. Stacked pull requests

Split a change into a stack when its steps can be reviewed on their own: fixes the feature needs,
refactors, the feature, and a breaking change last. Fixes go first, so that they can be merged
while the feature is still in review.

- Each pull request passes CI on its own and has its own tests, docs and changelog lines.
- Pull request N is based on branch N-1, the first one on main. Branches are named
  `<topic>-stack/<n>-<name>`, for example `fast-defaults-stack/2-free-slots`.
- #2-#7 were such a stack: a consumer fix first (#2), then performance work and a feature
  (#3-#5), the breaking change of the defaults (#6) and a last performance change (#7).
- Merge strictly in order with "Squash and merge", and only the pull request whose base is main:
  a pull request based on another branch merges into that branch. After each merge, restack the
  rest (section 4) before merging the next one.
- The order of the stack goes in a comment on each pull request (`publish.sh` posts it), not into
  the descriptions, which become commit messages.

**Build the stages** in a scratch clone at the current main, never in the workspace: one commit
per pull request, each the previous stage plus its change. When the work already exists as one
diff in the workspace, commit it once in the scratch clone (section 6 shows how), then split that
commit into the stages. The last stage must be exactly the work: `kit.py build --workspace .`
compares the files.

**Test the branches against each other:**

- Every stage passes the CI checks on its own:
  `$SKILL/verify_stages.sh SCRATCH refs/kits/<topic>/stage-1 refs/kits/<topic>/stage-2 ...`.
- Each pull request contains only its own change: read `git diff stage-<N-1> stage-<N>` for every
  stage. After a restack, `git range-diff OLD_BASE..OLD_TIP NEW_BASE..NEW_TIP` must show the same
  patches; when main did not move, the trees must be the same too.
- Rehearse the merges: `$SKILL/rehearse.sh --repo SCRATCH KIT` publishes against a local origin,
  applies the fixes, adds a review commit, squash-merges every pull request in order with a
  restack after each, and checks every tree on the way.
- On GitHub, the CI of each pull request then tests exactly its stage: its branch merged into the
  branch before it.

## 4. After publishing: reviews, restacks, a moving main

- **A review comment or a failed check** becomes a new commit on the branch that needs it, never an
  amended or rewritten published commit. The branches after it are rebased onto it; with a kit,
  that is a fix (section 6).
- **After each squash merge**, restack. Rebase the next branch onto main without the merged
  commits (`git rebase --onto origin/main <last head of the merged branch> <next branch>`), each
  later branch onto its rebased predecessor in the same way, and push them with
  `--force-with-lease=<branch>:<old tip>`. Then point the next pull request at main
  (`gh pr edit <n> --base main`; GitHub only does it itself when the merged branch is deleted).
  `publish.sh restack` does all of this, and stops before pushing anything if a rebase conflicts.
- **When main moves** under an open stack (other merges, conflicts in `CHANGELOG.md`), restack as
  well: the bottom branch is rebased onto main, and CI then tests the new combination.

## 5. Fixing a failing check

1. **Find what failed.** With credentials: `gh pr checks <n>` and `gh run view <run> --log-failed`.
   Without them, the check runs of a commit are public, but their logs need a login:

   ```bash
   curl -s https://api.github.com/repos/kalaspuff/fanferro/commits/<sha>/check-runs |
     jq -r '.check_runs[] | "\(.conclusion) \(.name) \(.html_url)"'
   ```

   Ask the user for the log of the failing step. When the workspace is shared, they can save
   `gh run view --job <id> --log-failed` to a file there.
2. **Reproduce it on the exact commit.** Fetch the PR's head into the scratch clone (`git fetch
   https://github.com/kalaspuff/fanferro.git refs/pull/<n>/head`, read only) and run the failing
   job's command with the same Python (`uv run -p 3.12 ...`). A failure only on MiniStack, or
   only in `cargo test --test aws_emulator`, is usually a protocol subtlety rather than flakiness:
   see `fanferro-e2e-testing` and ADR 0007. For a platform you cannot run (Windows, macOS), work
   out what differs there: clock resolution (Windows' clock advances in steps of about 15.6 ms,
   so two timestamps can be equal and short timeouts round), path separators, line endings, the
   event loop (the proactor loop on Windows), signals, case-insensitive or locked files. Emulate
   it so that the test fails first and passes after the fix, for example with a frozen or coarse
   clock in a fixture.
3. **Fix it on the earliest branch that has the cause** (`git log -S`, `git blame` on the stage
   commits), as a commit of its own (`test: ...`, `fix(<scope>): ...`).
4. **Verify that stage and every stage after it** again, then publish the fix: with credentials,
   push it and restack the branches after it; with a kit, add it with `kit.py fix` (section 6).

## 6. Without GitHub credentials: publish kits

When you cannot push, or the workspace is the user's checkout and you must not commit there, hand
over a **publish kit**: a directory `.stacks/<topic>/` in the workspace that ignores itself, with
each pull request's patch, commit message and description, the tested trees, and `publish.sh`.
The user runs it on their machine. It builds the commits in a temporary worktree with the user's
identity and refuses to push anything that is not exactly the tested tree. Later it also restacks
after merges and applies fixes.

```bash
SCRATCH=~/.cache/fanferro-stack/<topic>     # outside the workspace
git clone --quiet https://github.com/kalaspuff/fanferro.git "$SCRATCH"   # read access is enough
# Commit the workspace's files on the commit the workspace is on, then move them onto main:
git -C "$SCRATCH" fetch --quiet "$PWD" HEAD && git -C "$SCRATCH" switch --quiet --detach FETCH_HEAD
git -C "$SCRATCH" --work-tree="$PWD" add --all
git -C "$SCRATCH" commit --quiet -F <message> && git -C "$SCRATCH" reset --quiet --hard
git -C "$SCRATCH" rebase --quiet origin/main   # what main has already drops out
git -C "$SCRATCH" show --stat HEAD             # only your files?
# The descriptions go in .stacks/<topic>/prs/1.md, 2.md, ...
python3 "$SKILL/kit.py" build --repo "$SCRATCH" --base origin/main --tip HEAD \
  --prefix <topic>-stack/ --names 1-<name>,2-<name> --workspace . .stacks/<topic>
"$SKILL/verify_stages.sh" "$SCRATCH" refs/kits/<topic>/stage-1 refs/kits/<topic>/stage-2
"$SKILL/rehearse.sh" --repo "$SCRATCH" .stacks/<topic>   # also with --bash <bash 3.2>: kits.md
```

Committing on the workspace's own commit matters: when main has moved past the workspace (the
user merged something meanwhile), a commit of the workspace's files on top of main would undo
those changes. For a stack, split the commit: `git reset --quiet origin/main` leaves the changes
in the scratch clone's working tree, then add and commit each stage's files in order, editing a
file to its intermediate state when a stage has only part of it. For a single pull request,
`--prefix docs/ --names <topic>` gives the branch `docs/<topic>`. `--workspace .` checks that
every file the pull requests change is exactly as in the workspace, and lists the other
differences (a newer main, or a change missing from the stack). `kit.py` also warns about titles,
line widths and placeholders: fix what it reports and build again.

**Hand over** the commands, one line on what each does, and what you verified:

```bash
.stacks/<topic>/publish.sh --dry-run   # builds and verifies the commits, pushes nothing
.stacks/<topic>/publish.sh             # pushes the branches and opens the pull requests
.stacks/<topic>/publish.sh restack     # after each squash merge (a stack only)
```

Until the user reports back, the work is handed over, not published.

**Fixes after publishing**, for a failed check or a review comment: never rebuild a published kit
(`kit.py build` refuses once `forks` exists), because the pull requests and their reviews belong
to the pushed commits. Add a fix instead:

```bash
git -C "$SCRATCH" switch --detach refs/kits/<topic>/stage-<n>   # the stage as published
# change, test and commit it (a conventional message with the trailers)
python3 "$SKILL/kit.py" fix --repo "$SCRATCH" --name <fix> --commit HEAD .stacks/<topic>
"$SKILL/verify_stages.sh" "$SCRATCH" refs/kits/<topic>/stage-<n> ... refs/kits/<topic>/stage-<last>
"$SKILL/rehearse.sh" --repo "$SCRATCH" .stacks/<topic>
```

The user then runs `.stacks/<topic>/publish.sh fix <fix> --dry-run`, and the same without
`--dry-run`: the fix becomes a new commit on its branch, the later branches are rebased onto it,
and every tree is checked before anything is pushed with a lease. Fixes are applied in the order
they were added.

**When everything is merged**, the kit directory and the local branches can go (`restack` prints
the command).

## 7. Changing these tools

After any change, run `$SKILL/selftest.sh`, `$SKILL/selftest.sh --bash <bash 3.2>` (macOS ships
bash 3.2; kits.md shows how to build one) and `shellcheck $SKILL/*.sh`. A new `gh` call in
`publish.sh` also needs support in `gh_stub.py`, which refuses options it does not know.

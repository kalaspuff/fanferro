# Publish kits

A publish kit hands pull requests over from an agent that cannot push to a user who can: an agent
without GitHub credentials, or one working in the user's own checkout, mounted into its machine,
where it must not commit. The agent builds and tests everything; the user runs one script, which
can only publish exactly what was tested. An agent that can push never needs a kit: it pushes and
opens its pull requests itself (SKILL.md, sections 2-5).

The tools are in `scripts/` next to this file (`$SKILL` in SKILL.md): `kit.py` builds and checks
kits, `publish.sh` is what the user runs, `verify_stages.sh` runs the CI checks on every stage,
`rehearse.sh` rehearses a kit against a local origin with a stub `gh`, and `selftest.sh` tests the
tools. SKILL.md's rules for titles, descriptions and stacks apply to the pull requests of a kit.

## Building a kit

Build the pull requests in a scratch clone at the current main, never in the workspace: one commit
per pull request, each the previous stage plus its change. A kit is a directory `.stacks/<topic>/`
in the workspace that ignores itself, with each pull request's patch, commit message and
description, the tested trees, and `publish.sh`. The user runs it on their machine. It builds the
commits in a temporary worktree with the user's identity and refuses to push anything that is not
exactly the tested tree. Later it also restacks after merges and applies fixes.

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
"$SKILL/rehearse.sh" --repo "$SCRATCH" .stacks/<topic>   # also with --bash <bash 3.2>: see below
```

Committing on the workspace's own commit matters: when main has moved past the workspace (the
user merged something meanwhile), a commit of the workspace's files on top of main would undo
those changes. For a stack, split the commit: `git reset --quiet origin/main` leaves the changes
in the scratch clone's working tree, then add and commit each stage's files in order, editing a
file to its intermediate state when a stage has only part of it. For a single pull request,
`--prefix docs/ --names <topic>` gives the branch `docs/<topic>`. `--workspace .` checks that
every file the pull requests change is exactly as in the workspace, and lists the other
differences (a newer main, or a change missing from the stack). `kit.py` also warns about titles,
commit messages wider than 72 columns, descriptions wrapped at more than 72 (a paragraph on one
line is fine) and placeholders: fix what it reports and build again.

Test the stages as SKILL.md describes, on the kit's refs: every stage passes `verify_stages.sh`
(which also keeps the checks out of the user's checkout), each stage contains only its own change
(`git diff stage-<N-1> stage-<N>`), and `rehearse.sh` publishes the kit against a local origin,
applies the fixes, adds a review commit, squash-merges every pull request in order with a restack
after each, and checks every tree on the way.

## Handing a kit over

Tell the user, in this order:

- what the pull requests are, by their titles;
- the commands (`--dry-run` first), and that they run on the user's machine with git and an
  authenticated `gh`:

  ```bash
  .stacks/<topic>/publish.sh --dry-run   # builds and verifies the commits, pushes nothing
  .stacks/<topic>/publish.sh             # pushes the branches and opens the pull requests
  .stacks/<topic>/publish.sh restack     # after each squash merge (a stack only)
  ```

- what was verified: the CI checks per stage (which Pythons, the emulator), the rehearsal, and the
  match with the workspace;
- for a stack, that each merge is followed by `restack`.

Until the user reports back, the work is handed over, not published. After the merge, the user's
working tree still holds the changes, uncommitted. When their main has no commits of its own,
`git switch main && git fetch && git reset origin/main` lines the checkout up without touching any
file.

## Following up on a kit's pull requests

- **Restacks.** `publish.sh restack`, run by the user after each squash merge, rebases the next
  branch onto main without the merged commits, each later branch onto its rebased predecessor,
  pushes them with `--force-with-lease=<branch>:<old tip>` and points the next pull request at
  main. It stops before pushing anything if a rebase conflicts.
- **Failing checks.** The check runs of a commit are public, but their logs need a login:

  ```bash
  curl -s https://api.github.com/repos/kalaspuff/fanferro/commits/<sha>/check-runs |
    jq -r '.check_runs[] | "\(.conclusion) \(.name) \(.html_url)"'
  ```

  Ask the user for the log of the failing step. When the workspace is shared, they can save
  `gh run view --job <id> --log-failed` to a file there. Fetch the pull request's head into the
  scratch clone to reproduce it (`git fetch https://github.com/kalaspuff/fanferro.git
  refs/pull/<n>/head`, read only), then fix it as SKILL.md section 5 describes.
- **Fixes after publishing**, for a failed check or a review comment: never rebuild a published kit
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
- **When everything is merged**, the kit directory and the local branches can go (`restack` prints
  the command).

## Files

```text
.stacks/<topic>/
  .gitignore        "*": the kit never shows up in git status, and is never committed
  README.md         for the user: the branches, the commands, the fixes
  publish.sh        a copy of scripts/publish.sh (a kit keeps the version it was tested with)
  stack.conf        TOPIC, BASE_COMMIT, PREFIX, NAMES (one per branch), TREES (one per branch),
                    FIXES (in the order they are applied); read, never sourced, by publish.sh
  patches/N.patch   `git diff --binary --full-index` of stage N against stage N-1
  messages/N.txt    the commit message of stage N; its first line is the pull request's title
  prs/N.md          the description of pull request N
  fixes/NAME.patch  a fix after publishing: its diff,
  fixes/NAME.txt      its commit message,
  fixes/NAME.trees    and "N BEFORE AFTER" for its branch and each one after it
  published/        stack.conf and patches/ as first published (kit.py fix keeps them; rehearse.sh
                    replays the user's history from them)
  forks             written by publish.sh on the user's machine: the commit each branch was built on
```

`kit.py build` writes everything but `forks`, taking the descriptions from `prs/` (the agent writes
them first) or `--bodies DIR`. `kit.py fix` adds a fix and makes `patches/` and `TREES` those of
the fixed stack, so that a
first `publish` after a fix builds the fixed stack and a repeated one finds the branches up to
date. `kit.py check` validates a kit: titles, widths, files, the chain of fixes and, with
`--repo`, that the patches rebuild the recorded trees.

## What publish.sh does

- **publish** (the default command; `--dry-run` stops after the verification). It fetches, then
  in a temporary worktree at `origin/main` applies each patch (3-way if main moved) and commits it
  with the user's identity, skipping commit hooks. If `origin/main` is still the kit's base, it
  compares every commit's tree with `TREES` and stops on any difference; if main moved, it warns
  that the combination is untested. Then it notes the files where the working tree differs from
  the stack, reuses branches left by an earlier run when their trees match, creates the local
  branches, pushes them, records the fork points and opens the pull requests (each based on the
  branch before it). For a stack, it comments the order on each pull request.
- **restack** reads the state of the pull requests. The lowest open branch is rebased onto main
  from its fork point (the merged pull request's last head, the recorded fork, or the merge base),
  and every later branch onto its rebased predecessor. Review commits stay. The branches are
  pushed with `--force-with-lease=<branch>:<old tip>`, the lowest open pull request is pointed at
  main, and the fork points are recorded. If a rebase conflicts, nothing is pushed, the script
  prints the commands to resolve that branch by hand, and running `restack` again continues with
  the branches after it.
- **fix NAME** commits `fixes/NAME.patch` as a new commit on its branch and rebases the branches
  after it. When every branch is as published (or has the fix already), each resulting tree must be
  the tested one. Otherwise (review commits) it warns, keeps those commits and lets CI judge. It
  pushes with a lease, leaves the bases alone, and does nothing when run again.
- **status** shows each branch locally and on the remote, and its pull request.

Every command takes `--dry-run`, `--yes`, `--remote NAME` and `--main NAME`.

## Rules for scripts that users run

1. **Portable shell.** macOS has bash 3.2 and BSD tools, so avoid associative arrays, `mapfile`,
   `${var,,}`, `declare -n`, `|&`, negative array indexes and empty arrays under `set -u`. Do not
   use `sed -i` or `\n` in sed replacements, and give `mktemp` a template. Use `set -euo pipefail`
   and `set -f`, and quote everything. Word lists may hold only validated words, such as names and
   ids.
2. **Never touch the user's checkout.** Work in a temporary `git worktree add --detach` and remove
   it in an `EXIT` trap, together with the temporary files.
3. **Verify by tree id before pushing,** and fail closed: "... nothing was pushed".
4. **Idempotent.** A run after a failure continues where it stopped; a run after success does
   nothing. Find earlier work by content (trees), not by commit ids, which change with every
   build.
5. **A dry run for every command, and a confirmation before pushing** (`--yes` skips it).
6. **Force-push only with `--force-with-lease=<ref>:<expected sha>`.** Never delete remote
   branches, never merge, never touch branches the kit does not own.
7. **On conflicts, stop and print the exact commands** to resolve them by hand, and make the next
   run continue from there.
8. **Messages state what happened and what to do next**; the README says what the kit contains.

## Testing such scripts

After any change to these tools, run `$SKILL/selftest.sh`, `$SKILL/selftest.sh --bash <bash 3.2>`
(macOS ships bash 3.2; see below) and `shellcheck $SKILL/*.sh`. A new `gh` call in `publish.sh`
also needs support in `gh_stub.py`, which refuses options it does not know.

- `$SKILL/gh_stub.py` plays the GitHub CLI against a local bare repository. Pull requests live in
  a JSON file, `--jq` filters run through the real `jq`, and `pr merge --squash
  [--delete-branch]` merges and retargets like GitHub does. `GH_STUB_FAIL_CREATE` makes opening a
  pull request fail, `GH_STUB_UNAUTHENTICATED` logs it out, and `GH_STUB_HOOK` with
  `GH_STUB_HOOK_MATCH` runs a command at a chosen call (someone else pushing in the middle of a
  run).
- `$SKILL/rehearse.sh --repo SCRATCH KIT` runs a real kit end to end: a failed publish and its
  continuation, a repeated publish, the fixes in order, a review commit, the merges and restacks.
  It checks trees, parents, authors, titles, bases, descriptions and comments.
- `$SKILL/selftest.sh` runs the tools on a synthetic repository, with a stack of three pull
  requests, a fix and a single pull request. The files include binary files, a mode change, a
  deletion, a symlink and a name with a space. It also covers the failure paths: a logged-out
  `gh`, a wrong tree, foreign branches, a moved main (with a 3-way merge), a conflicting main, a
  conflicting review commit resolved by hand, a race with another push, merges out of order and
  fixes that do not apply or do not give the tested tree. When you change a script, also break it
  on purpose and check that the self-test fails.
- Run the self-test with a real bash 3.2. Its shipped parser is stale, so building it needs bison,
  and GCC 14 refuses its old C unless the build tools get the same flags:

  ```bash
  curl -LO https://ftp.gnu.org/gnu/bash/bash-3.2.57.tar.gz
  # sha256 3fa9daf85ebf35068f090ce51283ddeeb3c75eb5bc70b1a4a7cb05868bfe06a4
  tar -xzf bash-3.2.57.tar.gz && cd bash-3.2.57
  ./configure --build="$(uname -m)-unknown-linux-gnu" --prefix="$HOME/.cache/bash32" \
    --without-bash-malloc CFLAGS="-O1 -std=gnu89 -w"
  rm -f y.tab.c y.tab.h   # apt-get install bison
  make YACC="bison -y" CFLAGS_FOR_BUILD="-O1 -std=gnu89 -w" && make install
  "$SKILL/selftest.sh" --bash "$HOME/.cache/bash32/bin/bash"   # SKILL: see SKILL.md
  ```

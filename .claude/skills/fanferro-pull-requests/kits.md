# Publish kits

A publish kit hands pull requests over from an agent that cannot push to a user who can. The
agent builds and tests everything; the user runs one script, which can only publish exactly what
was tested. The tools are in `scripts/` next to this file (`$SKILL` in SKILL.md); `selftest.sh`
tests them.

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
- Run the self-test with a real bash 3.2. Its shipped parser is stale, so building it needs bison:

  ```bash
  curl -LO https://ftp.gnu.org/gnu/bash/bash-3.2.57.tar.gz
  # sha256 3fa9daf85ebf35068f090ce51283ddeeb3c75eb5bc70b1a4a7cb05868bfe06a4
  tar -xzf bash-3.2.57.tar.gz && cd bash-3.2.57
  ./configure --build="$(uname -m)-unknown-linux-gnu" --prefix="$HOME/.cache/bash32" \
    --without-bash-malloc CFLAGS="-O1 -std=gnu89 -w"
  rm -f y.tab.c y.tab.h && make YACC="bison -y" && make install   # apt-get install bison
  "$SKILL/selftest.sh" --bash "$HOME/.cache/bash32/bin/bash"   # SKILL: see SKILL.md
  ```

## Handing a kit over

Tell the user, in this order:

- what the pull requests are, by their titles;
- the commands (`--dry-run` first), and that they run on the user's machine with git and an
  authenticated `gh`;
- what was verified: the CI checks per stage (which Pythons, the emulator), the rehearsal, and the
  match with the workspace;
- for a stack, that each merge is followed by `restack`.

After the merge, the user's working tree still holds the changes, uncommitted. When their main has
no commits of its own, `git switch main && git fetch && git reset origin/main` lines the checkout up
without touching any file.

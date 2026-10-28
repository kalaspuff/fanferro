#!/usr/bin/env bash
# Tests the publish-kit tools (kit.py, publish.sh, rehearse.sh, gh_stub.py) on a synthetic
# repository: a stack of three pull requests with a fix, a single pull request, and the failure
# paths (conflicts, foreign branches, a moved main, merges out of order, a logged-out gh). Run it
# after changing any of them; it works in a temporary directory and takes about a minute.
#
#   selftest.sh [--bash BASH]   BASH runs publish.sh (for example a build of bash 3.2, as on macOS)
set -euo pipefail

SCRIPTS=$(cd "$(dirname "$0")" && pwd -P)
RUN_BASH=bash
if [ "${1:-}" = --bash ]; then RUN_BASH=${2:?--bash needs a bash}; fi
T=$(mktemp -d "${TMPDIR:-/tmp}/kit-selftest.XXXXXX")
T=$(cd "$T" && pwd -P) # macOS: $TMPDIR is under /var, a symlink to /private/var
OUT=$T/out.txt
mkdir -p "$T/tmp"
export TMPDIR=$T/tmp # the tools' temporary files, which must all be gone at the end
trap 'echo "left $T for a look" >&2' ERR
fail() {
  echo "SELFTEST FAILED: $*" >&2
  if [ -s "$OUT" ]; then sed 's/^/  | /' "$OUT" >&2; fi
  echo "left $T for a look" >&2
  trap - ERR
  exit 1
}
step() { printf '\n##### %s\n' "$*"; }
agent() { git -C "$AGENT" -c user.name=Agent -c user.email=agent@example.com "$@"; }
lines() { # lines PREFIX N: the lines "PREFIX 1" .. "PREFIX N"
  local index=1
  while [ "$index" -le "$2" ]; do
    printf '%s %s\n' "$1" "$index"
    index=$((index + 1))
  done
}
replace_line() { # replace_line FILE N TEXT
  awk -v n="$2" -v text="$3" 'NR == n { print text; next } { print }' "$1" >"$1.new" && mv "$1.new" "$1"
}

# ---- the agent's scratch repository: a base on main, a stack of three, a single change ----------

step "the scratch repository"
AGENT=$T/agent
git init --quiet -b main "$AGENT"
mkdir -p "$AGENT/src" "$AGENT/docs" "$AGENT/scripts"
printf '# Demo\n' >"$AGENT/README.md"
printf '# Changelog\n\n## [Unreleased]\n' >"$AGENT/CHANGELOG.md"
lines line 20 >"$AGENT/src/app.txt"
lines guide 20 >"$AGENT/docs/guide.md"
printf 'old\n' >"$AGENT/docs/old.md"
printf 'spaces\n' >"$AGENT/docs/with space.md"
printf '#!/bin/sh\necho run\n' >"$AGENT/scripts/run.sh"
agent add -A
agent commit --quiet -m "chore: the base"
BASE=$(git -C "$AGENT" rev-parse HEAD)
agent switch --quiet -c stack
replace_line "$AGENT/src/app.txt" 2 "line 2, fixed"
mkdir -p "$AGENT/tests"
printf 'one\n' >"$AGENT/tests/one.txt"
printf -- '- Line 2 is right.\n' >>"$AGENT/CHANGELOG.md"
agent add -A
agent commit --quiet -F - <<'EOF'
fix(app): line 2 says what it means

The second line was wrong; a test covers it.

Co-Authored-By: Example Agent <agent@example.com>
EOF
replace_line "$AGENT/src/app.txt" 15 "line 15, changed"
replace_line "$AGENT/docs/guide.md" 5 "guide 5, rewritten"
printf 'spaces, changed\n' >"$AGENT/docs/with space.md"
printf -- '- A longer guide.\n' >>"$AGENT/CHANGELOG.md"
agent add -A
agent commit --quiet -m "feat(guide): a longer guide" -m "It explains more."
git -C "$AGENT" rm --quiet docs/old.md
mkdir -p "$AGENT/assets"
python3 -c 'import sys; sys.stdout.buffer.write(bytes(range(256)) * 4)' >"$AGENT/assets/logo.bin"
chmod +x "$AGENT/scripts/run.sh"
replace_line "$AGENT/docs/guide.md" 18 "guide 18, new"
ln -s guide.md "$AGENT/docs/latest.md"
agent add -A
agent commit --quiet -m "feat!: drop the old docs" -m "BREAKING CHANGE: docs/old.md is gone."
agent switch --quiet -c single main
printf '# Demo\n\nNow with a sentence.\n' >"$AGENT/README.md"
agent commit --quiet -am "docs: say what the demo is"
agent switch --quiet --detach main
mkdir -p "$T/bodies" "$T/single-body"
for index in 1 2 3; do
  # shellcheck disable=SC2016 # Markdown backticks
  printf '## Summary\n\nPull request %s of the demo stack, wrapped at\n72 columns.\n\n```text\nA code block may be as long as it likes, GitHub does not rewrap it at all.\n```\n' "$index" >"$T/bodies/$index.md"
done
printf 'A sentence for the README.\n' >"$T/single-body/1.md"
WS=$T/workspace # the user's checkout, where kits live
git clone --quiet "$AGENT" "$WS"

step "kit.py build: a stack of three"
python3 "$SCRIPTS/kit.py" build --repo "$AGENT" --base main --tip stack --prefix demo-stack/ \
  --names 1-app,2-guide,3-cleanup --bodies "$T/bodies" "$WS/.stacks/demo" >"$OUT" 2>&1 || fail "build failed"
grep -q "✓ the kit is consistent" "$OUT" || fail "the new kit is not consistent"
[ -z "$(git -C "$WS" status --porcelain)" ] || fail "the kit shows up in git status"
[ "$(git -C "$AGENT" rev-parse refs/kits/demo/stage-3)" = "$(git -C "$AGENT" rev-parse stack)" ] || fail "no stage refs"
# shellcheck disable=SC2016 # Markdown backticks
grep -q '^| 2 | `demo-stack/2-guide` | feat(guide): a longer guide |$' "$WS/.stacks/demo/README.md" || fail "README table"

step "kit.py check finds broken kits"
cp -R "$WS/.stacks/demo" "$T/broken"
printf 'Fix the app\n' >"$T/broken/messages/1.txt"
printf 'not a patch\n' >"$T/broken/patches/2.patch"
if python3 "$SCRIPTS/kit.py" check --repo "$AGENT" "$T/broken" >"$OUT" 2>&1; then fail "check accepted a broken kit"; fi
grep -q "is not a conventional title" "$OUT" || fail "check missed the title"
printf 'fix(app): line 2 says what it means\n' >"$T/broken/messages/1.txt"
if python3 "$SCRIPTS/kit.py" check --repo "$AGENT" "$T/broken" >"$OUT" 2>&1; then fail "check accepted a broken patch"; fi
grep -q "patches: patch 2 does not apply" "$OUT" || fail "check missed the patch"

step "kit.py check: paragraphs on one line pass, paragraphs wrapped wider than 72 columns do not"
cp -R "$WS/.stacks/demo" "$T/widths"
WORDS=$(lines word 30 | tr '\n' ' ')
printf '## Summary\n\n%s\n\n- %s\n  - %s\n' "$WORDS" "$WORDS" "$WORDS" >"$T/widths/prs/1.md"
python3 "$SCRIPTS/kit.py" check "$T/widths" >"$OUT" 2>&1 || fail "check failed on long one-line paragraphs"
grep -q "prs/1.md" "$OUT" && fail "a warning for paragraphs on one line"
printf '%s\n%s\n' "$(printf '%s' "$WORDS" | cut -c1-98)" "and the rest of the paragraph." >"$T/widths/prs/2.md"
python3 "$SCRIPTS/kit.py" check "$T/widths" >"$OUT" 2>&1 || fail "check failed on a paragraph wrapped at 98"
grep -q "prs/2.md: line 1 is longer than 72 characters and wrapped" "$OUT" || fail "check missed the wrapped paragraph"

step "kit.py build: a single pull request"
python3 "$SCRIPTS/kit.py" build --repo "$AGENT" --base main --tip single --prefix docs/ --names demo-readme \
  --bodies "$T/single-body" "$WS/.stacks/single" >"$OUT" 2>&1 || fail "build of the single pull request failed"
step "kit.py build --workspace: the pull requests' files must be the workspace's"
mkdir -p "$T/files"
git -C "$AGENT" archive single | tar -x -C "$T/files"
build_single() {
  python3 "$SCRIPTS/kit.py" build --repo "$AGENT" --base main --tip single --prefix docs/ --names demo-readme \
    --bodies "$T/single-body" --workspace "$T/files" "$T/workspace-kit" >"$OUT" 2>&1
}
build_single || fail "build with --workspace failed"
grep -q "the files the pull requests change are exactly those of $T/files" "$OUT" || fail "no workspace match reported"
grep -q "warning:" "$OUT" && fail "a warning for a workspace that matches"
printf 'stray\n' >"$T/files/stray.txt"
build_single || fail "a file no pull request changes stopped the build"
grep -q "also differs in 1 file(s) that no pull request changes: stray.txt" "$OUT" || fail "the extra file is not reported"
printf '# Demo\n\nAnother sentence.\n' >"$T/files/README.md"
if build_single; then fail "build accepted a workspace with another version of a changed file"; fi
grep -q "^M[[:space:]]*README.md$" "$OUT" || fail "the changed file is not reported"

step "rehearse.sh: the single pull request"
"$SCRIPTS/rehearse.sh" --repo "$AGENT" --bash "$RUN_BASH" "$WS/.stacks/single" >"$OUT" 2>&1 || fail "the rehearsal failed"
grep -q "REHEARSAL OK: 1 pull request(s)" "$OUT" || fail "no REHEARSAL OK"

step "rehearse.sh: the stack of three"
"$SCRIPTS/rehearse.sh" --repo "$AGENT" --bash "$RUN_BASH" "$WS/.stacks/demo" >"$OUT" 2>&1 || fail "the rehearsal failed"
grep -q "REHEARSAL OK: 3 pull request(s), fixes: none" "$OUT" || fail "no REHEARSAL OK"

step "kit.py fix: a fix for the second pull request"
cp -R "$WS/.stacks/demo" "$T/demo-as-published"
printf '2 %s\n' "$(git -C "$AGENT" rev-parse refs/kits/demo/stage-1)" >"$WS/.stacks/demo/forks" # as if published
if python3 "$SCRIPTS/kit.py" build --repo "$AGENT" --base main --tip stack --prefix demo-stack/ \
  --names 1-app,2-guide,3-cleanup --bodies "$T/bodies" "$WS/.stacks/demo" >"$OUT" 2>&1; then
  fail "build overwrote a published kit"
fi
grep -q "was published (it has forks)" "$OUT" || fail "wrong message for a published kit"
agent switch --quiet --detach refs/kits/demo/stage-2
replace_line "$AGENT/src/app.txt" 16 "line 16, covered"
printf 'one\nsixteen\n' >"$AGENT/tests/one.txt"
agent commit --quiet -am "test(app): cover line 16" -m "Co-Authored-By: Example Agent <agent@example.com>"
python3 "$SCRIPTS/kit.py" fix --repo "$AGENT" --name cover-16 --commit HEAD "$WS/.stacks/demo" >"$OUT" 2>&1 || fail "kit.py fix failed"
grep -q "✓ the kit is consistent" "$OUT" || fail "the fixed kit is not consistent"
[ "$(sed -n 's/^FIXES=//p' "$WS/.stacks/demo/stack.conf")" = cover-16 ] || fail "FIXES"
[ "$(wc -l <"$WS/.stacks/demo/fixes/cover-16.trees" | tr -d ' ')" = 2 ] || fail "the trees of the fix"
[ "$(git -C "$AGENT" rev-parse 'refs/kits/demo/stage-2^{tree}')" = "$(git -C "$AGENT" rev-parse 'HEAD^{tree}')" ] || fail "stage-2 ref"
cmp -s "$WS/.stacks/demo/published/stack.conf" "$T/demo-as-published/stack.conf" || fail "published/ is not the kit as published"
step "rehearse.sh: the stack, published, then fixed"
rm -f "$WS/.stacks/demo/forks"
"$SCRIPTS/rehearse.sh" --repo "$AGENT" --bash "$RUN_BASH" "$WS/.stacks/demo" >"$OUT" 2>&1 || fail "the rehearsal failed"
grep -q "REHEARSAL OK: 3 pull request(s), fixes: cover-16" "$OUT" || fail "no REHEARSAL OK"

step "kit.py fix: a fix that conflicts with a later branch is refused"
agent switch --quiet --detach refs/kits/demo/stage-2
replace_line "$AGENT/docs/guide.md" 18 "guide 18, from the fix"
agent commit --quiet -am "fix(guide): line 18"
cp -R "$WS/.stacks/demo" "$T/demo-before-conflict"
if python3 "$SCRIPTS/kit.py" fix --repo "$AGENT" --name line-18 --commit HEAD "$WS/.stacks/demo" >"$OUT" 2>&1; then
  fail "kit.py fix accepted a conflicting fix"
fi
grep -q "the fix conflicts with a branch after demo-stack/2-guide" "$OUT" || fail "wrong message for a conflicting fix"
diff -r "$WS/.stacks/demo" "$T/demo-before-conflict" >/dev/null || fail "a refused fix changed the kit"

# ---- failure paths, each in a sandbox of its own --------------------------------------------------

KIT=$T/demo-as-published # the stack as published, without fixes
sandbox() { # sandbox NAME: origin (main = the base), the user's clone with the kit, the stub gh
  S=$T/edge-$1
  mkdir -p "$S/bin"
  ORIGIN=$S/origin.git
  U=$S/user
  git init --quiet --bare "$ORIGIN"
  git --git-dir="$ORIGIN" fetch --quiet "$AGENT" main:main
  git --git-dir="$ORIGIN" symbolic-ref HEAD refs/heads/main
  git clone --quiet "$ORIGIN" "$U"
  git -C "$U" config user.name "Edge User"
  git -C "$U" config user.email edge@example.com
  mkdir -p "$U/.stacks"
  cp -R "$KIT" "$U/.stacks/demo"
  rm -f "$U/.stacks/demo/forks"
  printf '#!/bin/sh\nexec python3 "%s/gh_stub.py" "$@"\n' "$SCRIPTS" >"$S/bin/gh"
  chmod +x "$S/bin/gh"
  export PATH="$S/bin:$PATH" GH_STUB_DB="$S/prs.json" GH_STUB_REMOTE="$ORIGIN"
}
run() { (cd "$U" && "$RUN_BASH" .stacks/demo/publish.sh "$@") >"$OUT" 2>&1; }
says() { grep -qF -- "$1" "$OUT" || fail "expected: $1"; }
tip() { git --git-dir="$ORIGIN" rev-parse --verify --quiet "refs/heads/$1" || true; }
heads() { git --git-dir="$ORIGIN" for-each-ref --format='%(refname) %(objectname)' refs/heads; }
no_worktrees() { [ "$(git -C "$U" worktree list | wc -l | tr -d ' ')" = 1 ] || fail "a worktree was left behind"; }
commit_on() { # commit_on BRANCH FILE LINE TEXT: someone else's commit on a branch of origin
  rm -rf "$S/other"
  git clone --quiet "$ORIGIN" "$S/other"
  git -C "$S/other" switch --quiet "$1"
  replace_line "$S/other/$2" "$3" "$4"
  git -C "$S/other" -c user.name=Other -c user.email=other@example.com commit --quiet -am "chore: $2 line $3"
  git -C "$S/other" push --quiet origin "$1"
}
merge() { gh pr merge "$(gh pr list --head "$1" --state all --json number --jq '.[0].number')" --squash --delete-branch >"$OUT" 2>&1 || fail "merging $1 failed"; }

step "a logged-out gh: publish stops before anything happens"
sandbox gh
if GH_STUB_UNAUTHENTICATED=1 run --yes; then fail "published without gh"; fi
says "gh is not logged in: run gh auth login"
[ "$(heads)" = "refs/heads/main $BASE" ] || fail "pushed without gh"

step "the dry run compares the working tree with the stack (a file with a space, a symlink, a binary)"
sandbox note
run --dry-run || fail "dry run failed"
says "note: your working tree differs from the stack in: CHANGELOG.md"
for index in 1 2 3; do git -C "$U" apply "$U/.stacks/demo/patches/$index.patch"; done
run --dry-run || fail "dry run failed"
grep -q "note:" "$OUT" && fail "a note although the working tree is the stack"
printf 'mine\n' >>"$U/docs/with space.md"
run --dry-run || fail "dry run failed"
says "note: your working tree differs from the stack in: docs/with space.md"

step "a kit whose patches do not give the tested trees: nothing is created or pushed"
sandbox wrong-tree
sed "s/^TREES=\([0-9a-f]*\) /TREES=\1 0000000000000000000000000000000000000000 /; s/ [0-9a-f]* \([0-9a-f]*\)$/ \1/" \
  "$U/.stacks/demo/stack.conf" >"$S/stack.conf"
mv "$S/stack.conf" "$U/.stacks/demo/stack.conf"
grep -q "^TREES=[0-9a-f]* 0\{40\} [0-9a-f]*$" "$U/.stacks/demo/stack.conf" || fail "the tree was not replaced"
if run --yes; then fail "published a stack that is not the tested one"; fi
says "demo-stack/2-guide would be tree"
[ "$(heads)" = "refs/heads/main $BASE" ] || fail "pushed anyway"
[ -z "$(git -C "$U" branch --list 'demo-stack/*')" ] || fail "created branches"
no_worktrees

step "foreign branches with the kit's names are refused"
sandbox foreign
git -C "$U" branch demo-stack/2-guide main
if run --yes; then fail "accepted a foreign branch"; fi
says "branches of this kit exist with other content"
[ "$(heads)" = "refs/heads/main $BASE" ] || fail "pushed anyway"
no_worktrees

step "main moved past the base (next to a line the stack changes): a 3-way merge, with a warning"
sandbox moved
commit_on main src/app.txt 4 "line 4, from main"
run --dry-run || fail "dry run failed"
says "warning: origin/main has moved past"
run --yes || fail "publish on a moved main failed"
[ "$(git --git-dir="$ORIGIN" rev-parse "$(tip demo-stack/1-app)^")" = "$(tip main)" ] || fail "branch 1 is not on the new main"
git --git-dir="$ORIGIN" show "$(tip demo-stack/1-app):src/app.txt" | grep -q "^line 4, from main$" || fail "main's change is lost"
git --git-dir="$ORIGIN" show "$(tip demo-stack/1-app):src/app.txt" | grep -q "^line 2, fixed$" || fail "the stack's change is lost"

step "main changed a line the stack changes: nothing is created or pushed"
sandbox conflict
commit_on main src/app.txt 2 "line 2, someone else's"
if run --yes; then fail "published over a conflict"; fi
says "does not apply to origin/main; nothing was pushed"
[ -z "$(git -C "$U" branch --list 'demo-stack/*')" ] || fail "created branches"
no_worktrees

step "a review commit that conflicts with the next branch: restack stops; resolved by hand, it goes on"
sandbox review
run --yes || fail "publish failed"
commit_on demo-stack/1-app src/app.txt 15 "line 15, from the review"
merge demo-stack/1-app
state=$(heads)
if run restack --yes; then fail "restacked over a conflict"; fi
says "conflicts; nothing was pushed"
[ "$(heads)" = "$state" ] || fail "pushed anyway"
no_worktrees
fork=$(sed -n 's/^.*git rebase --onto [0-9a-f]* \([0-9a-f]*\) .*$/\1/p' "$OUT")
[ -n "$fork" ] || fail "no fork point in the instructions"
git -C "$U" fetch --quiet origin
git -C "$U" worktree add --quiet -B demo-stack/2-guide "$S/restack" origin/demo-stack/2-guide
(
  cd "$S/restack"
  if git rebase --quiet --onto origin/main "$fork" >/dev/null 2>&1; then exit 1; fi # it conflicts
  replace_line src/app.txt 15 "line 15, changed"
  git add src/app.txt
  GIT_EDITOR=true git -c user.name="Edge User" -c user.email=edge@example.com rebase --continue >/dev/null
  git push --quiet --force-with-lease origin demo-stack/2-guide
) || fail "resolving by hand failed"
git -C "$U" worktree remove "$S/restack"
run restack --yes || fail "restack after resolving failed"
[ "$(git --git-dir="$ORIGIN" rev-parse "$(tip demo-stack/2-guide)^")" = "$(tip main)" ] || fail "2 is not on main"
[ "$(git --git-dir="$ORIGIN" rev-parse "$(tip demo-stack/3-cleanup)^")" = "$(tip demo-stack/2-guide)" ] || fail "3 is not on 2"
[ "$(gh pr view demo-stack/2-guide --json baseRefName --jq .baseRefName)" = main ] || fail "#2 does not target main"

step "someone pushes to a branch while restack runs: the lease refuses, nothing is overwritten"
sandbox race
run --yes || fail "publish failed"
merge demo-stack/1-app
git clone --quiet "$ORIGIN" "$S/racer"
git -C "$S/racer" switch --quiet demo-stack/3-cleanup
printf 'raced\n' >"$S/racer/RACE.md"
git -C "$S/racer" add RACE.md
git -C "$S/racer" -c user.name=Racer -c user.email=racer@example.com commit --quiet -m "docs: a race"
if GH_STUB_HOOK="git -C '$S/racer' push --quiet origin demo-stack/3-cleanup" GH_STUB_HOOK_MATCH=headRefOid run restack --yes; then
  fail "restack overwrote a concurrent push"
fi
says "pushing demo-stack/3-cleanup failed (did someone push to it meanwhile?)"
[ "$(tip demo-stack/3-cleanup)" = "$(git -C "$S/racer" rev-parse HEAD)" ] || fail "the concurrent push was overwritten"
run restack --yes || fail "restack after the race failed"
[ "$(git --git-dir="$ORIGIN" log -1 --format=%s "$(tip demo-stack/3-cleanup)")" = "docs: a race" ] || fail "the raced commit is lost"
git --git-dir="$ORIGIN" merge-base --is-ancestor "$(tip demo-stack/2-guide)" "$(tip demo-stack/3-cleanup)" || fail "3 is not on 2"

step "main moved without a merge: restack puts the stack on it"
sandbox main-moved
run --yes || fail "publish failed"
commit_on main README.md 1 "# Moved"
run restack --yes || fail "restack failed"
[ "$(git --git-dir="$ORIGIN" rev-parse "$(tip demo-stack/1-app)^")" = "$(tip main)" ] || fail "1 is not on main"
[ "$(git --git-dir="$ORIGIN" rev-parse "$(tip demo-stack/3-cleanup)^")" = "$(tip demo-stack/2-guide)" ] || fail "3 is not on 2"
[ "$(gh pr view demo-stack/2-guide --json baseRefName --jq .baseRefName)" = demo-stack/1-app ] || fail "#2's base changed"

step "a pull request merged before the one below it: restack refuses"
sandbox order
run --yes || fail "publish failed"
merge demo-stack/2-guide
if run restack --yes; then fail "restacked out of order"; fi
says "was merged before an earlier pull request of the stack"
step "publish on a merged kit points to restack"
sandbox merged
run --yes || fail "publish failed"
merge demo-stack/1-app
if run --yes; then fail "published a merged kit again"; fi
says "is merged already: run .stacks/demo/publish.sh restack"

KIT=$WS/.stacks/demo # the kit with the fix cover-16
publish_as_published() { # publishes the kit as it was published, then gives the user the fixed kit
  cp "$KIT/published/stack.conf" "$U/.stacks/demo/stack.conf"
  rm -rf "$U/.stacks/demo/patches" "$U/.stacks/demo/fixes"
  cp -R "$KIT/published/patches" "$U/.stacks/demo/patches"
  run --yes || fail "publish failed"
  rm -rf "$U/.stacks/demo/patches"
  cp -R "$KIT/patches" "$KIT/fixes" "$U/.stacks/demo/"
  cp "$KIT/stack.conf" "$U/.stacks/demo/stack.conf"
}

step "fix with a review commit on a later branch: kept, with a warning"
sandbox fix-review
publish_as_published
commit_on demo-stack/3-cleanup docs/guide.md 19 "guide 19, from the review"
run fix cover-16 --yes || fail "fix failed"
says "warning: the branches have commits this kit did not make"
[ "$(git --git-dir="$ORIGIN" log -1 --format=%s "$(tip demo-stack/3-cleanup)")" = "chore: docs/guide.md line 19" ] || fail "the review commit is lost"
[ "$(git --git-dir="$ORIGIN" log -1 --format=%s "$(tip demo-stack/2-guide)")" = "test(app): cover line 16" ] || fail "no fix commit"
git --git-dir="$ORIGIN" merge-base --is-ancestor "$(tip demo-stack/2-guide)" "$(tip demo-stack/3-cleanup)" || fail "3 is not on the fixed 2"

step "a fix whose result is not the tested tree: nothing is pushed"
sandbox fix-tree
publish_as_published
awk '$1 == 3 { $3 = "0000000000000000000000000000000000000000" } { print }' "$U/.stacks/demo/fixes/cover-16.trees" >"$S/trees"
mv "$S/trees" "$U/.stacks/demo/fixes/cover-16.trees"
state=$(heads)
if run fix cover-16 --yes; then fail "pushed a fix that is not the tested tree"; fi
says "demo-stack/3-cleanup would be tree"
[ "$(heads)" = "$state" ] || fail "pushed anyway"
no_worktrees

step "fix that conflicts with a review commit on its branch: nothing is pushed"
sandbox fix-conflict
publish_as_published
commit_on demo-stack/2-guide src/app.txt 16 "line 16, from the review"
state=$(heads)
if run fix cover-16 --yes; then fail "applied a conflicting fix"; fi
says "fixes/cover-16.patch does not apply to demo-stack/2-guide; nothing was pushed"
[ "$(heads)" = "$state" ] || fail "pushed anyway"
no_worktrees

[ -z "$(ls -A "$T/tmp")" ] || fail "temporary files were left behind: $(ls "$T/tmp")"
trap - ERR
rm -rf "$T"
# shellcheck disable=SC2016 # expanded by the bash that ran publish.sh
echo "SELFTEST OK (publish.sh ran with bash $("$RUN_BASH" -c 'echo "$BASH_VERSION"'))"

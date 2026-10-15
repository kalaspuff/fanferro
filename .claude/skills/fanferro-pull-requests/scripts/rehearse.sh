#!/usr/bin/env bash
# Rehearses a publish kit end to end before it is handed over. A local bare "origin" plays GitHub
# (its main is the kit's base), a clone plays the user (with the changes uncommitted in its working
# tree, as the agent left them), and gh_stub.py plays the GitHub CLI. The rehearsal publishes the
# kit (with a failure on the way, then again), runs it once more, applies the kit's fixes in order,
# adds a review commit, then squash-merges every pull request in order with a restack after each
# merge. It checks every tree, commit, pull request and comment along the way.
#
#   rehearse.sh [--repo REPO] [--bash BASH] [--keep] KIT
#
# REPO has the kit's base commit: the scratch repository the kit was built from (by default, the
# repository the kit is in). It is only read. BASH runs publish.sh (for example a build of bash
# 3.2, the version of macOS). --keep leaves the sandbox for a look; a failed rehearsal always does.
# Needs git 2.38+, python3 and jq.
set -euo pipefail
set -f

SCRIPTS=$(cd "$(dirname "$0")" && pwd -P)
REPO=""
RUN_BASH=bash
KEEP=0
KIT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --repo)
      shift
      REPO=${1:?--repo needs a repository}
      ;;
    --bash)
      shift
      RUN_BASH=${1:?--bash needs a bash}
      ;;
    --keep) KEEP=1 ;;
    -h | --help)
      awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 2
      ;;
    *) KIT=$1 ;;
  esac
  shift
done
[ -n "$KIT" ] || {
  echo "usage: rehearse.sh [--repo REPO] [--bash BASH] [--keep] KIT" >&2
  exit 2
}
KIT=$(cd "$KIT" && pwd -P)
[ -n "$REPO" ] || REPO=$(git -C "$KIT" rev-parse --show-toplevel)
REPO=$(cd "$REPO" && pwd -P)
for tool in git python3 jq; do command -v "$tool" >/dev/null || {
  echo "rehearse.sh needs $tool" >&2
  exit 2
}; done

conf() { sed -n "s/^$1=//p" "$KIT/stack.conf" | tail -n 1; }
# shellcheck disable=SC2086 # word lists are split on purpose
nth() { printf '%s\n' $1 | sed -n "$2p"; }
# shellcheck disable=SC2086
words() { set -- $1 && printf '%s' "$#"; }
BASE=$(conf BASE_COMMIT)
PREFIX=$(conf PREFIX)
NAMES=$(conf NAMES)
TREES=$(conf TREES)
FIXES=$(conf FIXES)
TOPIC=$(conf TOPIC)
COUNT=$(words "$NAMES")
PUBLISHED_TREES=$TREES
if [ -n "$FIXES" ]; then PUBLISHED_TREES=$(sed -n 's/^TREES=//p' "$KIT/published/stack.conf"); fi
KIT_TOP=$(git -C "$KIT" rev-parse --show-toplevel 2>/dev/null || true)
case "$KIT" in
  "$KIT_TOP"/*) KIT_REL=${KIT#"$KIT_TOP"/} ;;
  *) KIT_REL=.stacks/$TOPIC ;;
esac
branch_of() { printf '%s%s' "$PREFIX" "$(nth "$NAMES" "$1")"; }
title_of() { head -n 1 "$KIT/messages/$1.txt"; }

SANDBOX=$(mktemp -d "${TMPDIR:-/tmp}/rehearse.XXXXXX")
ORIGIN=$SANDBOX/origin.git
USER_REPO=$SANDBOX/user
OUT=$SANDBOX/out.txt
OK=0
finish() {
  if [ "$OK" -eq 1 ] && [ "$KEEP" -eq 0 ]; then
    rm -rf "$SANDBOX"
  else
    echo "sandbox: $SANDBOX" >&2
  fi
}
trap finish EXIT
fail() {
  echo "REHEARSAL FAILED: $*" >&2
  if [ -s "$OUT" ]; then sed 's/^/  | /' "$OUT" >&2; fi
  exit 1
}
step() { printf '=== %s\n' "$*"; }
mkdir -p "$SANDBOX/bin"
printf '#!/bin/sh\nexec python3 "%s/gh_stub.py" "$@"\n' "$SCRIPTS" >"$SANDBOX/bin/gh"
chmod +x "$SANDBOX/bin/gh"
export PATH="$SANDBOX/bin:$PATH" GH_STUB_DB="$SANDBOX/prs.json" GH_STUB_REMOTE="$ORIGIN" GH_STUB_LOG="$SANDBOX/gh.log"

publish_kit() { (cd "$USER_REPO" && "$RUN_BASH" "$KIT_REL/publish.sh" "$@") >"$OUT" 2>&1; } # as the user does
expect_output() { grep -qF -- "$1" "$OUT" || fail "expected the output to say: $1"; }
remote_tip() { git --git-dir="$ORIGIN" rev-parse --verify --quiet "refs/heads/$1" || true; }
remote_tree() { git --git-dir="$ORIGIN" rev-parse "refs/heads/$1^{tree}"; }
refs_state() { git --git-dir="$ORIGIN" for-each-ref --format='%(refname) %(objectname)' refs/heads; }
pr_of() { jq -r --arg head "$1" '[.prs[] | select(.head == $head)] | max_by(.number) | .number' "$GH_STUB_DB"; }
pr_get() { jq -r --argjson n "$1" ".prs[] | select(.number == \$n) | .$2" "$GH_STUB_DB"; }
no_worktrees() { [ "$(git -C "$USER_REPO" worktree list | wc -l | tr -d ' ')" = 1 ] || fail "a temporary worktree was left behind"; }
user_status() { git -C "$USER_REPO" status --porcelain | sort; }

# ---- the sandbox --------------------------------------------------------------------------------

step "setting up $SANDBOX (origin's main = ${BASE:0:12}, $COUNT pull request(s), fixes: ${FIXES:-none})"
git init --quiet --bare "$ORIGIN"
git --git-dir="$ORIGIN" fetch --quiet "$REPO" "$BASE" || fail "cannot fetch $BASE from $REPO"
git --git-dir="$ORIGIN" update-ref refs/heads/main "$BASE"
git --git-dir="$ORIGIN" symbolic-ref HEAD refs/heads/main
git clone --quiet "$ORIGIN" "$USER_REPO"
git -C "$USER_REPO" config user.name "Rehearsal User"
git -C "$USER_REPO" config user.email user@example.com
# The user's working tree holds the final stage, uncommitted, as the agent left it.
index=1
while [ "$index" -le "$COUNT" ]; do
  git -C "$USER_REPO" apply --whitespace=nowarn "$KIT/patches/$index.patch" || fail "patch $index does not apply to the base"
  index=$((index + 1))
done
STATUS_BEFORE=$(user_status)
mkdir -p "$USER_REPO/$(dirname "$KIT_REL")"
cp -R "$KIT" "$USER_REPO/$KIT_REL"
USER_KIT=$USER_REPO/$KIT_REL
rm -f "$USER_KIT/forks"
if [ -n "$FIXES" ]; then # the kit as it was published
  cp "$KIT/published/stack.conf" "$USER_KIT/stack.conf"
  rm -rf "$USER_KIT/patches" "$USER_KIT/fixes"
  cp -R "$KIT/published/patches" "$USER_KIT/patches"
fi
[ "$(user_status)" = "$STATUS_BEFORE" ] || fail "the kit shows up in git status (its .gitignore must be '*')"

# ---- publishing ---------------------------------------------------------------------------------

step "dry run"
publish_kit --dry-run || fail "the dry run failed"
expect_output "verified: every branch is exactly the tree that was tested"
index=1
while [ "$index" -le "$COUNT" ]; do
  [ -z "$(remote_tip "$(branch_of "$index")")" ] || fail "the dry run pushed $(branch_of "$index")"
  [ -z "$(git -C "$USER_REPO" branch --list "$(branch_of "$index")")" ] || fail "the dry run created $(branch_of "$index")"
  index=$((index + 1))
done
no_worktrees

step "publish, with opening the last pull request failing"
if GH_STUB_FAIL_CREATE=$(branch_of "$COUNT") publish_kit --yes; then fail "publish went on although gh pr create failed"; fi
expect_output "opening the pull request for $(branch_of "$COUNT") failed; run the script again to continue"
step "publish again: it continues"
publish_kit --yes || fail "publishing again failed"
index=1
while [ "$index" -le "$COUNT" ]; do
  branch=$(branch_of "$index")
  tip=$(remote_tip "$branch")
  [ -n "$tip" ] || fail "$branch was not pushed"
  [ "$(remote_tree "$branch")" = "$(nth "$PUBLISHED_TREES" "$index")" ] || fail "$branch is not the tested tree"
  [ "$(git -C "$USER_REPO" rev-parse "refs/heads/$branch")" = "$tip" ] || fail "the local $branch is not what was pushed"
  [ "$(git --git-dir="$ORIGIN" log -1 --format='%an <%ae>|%s' "$tip")" = "Rehearsal User <user@example.com>|$(title_of "$index")" ] ||
    fail "the commit of $branch is not the user's or has another title"
  if [ "$index" -eq 1 ]; then parent=$BASE; else parent=$(remote_tip "$(branch_of $((index - 1)))"); fi
  [ "$(git --git-dir="$ORIGIN" rev-parse "$tip^")" = "$parent" ] || fail "$branch is not one commit on the branch before it"
  number=$(pr_of "$branch")
  [ "$(pr_get "$number" state)" = OPEN ] || fail "$branch has no open pull request"
  if [ "$index" -eq 1 ]; then base=main; else base=$(branch_of $((index - 1))); fi
  [ "$(pr_get "$number" base)" = "$base" ] || fail "#$number is based on $(pr_get "$number" base), not $base"
  [ "$(pr_get "$number" title)" = "$(title_of "$index")" ] || fail "#$number has another title"
  jq -j --argjson n "$number" '.prs[] | select(.number == $n) | .body' "$GH_STUB_DB" >"$SANDBOX/body.md"
  cmp -s "$SANDBOX/body.md" "$KIT/prs/$index.md" || fail "the description of #$number is not prs/$index.md"
  index=$((index + 1))
done
NUMBERS=""
index=1
while [ "$index" -le "$COUNT" ]; do
  NUMBERS="$NUMBERS $(pr_of "$(branch_of "$index")")"
  index=$((index + 1))
done
check_comments() { # each pull request of a stack has one comment listing the stack, in order
  local index=1 number comments line j other
  for number in $NUMBERS; do
    comments=$(jq -r --argjson n "$number" '.prs[] | select(.number == $n) | .comments | length' "$GH_STUB_DB")
    if [ "$COUNT" -eq 1 ]; then
      [ "$comments" = 0 ] || fail "a single pull request got a stack comment"
      continue
    fi
    [ "$comments" = 1 ] || fail "#$number has $comments comments, not one stack comment"
    jq -r --argjson n "$number" '.prs[] | select(.number == $n) | .comments[0]' "$GH_STUB_DB" >"$SANDBOX/comment.md"
    grep -qF "<!-- publish-kit:stack -->" "$SANDBOX/comment.md" || fail "the comment on #$number has no marker"
    j=1
    for other in $NUMBERS; do
      line=$(grep "^$j\. #$other " "$SANDBOX/comment.md") || fail "the comment on #$number does not list #$other as number $j"
      case "$line" in
        *"← this pull request") [ "$j" -eq "$index" ] || fail "the comment on #$number marks #$other as itself" ;;
        *) [ "$j" -ne "$index" ] || fail "the comment on #$number does not mark itself" ;;
      esac
      j=$((j + 1))
    done
    index=$((index + 1))
  done
}
check_comments
[ "$(user_status)" = "$STATUS_BEFORE" ] || fail "publishing changed the user's working tree"
[ -f "$USER_KIT/forks" ] || fail "publish did not record the fork points"
no_worktrees

step "publish once more: nothing changes"
state=$(refs_state)
publish_kit --yes || fail "publishing a third time failed"
expect_output "the branches exist already with this content; using them"
[ "$(refs_state)" = "$state" ] || fail "publishing again pushed"
check_comments
step "status"
publish_kit status || fail "status failed"
expect_output "#$(nth "$NUMBERS" 1) OPEN into main"

# ---- fixes --------------------------------------------------------------------------------------

if [ -n "$FIXES" ]; then
  # The agent updated the kit after it was published: the user's copy gets the new files.
  rm -rf "$USER_KIT/patches" "$USER_KIT/fixes"
  cp -R "$KIT/patches" "$KIT/fixes" "$USER_KIT/"
  cp "$KIT/stack.conf" "$KIT/README.md" "$USER_KIT/"
  for name in $FIXES; do
    trees_file=$KIT/fixes/$name.trees
    stage=$(sed -n '1s/ .*//p' "$trees_file")
    branch=$(branch_of "$stage")
    step "fix $name (on $branch), dry run first"
    state=$(refs_state)
    publish_kit fix "$name" --dry-run || fail "the dry run of fix $name failed"
    expect_output "verified: with the fix, every branch is exactly the tree that was tested"
    [ "$(refs_state)" = "$state" ] || fail "the dry run of fix $name pushed"
    old_tip=$(remote_tip "$branch")
    publish_kit fix "$name" --yes || fail "fix $name failed"
    expect_output "verified: with the fix, every branch is exactly the tree that was tested"
    [ "$(git --git-dir="$ORIGIN" rev-parse "$(remote_tip "$branch")^")" = "$old_tip" ] || fail "fix $name is not one new commit on $branch"
    [ "$(git --git-dir="$ORIGIN" log -1 --format=%s "$(remote_tip "$branch")")" = "$(head -n 1 "$KIT/fixes/$name.txt")" ] ||
      fail "the commit of fix $name has another title"
    while read -r index _ after; do
      [ "$(remote_tree "$(branch_of "$index")")" = "$after" ] || fail "after fix $name, $(branch_of "$index") is not the tested tree"
      if [ "$index" -gt "$stage" ]; then
        [ "$(git --git-dir="$ORIGIN" rev-parse "$(remote_tip "$(branch_of "$index")")^")" = "$(remote_tip "$(branch_of $((index - 1)))")" ] ||
          fail "after fix $name, $(branch_of "$index") is not on the branch before it"
      fi
    done <"$trees_file"
    state=$(refs_state)
    publish_kit fix "$name" --yes || fail "fix $name a second time failed"
    expect_output "nothing to push: every branch has the fix"
    [ "$(refs_state)" = "$state" ] || fail "fix $name a second time pushed"
    no_worktrees
  done
  index=1
  while [ "$index" -le "$COUNT" ]; do
    [ "$(remote_tree "$(branch_of "$index")")" = "$(nth "$TREES" "$index")" ] || fail "after the fixes, $(branch_of "$index") is not the tree in stack.conf"
    if [ "$index" -eq 1 ]; then base=main; else base=$(branch_of $((index - 1))); fi
    [ "$(pr_get "$(nth "$NUMBERS" "$index")" base)" = "$base" ] || fail "a fix changed the base of #$(nth "$NUMBERS" "$index")"
    index=$((index + 1))
  done
  step "publish after the fixes: the kit's patches match the fixed branches"
  state=$(refs_state)
  publish_kit --yes || fail "publish after the fixes failed"
  expect_output "the branches exist already with this content; using them"
  [ "$(refs_state)" = "$state" ] || fail "publish after the fixes pushed"
fi

# ---- review, merges and restacks ----------------------------------------------------------------

step "a reviewer pushes a commit to $(branch_of 1)"
git clone --quiet "$ORIGIN" "$SANDBOX/reviewer"
git -C "$SANDBOX/reviewer" switch --quiet "$(branch_of 1)"
printf 'A review comment, addressed.\n' >"$SANDBOX/reviewer/REHEARSAL-REVIEW.md"
git -C "$SANDBOX/reviewer" add REHEARSAL-REVIEW.md
git -C "$SANDBOX/reviewer" -c user.name=Reviewer -c user.email=reviewer@example.com commit --quiet -m "docs: address a review comment"
git -C "$SANDBOX/reviewer" push --quiet origin "$(branch_of 1)"

if [ "$COUNT" -gt 1 ]; then
  step "restack before any merge: the branches after $(branch_of 1) get the review commit"
else
  step "restack before any merge: nothing to do"
fi
tip=$(remote_tip "$(branch_of 1)")
publish_kit restack --yes || fail "restack before any merge failed"
[ "$(remote_tip "$(branch_of 1)")" = "$tip" ] || fail "restack changed $(branch_of 1), which is on main"
index=2
while [ "$index" -le "$COUNT" ]; do
  branch=$(branch_of "$index")
  [ "$(git --git-dir="$ORIGIN" diff --name-only "$(nth "$TREES" "$index")" "$(remote_tree "$branch")")" = REHEARSAL-REVIEW.md ] ||
    fail "after the restack, $branch is not its tested tree plus the review commit"
  index=$((index + 1))
done
state=$(refs_state)
publish_kit restack --yes || fail "restack a second time failed"
expect_output "the open branches are on origin/main already"
[ "$(refs_state)" = "$state" ] || fail "restack a second time pushed"

index=1
while [ "$index" -le "$COUNT" ]; do
  number=$(nth "$NUMBERS" "$index")
  [ "$(pr_get "$number" base)" = main ] || fail "#$number is not based on main when its turn comes"
  if [ "$index" -eq 2 ] && [ "$COUNT" -ge 3 ]; then
    step "merge #$number, keeping its branch (so GitHub does not retarget the next one)"
    gh pr merge "$number" --squash >"$OUT" 2>&1 || fail "merging #$number failed"
  else
    step "merge #$number and delete its branch"
    gh pr merge "$number" --squash --delete-branch >"$OUT" 2>&1 || fail "merging #$number failed"
  fi
  step "restack after #$number"
  publish_kit restack --yes || fail "restack after #$number failed"
  if [ "$index" -lt "$COUNT" ]; then
    main_tip=$(remote_tip main)
    next=$(branch_of $((index + 1)))
    [ "$(pr_get "$(nth "$NUMBERS" $((index + 1)))" base)" = main ] || fail "restack did not point #$(nth "$NUMBERS" $((index + 1))) at main"
    git --git-dir="$ORIGIN" merge-base --is-ancestor "$main_tip" "$(remote_tip "$next")" || fail "$next is not on main after the restack"
    later=$((index + 1))
    while [ "$later" -le "$COUNT" ]; do
      branch=$(branch_of "$later")
      [ "$(git --git-dir="$ORIGIN" diff --name-only "$(nth "$TREES" "$later")" "$(remote_tree "$branch")")" = REHEARSAL-REVIEW.md ] ||
        fail "after the restack, $branch is not its tested tree plus the review commit"
      if [ "$later" -gt $((index + 1)) ]; then
        [ "$(pr_get "$(nth "$NUMBERS" "$later")" base)" = "$(branch_of $((later - 1)))" ] || fail "restack changed the base of #$(nth "$NUMBERS" "$later")"
      fi
      later=$((later + 1))
    done
    state=$(refs_state)
    publish_kit restack --yes || fail "restack a second time failed"
    expect_output "the open branches are on origin/main already"
    [ "$(refs_state)" = "$state" ] || fail "restack a second time pushed"
  else
    expect_output "every pull request of this kit is merged"
  fi
  no_worktrees
  index=$((index + 1))
done

step "main is the stack: its tested trees plus the review commit, in $COUNT squash commit(s)"
[ "$(git --git-dir="$ORIGIN" diff --name-only "$(nth "$TREES" "$COUNT")" "$(remote_tree main)")" = REHEARSAL-REVIEW.md ] ||
  fail "main is not the final tree plus the review commit"
[ "$(git --git-dir="$ORIGIN" rev-list --count "$BASE..main")" = "$COUNT" ] || fail "main does not have one commit per pull request"
index=1
for commit in $(git --git-dir="$ORIGIN" rev-list --reverse "$BASE..main"); do
  [ "$(git --git-dir="$ORIGIN" log -1 --format=%s "$commit")" = "$(title_of "$index") (#$(nth "$NUMBERS" "$index"))" ] ||
    fail "squash commit $index has another title"
  index=$((index + 1))
done
[ "$(user_status)" = "$STATUS_BEFORE" ] || fail "the user's working tree changed"
OK=1
# shellcheck disable=SC2016 # expanded by the bash that ran publish.sh
echo "REHEARSAL OK: $COUNT pull request(s), fixes: ${FIXES:-none} (publish.sh ran with bash $("$RUN_BASH" -c 'echo "$BASH_VERSION"'))"

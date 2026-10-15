#!/usr/bin/env bash
# Publishes the pull requests of this publish kit (the files next to this script).
#
#   publish.sh --dry-run   build the commits in a temporary worktree and verify them; push nothing
#   publish.sh             create the branches, push them and open the pull requests
#   publish.sh restack     after a squash merge, a commit pushed to a branch, or a moved main:
#                          rebase each open branch onto the one before it (the first onto main)
#   publish.sh fix NAME    commit fixes/NAME.patch onto its branch, rebase the branches after it
#   publish.sh status      the branches and pull requests of this kit
#
# Options: --dry-run (with any command: show what would happen and change nothing), --yes (do not
# ask before pushing), --remote NAME (default: origin), --main NAME (default: main).
#
# Commits are made with your git identity in a temporary worktree: your checkout and working tree
# are never touched. Nothing is pushed unless every branch is exactly the tree that was tested
# (when main has moved since, the script says so and asks). README.md describes this kit.
#
# Needs git, and an authenticated GitHub CLI (`gh auth login`) for everything but `publish.sh
# --dry-run`. Runs with the bash 3.2 of macOS.
set -euo pipefail
set -f # word lists are split, never globbed

KIT_DIR=$(cd "$(dirname "$0")" && pwd -P)
REMOTE=origin
MAIN=main
MARKER="<!-- publish-kit:stack -->" # marks the comment that lists the stack

usage() { awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"; }
die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}
info() { printf '%s\n' "$*"; }

[ -f "$KIT_DIR/stack.conf" ] || die "$KIT_DIR/stack.conf is missing: this script belongs in a publish kit"
conf() { sed -n "s/^$1=//p" "$KIT_DIR/stack.conf" | tail -n 1; } # conf KEY: its value in stack.conf
BASE_COMMIT=$(conf BASE_COMMIT)
PREFIX=$(conf PREFIX)
NAMES=$(conf NAMES)
TREES=$(conf TREES)
FIXES=$(conf FIXES)
# shellcheck disable=SC2086 # word lists are split on purpose
words() { set -- $1 && printf '%s' "$#"; } # words "LIST": the number of words
# shellcheck disable=SC2086
nth() { printf '%s\n' $1 | sed -n "$2p"; } # nth "LIST" N: the Nth word
COUNT=$(words "$NAMES")
if [ "$COUNT" -lt 1 ] || [ "$(words "$TREES")" -ne "$COUNT" ] || [ ${#BASE_COMMIT} -ne 40 ]; then
  die "stack.conf needs BASE_COMMIT, NAMES and one tree per name in TREES"
fi

REPO_DIR=$(git -C "$KIT_DIR" rev-parse --show-toplevel 2>/dev/null) || die "$KIT_DIR is not inside a git repository"
case "$KIT_DIR" in
  "$REPO_DIR"/*) SELF=${KIT_DIR#"$REPO_DIR"/}/publish.sh ;; # how to run it from the repository's top
  *) SELF=$KIT_DIR/publish.sh ;;
esac
FORKS="$KIT_DIR/forks"

git_() { git -C "$REPO_DIR" "$@"; }
branch_of() { printf '%s%s' "$PREFIX" "$(nth "$NAMES" "$1")"; }
title_of() { head -n 1 "$KIT_DIR/messages/$1.txt"; }
all_branches() {
  local name list=""
  for name in $NAMES; do list="$list $PREFIX$name"; done
  printf '%s' "${list# }"
}

COMMAND=publish
DRY_RUN=0
YES=0
FIX_NAME=""
while [ $# -gt 0 ]; do
  case "$1" in
    publish | restack | status) COMMAND=$1 ;;
    fix)
      [ $# -ge 2 ] || die "usage: publish.sh fix NAME (the fixes of this kit: ${FIXES:-none})"
      COMMAND=fix
      FIX_NAME=$2
      shift
      ;;
    --dry-run) DRY_RUN=1 ;;
    --yes | -y) YES=1 ;;
    --remote)
      shift
      REMOTE=${1:?--remote needs a remote name}
      ;;
    --main)
      shift
      MAIN=${1:?--main needs a branch name}
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
  shift
done

TEMP_DIR="" # temporary files and the worktree, removed on exit
WORKTREE=""
cleanup() {
  if [ -n "$WORKTREE" ]; then
    git_ worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
  fi
  if [ -n "$TEMP_DIR" ]; then
    rm -rf "$TEMP_DIR"
    git_ worktree prune >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
temp_dir() { if [ -z "$TEMP_DIR" ]; then TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/publish-kit.XXXXXX"); fi; }

new_temp() { # new_temp VARIABLE: sets VARIABLE to a new temporary file
  local new_temp_path # (a name no caller uses: the caller's variable is set by name)
  temp_dir
  # shellcheck disable=SC2034 # used by the eval below
  new_temp_path=$(mktemp "$TEMP_DIR/file.XXXXXX")
  eval "$1=\$new_temp_path"
}

new_worktree() { # new_worktree COMMIT: a detached temporary worktree at COMMIT
  temp_dir
  WORKTREE=$TEMP_DIR/worktree
  git_ worktree add --quiet --detach "$WORKTREE" "$1"
}

confirm() {
  local answer
  if [ "$YES" -eq 1 ]; then return 0; fi
  printf '%s [y/N] ' "$1"
  read -r answer || answer=""
  case "$answer" in
    y | Y | yes | YES) return 0 ;;
    *) die "stopped; nothing more was changed" ;;
  esac
}

require_gh() {
  command -v gh >/dev/null 2>&1 || die "the GitHub CLI (gh) is required: https://cli.github.com"
  gh auth status >/dev/null 2>&1 || die "gh is not logged in: run gh auth login"
}

fetch() {
  info "fetching $REMOTE"
  git_ fetch --quiet --prune "$REMOTE" || die "git fetch $REMOTE failed"
}

remote_commit() { git_ rev-parse --verify --quiet "refs/remotes/$REMOTE/$1^{commit}" || true; }
local_commit() { git_ rev-parse --verify --quiet "refs/heads/$1^{commit}" || true; }
main_commit() {
  git_ rev-parse --verify --quiet "refs/remotes/$REMOTE/$MAIN^{commit}" ||
    die "there is no $REMOTE/$MAIN (see --remote and --main)"
}

# The open (or, with STATE, the merged or any) pull request of a branch: its number, or nothing.
pr_number() { gh pr list --head "$1" --state "${2:-open}" --json number --jq '.[0].number // empty'; }
pr_field() { gh pr view "$1" --json "$2" --jq ".$2"; }

apply_patch() { # apply_patch FILE: applies a patch to the worktree and its index (3-way if needed)
  git -C "$WORKTREE" apply --index --whitespace=nowarn "$1" 2>/dev/null ||
    git -C "$WORKTREE" apply --index --3way --whitespace=nowarn "$1"
}

# Commits the worktree's index with your identity. Commit hooks are skipped: instead, the trees are
# compared with the tested ones.
commit_with() { git -C "$WORKTREE" commit --quiet --no-verify --cleanup=whitespace -F "$1"; }

# ---- publish ----------------------------------------------------------------------------------

BASE=""
COMMITS=""
UNTESTED=0
EXISTING=0

build() { # builds the commits of the stack on REMOTE/MAIN in a temporary worktree (COMMITS)
  local index patch
  BASE=$(main_commit)
  if ! git_ cat-file -e "$BASE_COMMIT^{commit}" 2>/dev/null || ! git_ merge-base --is-ancestor "$BASE_COMMIT" "$BASE"; then
    die "$REMOTE/$MAIN does not contain $BASE_COMMIT, the commit this kit was built on"
  fi
  new_worktree "$BASE"
  index=1
  while [ "$index" -le "$COUNT" ]; do
    patch="$KIT_DIR/patches/$index.patch"
    [ -f "$patch" ] || die "missing $patch"
    apply_patch "$patch" ||
      die "patch $index ($(title_of "$index")) does not apply to $REMOTE/$MAIN; nothing was pushed"
    commit_with "$KIT_DIR/messages/$index.txt"
    COMMITS="$COMMITS $(git -C "$WORKTREE" rev-parse HEAD)"
    index=$((index + 1))
  done
  verify
}

verify() { # compares the built commits with the tested trees
  local index=1 commit tree expected
  if [ "$BASE" = "$BASE_COMMIT" ]; then
    for commit in $COMMITS; do
      tree=$(git_ rev-parse "$commit^{tree}")
      expected=$(nth "$TREES" "$index")
      [ "$tree" = "$expected" ] ||
        die "$(branch_of "$index") would be tree $tree, but the tested tree is $expected; nothing was pushed"
      index=$((index + 1))
    done
    info "verified: every branch is exactly the tree that was tested"
  else
    UNTESTED=1
    info "warning: $REMOTE/$MAIN has moved past $(git_ rev-parse --short "$BASE_COMMIT"), the commit this kit was tested on:"
    info "         the patches apply, but this combination is untested (CI tests it)"
  fi
  note_working_tree
}

note_working_tree() { # says whether the working tree has the stack's version of the files it changes
  local names file path ours differing=""
  new_temp names
  git -C "$WORKTREE" diff --name-only -z "$BASE" HEAD >"$names"
  while IFS= read -r -d '' file; do
    path="$REPO_DIR/$file"
    if git -C "$WORKTREE" cat-file -e "HEAD:$file" 2>/dev/null; then
      ours=$(git -C "$WORKTREE" rev-parse "HEAD:$file")
      if [ -L "$path" ]; then
        [ "$(printf '%s' "$(readlink "$path")" | git_ hash-object --stdin)" = "$ours" ] || differing="$differing $file"
      elif [ -f "$path" ]; then
        [ "$(git_ hash-object -- "$path")" = "$ours" ] || differing="$differing $file"
      else
        differing="$differing $file"
      fi
    elif [ -e "$path" ] || [ -L "$path" ]; then
      differing="$differing $file" # the stack deletes it
    fi
  done <"$names"
  if [ -n "$differing" ]; then
    info "note: your working tree differs from the stack in:$differing"
  fi
}

show_commits() {
  local index=1 commit
  info ""
  info "the stack, on $REMOTE/$MAIN ($(git_ rev-parse --short "$BASE")):"
  for commit in $COMMITS; do
    info "  $(branch_of "$index")  $(git_ rev-parse --short "$commit")  $(title_of "$index")"
    index=$((index + 1))
  done
  info ""
}

# Branches left by an earlier run (local, or else on the remote) are reused when they have the same
# trees, so that a second run pushes nothing again.
check_branches() {
  local index=1 commit branch tip existing=0 matching=0 kept=""
  for commit in $COMMITS; do
    branch=$(branch_of "$index")
    tip=$(local_commit "$branch")
    if [ -z "$tip" ]; then tip=$(remote_commit "$branch"); fi
    if [ -n "$tip" ]; then
      existing=$((existing + 1))
      if [ "$(git_ rev-parse "$tip^{tree}")" = "$(git_ rev-parse "$commit^{tree}")" ]; then
        matching=$((matching + 1))
      fi
    fi
    kept="$kept $tip"
    index=$((index + 1))
  done
  if [ "$existing" -eq 0 ]; then
    return
  fi
  [ "$matching" -eq "$COUNT" ] ||
    die "branches of this kit exist with other content. Once published, change them with $SELF fix or restack (see $SELF status); to start again, delete them: git branch -D $(all_branches)"
  COMMITS=$kept
  EXISTING=1
  info "the branches exist already with this content; using them"
}

create_branches() {
  local index=1 commit branch
  for commit in $COMMITS; do
    branch=$(branch_of "$index")
    if [ -z "$(local_commit "$branch")" ]; then git_ branch "$branch" "$commit"; fi
    index=$((index + 1))
  done
  if [ "$EXISTING" -eq 0 ]; then info "created the branches $(all_branches)"; fi
}

push_branches() {
  local index=1 commit branch remote
  for commit in $COMMITS; do
    branch=$(branch_of "$index")
    remote=$(remote_commit "$branch")
    if [ -z "$remote" ]; then
      git_ push --quiet --set-upstream "$REMOTE" "$branch" || die "pushing $branch failed"
      info "pushed $branch"
    elif [ "$remote" = "$commit" ]; then
      info "already pushed: $branch"
    else
      die "$REMOTE/$branch exists with other commits; after merges use $SELF restack, or delete that branch on $REMOTE"
    fi
    index=$((index + 1))
  done
}

record_publish_forks() {
  local index=2 forks=""
  while [ "$index" -le "$COUNT" ]; do
    forks="$forks $index $(nth "$COMMITS" $((index - 1)))"
    index=$((index + 1))
  done
  rm -f "$FORKS"
  record_forks "$forks"
}

open_prs() {
  local index=1 branch base number numbers=""
  while [ "$index" -le "$COUNT" ]; do
    branch=$(branch_of "$index")
    if [ "$index" -eq 1 ]; then base=$MAIN; else base=$(branch_of $((index - 1))); fi
    number=$(pr_number "$branch")
    if [ -z "$number" ]; then
      gh pr create --base "$base" --head "$branch" --title "$(title_of "$index")" \
        --body-file "$KIT_DIR/prs/$index.md" >/dev/null ||
        die "opening the pull request for $branch failed; run the script again to continue"
      number=$(pr_number "$branch")
      [ -n "$number" ] || die "cannot find the pull request just opened for $branch"
      info "opened #$number  $(title_of "$index")"
    else
      info "exists: #$number  $(title_of "$index")"
    fi
    numbers="$numbers $number"
    index=$((index + 1))
  done
  if [ "$COUNT" -gt 1 ]; then comment_stack "$numbers"; fi
  info ""
  for number in $numbers; do info "  $(pr_field "$number" url)"; done
}

# The order of the stack goes into a comment on each pull request, not into its description: the
# description becomes the body of the squash commit on main.
comment_stack() { # comment_stack "NUMBERS"
  local numbers=$1 index=0 number other j comment found
  new_temp comment
  for number in $numbers; do
    index=$((index + 1))
    found=$(gh pr view "$number" --json comments --jq "[.comments[] | select(.body | contains(\"$MARKER\"))] | length")
    if [ "$found" != 0 ]; then continue; fi
    {
      printf '%s\n' "$MARKER"
      # shellcheck disable=SC2016 # Markdown backticks
      printf '**Stack** of %s pull requests: merge them in this order with "Squash and merge"; after each merge, the branches after it are rebased onto `%s`.\n\n' "$COUNT" "$MAIN"
      j=1
      for other in $numbers; do
        if [ "$j" -eq "$index" ]; then
          printf '%s. #%s %s ← this pull request\n' "$j" "$other" "$(title_of "$j")"
        else
          printf '%s. #%s %s\n' "$j" "$other" "$(title_of "$j")"
        fi
        j=$((j + 1))
      done
    } >"$comment"
    gh pr comment "$number" --body-file "$comment" >/dev/null ||
      die "commenting the stack on #$number failed; run the script again to continue"
  done
}

after_merge_hint() {
  info "If your working tree has these changes uncommitted (where the agent left them): once everything is"
  info "merged, and if your $MAIN has no commits of its own, \`git switch $MAIN && git fetch && git reset $REMOTE/$MAIN\`"
  info "makes your checkout match $REMOTE/$MAIN without touching any file; what still differs is a local change."
}

publish() {
  local merged
  command -v git >/dev/null 2>&1 || die "git is required"
  if [ "$DRY_RUN" -eq 0 ]; then
    require_gh
    merged=$(pr_number "$(branch_of 1)" merged)
    [ -z "$merged" ] || die "#$merged of this kit is merged already: run $SELF restack (or $SELF status)"
  fi
  fetch
  build
  check_branches
  show_commits
  if [ "$DRY_RUN" -eq 1 ]; then
    info "dry run: no branch was created, nothing was pushed and no pull request was opened."
    info "Without --dry-run, the script creates these branches, pushes them to $REMOTE and opens the pull requests."
    return
  fi
  if [ "$UNTESTED" -eq 1 ]; then info "(untested on the current $REMOTE/$MAIN, see the warning above)"; fi
  confirm "Create these branches, push them to $REMOTE ($(git_ remote get-url "$REMOTE")) and open the pull requests?"
  create_branches
  push_branches
  if [ "$EXISTING" -eq 0 ] || [ ! -f "$FORKS" ]; then record_publish_forks; fi
  open_prs
  info ""
  if [ "$COUNT" -gt 1 ]; then
    info "Next: merge #$(pr_number "$(branch_of 1)") with \"Squash and merge\", then run $SELF restack,"
    info "and so on for each pull request, in order."
  else
    info "Next: review and merge #$(pr_number "$(branch_of 1)") with \"Squash and merge\"."
  fi
  after_merge_hint
}

# ---- restack ----------------------------------------------------------------------------------

# The commit each branch was built on (the tip of the branch before it), recorded by publish,
# restack and fix, so that a branch's own commits are known after the branches before it changed.
recorded_fork() { # recorded_fork N: the recorded fork point of branch N, or nothing
  if [ -f "$FORKS" ]; then sed -n "s/^$1 //p" "$FORKS"; fi
}

record_forks() { # record_forks "N COMMIT N COMMIT ...": remembers fork points
  local file
  new_temp file
  if [ -f "$FORKS" ]; then cp "$FORKS" "$file"; fi
  # shellcheck disable=SC2086 # pairs of words
  set -- $1
  while [ $# -ge 2 ]; do
    grep -v "^$1 " "$file" >"$file.new" || true
    printf '%s %s\n' "$1" "$2" >>"$file.new"
    sort "$file.new" >"$file"
    rm -f "$file.new"
    shift 2
  done
  cp "$file" "$FORKS"
}

# Rebases the branches FROM..COUNT, each onto the new tip of the branch before it (the first onto
# PREVIOUS_NEW, after being built on PREVIOUS_OLD). Sets OLD_TIP_<n> and NEW_TIP_<n>, adds the
# branches whose tip changes to CHANGED, and their fork points to NEW_FORKS (also the first one's
# with RECORD_FIRST=1). AGAIN names the command to run again after resolving a conflict by hand.
CHANGED=""
NEW_FORKS=""
rebase_from() { # rebase_from FROM PREVIOUS_OLD PREVIOUS_NEW RECORD_FIRST AGAIN
  local index=$1 previous_old=$2 previous_new=$3 branch old_tip fork new_tip
  while [ "$index" -le "$COUNT" ]; do
    branch=$(branch_of "$index")
    old_tip=$(remote_commit "$branch")
    [ -n "$old_tip" ] || die "$REMOTE/$branch does not exist"
    if git_ merge-base --is-ancestor "$previous_new" "$old_tip"; then
      new_tip=$old_tip # on the branch before it (or main) already
      info "  $branch: on $(git_ rev-parse --short "$previous_new") already"
    else
      fork=$(recorded_fork "$index")
      if [ -z "$fork" ] || ! git_ merge-base --is-ancestor "$fork" "$old_tip" 2>/dev/null; then
        fork=$(git_ merge-base "$old_tip" "$previous_old")
      fi
      git -C "$WORKTREE" checkout --quiet --detach "$old_tip"
      if ! git -C "$WORKTREE" rebase --quiet --no-verify --onto "$previous_new" "$fork" >/dev/null 2>&1; then
        git -C "$WORKTREE" rebase --abort >/dev/null 2>&1 || true
        info "error: rebasing $branch onto $(git_ rev-parse --short "$previous_new") conflicts; nothing was pushed." >&2
        info "Resolve it in a separate worktree, then run $5 again for the branches after it:" >&2
        info "  git worktree add -B $branch ../restack $REMOTE/$branch && cd ../restack" >&2
        info "  git rebase --onto $(git_ rev-parse --short "$previous_new") $(git_ rev-parse --short "$fork")   # resolve, then: git rebase --continue" >&2
        info "  git push --force-with-lease $REMOTE $branch && cd - && git worktree remove ../restack" >&2
        exit 1
      fi
      new_tip=$(git -C "$WORKTREE" rev-parse HEAD)
      CHANGED="$CHANGED $index"
      info "  $branch: $(git_ rev-parse --short "$old_tip") -> $(git_ rev-parse --short "$new_tip") on $(git_ rev-parse --short "$previous_new")"
    fi
    if [ "$index" -gt "$1" ] || [ "$4" -eq 1 ]; then NEW_FORKS="$NEW_FORKS $index $previous_new"; fi
    eval "OLD_TIP_$index=\$old_tip NEW_TIP_$index=\$new_tip"
    previous_old=$old_tip
    previous_new=$new_tip
    index=$((index + 1))
  done
}

push_changed() { # force-pushes the branches in CHANGED (with lease) and moves the local branches
  local position branch old_tip new_tip
  for position in $CHANGED; do
    branch=$(branch_of "$position")
    eval "old_tip=\$OLD_TIP_$position new_tip=\$NEW_TIP_$position"
    git_ push --quiet --force-with-lease="refs/heads/$branch:$old_tip" "$REMOTE" "$new_tip:refs/heads/$branch" ||
      die "pushing $branch failed (did someone push to it meanwhile?)"
    if [ -n "$(local_commit "$branch")" ]; then
      git_ branch --force "$branch" "$new_tip" >/dev/null 2>&1 ||
        info "note: $branch is checked out; update it with: git reset --hard $REMOTE/$branch"
    fi
    info "pushed $branch"
  done
}

changed_branches() {
  local position list=""
  for position in $CHANGED; do list="$list $(branch_of "$position")"; done
  printf '%s' "${list# }"
}

restack() {
  local index state first_open=0 last_merged=0 branch number merged_head main_tip previous_old
  require_gh
  fetch
  index=1
  while [ "$index" -le "$COUNT" ]; do
    branch=$(branch_of "$index")
    if [ -n "$(pr_number "$branch" merged)" ]; then
      state=MERGED
    elif [ -n "$(pr_number "$branch")" ]; then
      state=OPEN
    else
      state=NONE
    fi
    case "$state" in
      MERGED)
        [ "$first_open" -eq 0 ] ||
          die "#$(pr_number "$branch" merged) ($branch) was merged before an earlier pull request of the stack; sort it out by hand"
        last_merged=$index
        ;;
      OPEN) if [ "$first_open" -eq 0 ]; then first_open=$index; fi ;;
      NONE) die "$branch has no open or merged pull request (closed?); publish first, or fix it by hand" ;;
    esac
    index=$((index + 1))
  done
  if [ "$first_open" -eq 0 ]; then
    info "every pull request of this kit is merged. Delete the local branches with"
    info "  git branch -D $(all_branches)"
    after_merge_hint
    return
  fi
  main_tip=$(main_commit)
  if [ "$last_merged" -gt 0 ]; then
    # The first open branch was built on the merged branch's last head (or on an older tip of it).
    number=$(pr_number "$(branch_of "$last_merged")" merged)
    merged_head=$(pr_field "$number" headRefOid)
    if ! git_ cat-file -e "$merged_head^{commit}" 2>/dev/null; then
      git_ fetch --quiet "$REMOTE" "refs/pull/$number/head" || true
      git_ cat-file -e "$merged_head^{commit}" 2>/dev/null ||
        die "commit $merged_head (the head of #$number) is not available; fetch it and try again"
    fi
    previous_old=$merged_head
  else
    branch=$(branch_of 1)
    [ -n "$(remote_commit "$branch")" ] || die "$REMOTE/$branch does not exist"
    previous_old=$(git_ merge-base "$(remote_commit "$branch")" "$main_tip")
  fi
  new_worktree "$main_tip"
  rebase_from "$first_open" "$previous_old" "$main_tip" 0 "$SELF restack"
  number=$(pr_number "$(branch_of "$first_open")")
  if [ "$DRY_RUN" -eq 1 ]; then
    if [ -n "$CHANGED" ]; then info "dry run: would force-push (with lease): $(changed_branches)"; fi
    if [ "$(pr_field "$number" baseRefName)" != "$MAIN" ]; then info "dry run: would point #$number at $MAIN"; fi
    info "dry run: nothing was pushed"
    return
  fi
  if [ -n "$CHANGED" ]; then
    confirm "Force-push $(changed_branches) (with lease) and point #$number at $MAIN?"
    push_changed
  else
    info "the open branches are on $REMOTE/$MAIN already"
  fi
  if [ -n "$NEW_FORKS" ]; then record_forks "$NEW_FORKS"; fi
  if [ "$(pr_field "$number" baseRefName)" != "$MAIN" ]; then
    gh pr edit "$number" --base "$MAIN" >/dev/null || die "changing the base of #$number to $MAIN failed"
  fi
  info "#$number targets $MAIN: merge it next (\"Squash and merge\"), then run $SELF restack again"
}

# ---- fix --------------------------------------------------------------------------------------

# fixes/NAME.patch and fixes/NAME.txt are a change for one branch and its commit message.
# fixes/NAME.trees has a line "N BEFORE AFTER" for that branch and each one after it: its tree as
# published (or after the fixes before this one) and as tested with this fix.
fix() {
  local name=$FIX_NAME patch message trees start branch old_start new_start index strict=1 expected actual
  case "$name" in
    "" | */* | .*) die "fix NAME: the name of a fix of this kit (${FIXES:-none})" ;;
  esac
  patch="$KIT_DIR/fixes/$name.patch"
  message="$KIT_DIR/fixes/$name.txt"
  trees="$KIT_DIR/fixes/$name.trees"
  if [ ! -f "$patch" ] || [ ! -f "$message" ] || [ ! -f "$trees" ]; then
    die "there is no fix $name (fixes/$name.patch, .txt and .trees); the fixes of this kit: ${FIXES:-none}"
  fi
  start=$(sed -n '1s/^\([0-9][0-9]*\) .*$/\1/p' "$trees")
  if [ -z "$start" ] || [ "$start" -lt 1 ] || [ "$start" -gt "$COUNT" ]; then
    die "fixes/$name.trees does not name a branch of this kit"
  fi
  require_gh
  fetch
  branch=$(branch_of "$start")
  [ -z "$(pr_number "$branch" merged)" ] || die "$branch is merged already; make the fix a pull request of its own"
  [ -n "$(pr_number "$branch")" ] || die "$branch has no open pull request"
  old_start=$(remote_commit "$branch")
  [ -n "$old_start" ] || die "$REMOTE/$branch does not exist"
  # Only from known states (as published, or with this fix already) is the result checked exactly.
  index=$start
  while [ "$index" -le "$COUNT" ]; do
    actual=$(remote_commit "$(branch_of "$index")")
    [ -n "$actual" ] || die "$REMOTE/$(branch_of "$index") does not exist"
    actual=$(git_ rev-parse "$actual^{tree}")
    if ! awk -v n="$index" -v tree="$actual" '$1 == n && ($2 == tree || $3 == tree) { found = 1 } END { exit !found }' "$trees"; then
      strict=0
    fi
    index=$((index + 1))
  done
  new_worktree "$old_start"
  if git -C "$WORKTREE" apply --check --reverse "$patch" >/dev/null 2>&1; then
    new_start=$old_start
    info "  $branch: has \"$(head -n 1 "$message")\" already"
  else
    apply_patch "$patch" || die "fixes/$name.patch does not apply to $branch; nothing was pushed"
    commit_with "$message"
    new_start=$(git -C "$WORKTREE" rev-parse HEAD)
    CHANGED=$start
    eval "OLD_TIP_$start=\$old_start NEW_TIP_$start=\$new_start"
    info "  $branch: $(git_ rev-parse --short "$old_start") -> $(git_ rev-parse --short "$new_start") (+ $(head -n 1 "$message"))"
  fi
  rebase_from $((start + 1)) "$old_start" "$new_start" 1 "$SELF fix $name"
  if [ "$strict" -eq 1 ]; then
    index=$start
    while [ "$index" -le "$COUNT" ]; do
      expected=$(awk -v n="$index" '$1 == n { print $3 }' "$trees")
      eval "actual=\${NEW_TIP_$index:-\$new_start}"
      actual=$(git_ rev-parse "$actual^{tree}")
      [ "$expected" = "$actual" ] ||
        die "$(branch_of "$index") would be tree $actual, but the tested tree is $expected; nothing was pushed"
      index=$((index + 1))
    done
    info "verified: with the fix, every branch is exactly the tree that was tested"
  else
    info "warning: the branches have commits this kit did not make (review fixes?); the result was not compared"
    info "         with the tested trees (CI tests it)"
  fi
  if [ -z "$CHANGED" ]; then
    info "nothing to push: every branch has the fix"
    return
  fi
  if [ "$DRY_RUN" -eq 1 ]; then
    info "dry run: would push $(changed_branches) (with a lease); nothing was pushed"
    return
  fi
  confirm "Push the fix to $branch and force-push the branches after it (with lease)?"
  push_changed
  record_forks "$NEW_FORKS"
  info "done: the pull requests show the fix as a new commit; their bases are unchanged"
}

# ---- status -----------------------------------------------------------------------------------

status() {
  local index=1 branch number state
  require_gh
  fetch
  info "$REMOTE/$MAIN is $(git_ rev-parse --short "$(main_commit)"); this kit was built on $(git_ rev-parse --short "$BASE_COMMIT" 2>/dev/null || printf '%s' "$BASE_COMMIT")"
  while [ "$index" -le "$COUNT" ]; do
    branch=$(branch_of "$index")
    number=$(pr_number "$branch" all)
    if [ -n "$number" ]; then
      state="#$number $(pr_field "$number" state) into $(pr_field "$number" baseRefName)"
    else
      state="no pull request"
    fi
    info "  $branch  local $(git_ rev-parse --short --verify --quiet "refs/heads/$branch" || echo -)  $REMOTE $(git_ rev-parse --short --verify --quiet "refs/remotes/$REMOTE/$branch" || echo -)  $state"
    index=$((index + 1))
  done
  if [ -n "$FIXES" ]; then info "fixes (run in this order): $FIXES"; fi
}

cd "$REPO_DIR"
case "$COMMAND" in
  publish) publish ;;
  restack) restack ;;
  fix) fix ;;
  status) status ;;
esac

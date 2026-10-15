#!/usr/bin/env bash
# Runs fanferro's CI checks on each stage of a stack, because every pull request must pass on its
# own. The stages are checked out one after another in one scratch clone (never the workspace), so
# cargo's and uv's caches stay warm between them.
#
#   verify_stages.sh [--dir DIR] [--python "3.12 3.15"] [--quick] REPO REF...
#
# REPO is the scratch repository (only read); REF... are the stage commits in order, for example
# refs/kits/<topic>/stage-1 refs/kits/<topic>/stage-2. For each stage: ruff format and lint,
# pyrefly, the consistency checks and actionlint, then pytest on each Python of --python (default:
# 3.12 and 3.15, the oldest and newest in CI; including the e2e tests on MiniStack unless --quick),
# and cargo fmt, clippy and test if the stage changes Rust files compared with the parent of the
# first REF (with the Rust end-to-end tests on a MiniStack of its own unless --quick). A stage
# fails, too, if the checks leave files changed. DIR (default: ~/.cache/fanferro-stages) holds the clone, the virtual environments, the logs and, unless
# CARGO_TARGET_DIR is set, cargo's target directory.
set -euo pipefail

DIR=${XDG_CACHE_HOME:-$HOME/.cache}/fanferro-stages
PYTHONS="3.12 3.15"
QUICK=0
REPO=""
REFS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)
      shift
      DIR=${1:?--dir needs a directory}
      ;;
    --python)
      shift
      PYTHONS=${1:?--python needs versions}
      ;;
    --quick) QUICK=1 ;;
    -h | --help)
      awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 2
      ;;
    *) if [ -z "$REPO" ]; then REPO=$1; else REFS="$REFS $1"; fi ;;
  esac
  shift
done
if [ -z "$REPO" ] || [ -z "$REFS" ]; then
  echo "usage: verify_stages.sh [--dir DIR] [--python \"3.12 3.15\"] [--quick] REPO REF..." >&2
  exit 2
fi
REPO=$(cd "$REPO" && pwd -P)
mkdir -p "$DIR/logs"
DIR=$(cd "$DIR" && pwd -P)
SRC=$DIR/src
export UV_LOCKED=1 # as in CI: the lock file must be up to date
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$DIR/target}

if [ ! -d "$SRC/.git" ]; then git clone --quiet --no-checkout "$REPO" "$SRC"; fi
git -C "$SRC" fetch --quiet --force "$REPO" "+refs/*:refs/source/*"
COMMITS=""
for ref in $REFS; do
  COMMITS="$COMMITS $(git -C "$REPO" rev-parse --verify "$ref^{commit}")"
done
# shellcheck disable=SC2086 # a list of commits
set -- $COMMITS
BASE=$(git -C "$REPO" rev-parse "$1^")
# shellcheck disable=SC2086 # a list of versions
LAST_PYTHON=$(printf '%s\n' $PYTHONS | tail -n 1)

check() { # check STAGE NAME COMMAND...: runs one check in the clone, logged; stops at a failure
  local stage=$1 name=$2 log
  shift 2
  log="$DIR/logs/stage-$stage-$(printf '%s' "$name" | tr -c 'a-zA-Z0-9.' '-').log"
  if (cd "$SRC" && "$@") >"$log" 2>&1; then
    printf '  ✓ %s\n' "$name"
  else
    printf '  ✗ %s (log: %s)\n' "$name" "$log"
    tail -n 40 "$log" | sed 's/^/    | /'
    exit 1
  fi
}
in_venv() { # in_venv VERSION COMMAND...: uv run in the clone's environment for a Python version
  local version=$1
  shift
  UV_PROJECT_ENVIRONMENT="$DIR/venv-$version" uv run --python "$version" "$@"
}
rust_e2e() { # rust_e2e: the Rust end-to-end tests against a MiniStack of their own, as in CI
  local port url pid status=0 tries=0
  port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  url="http://127.0.0.1:$port"
  GATEWAY_PORT=$port MINISTACK_REGION=us-east-1 "$DIR/venv-$LAST_PYTHON/bin/ministack" >"$DIR/logs/ministack.log" 2>&1 &
  pid=$!
  until curl -sf "$url/_ministack/health" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -gt 120 ] || ! kill -0 "$pid" 2>/dev/null; then
      echo "MiniStack did not become healthy (log: $DIR/logs/ministack.log)"
      kill "$pid" 2>/dev/null || true
      return 1
    fi
    sleep 0.5
  done
  FANFERRO_TEST_ENDPOINT_URL=$url cargo test -p fanferro-core --test aws_emulator || status=$?
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return "$status"
}
pytest_summary() { tail -n 1 "$DIR/logs/stage-$1-pytest-$2.log" | sed 's/=//g; s/^ *//; s/ *$//'; }

stage=0
SUMMARY=""
for commit in $COMMITS; do
  stage=$((stage + 1))
  git -C "$SRC" checkout --quiet --force --detach "$commit"
  # The editable install keeps the native extension in the tree; uv rebuilds it when Rust changes.
  git -C "$SRC" clean -fdxq -e 'python/fanferro/_core*'
  printf '== stage %s: %s %s\n' "$stage" "$(git -C "$SRC" rev-parse --short HEAD)" "$(git -C "$SRC" log -1 --format=%s)"
  check "$stage" "ruff format" in_venv "$LAST_PYTHON" ruff format --check python tests examples scripts
  check "$stage" "ruff check" in_venv "$LAST_PYTHON" ruff check python tests examples scripts
  check "$stage" "pyrefly" in_venv "$LAST_PYTHON" pyrefly check
  check "$stage" "consistency" in_venv "$LAST_PYTHON" python scripts/check_consistency.py
  check "$stage" "actionlint" uvx --from actionlint-py actionlint
  results=""
  for version in $PYTHONS; do
    if [ "$QUICK" -eq 1 ]; then
      check "$stage" "pytest-$version" in_venv "$version" pytest -q -p no:cacheprovider -m "not e2e"
    else
      check "$stage" "pytest-$version" in_venv "$version" pytest -q -p no:cacheprovider
    fi
    results="$results; $version: $(pytest_summary "$stage" "$version")"
  done
  if ! git -C "$SRC" diff --quiet "$BASE" "$commit" -- crates Cargo.toml Cargo.lock rust-toolchain.toml; then
    check "$stage" "cargo fmt" cargo fmt --all -- --check
    check "$stage" "cargo clippy" cargo clippy --workspace --all-targets -- -D warnings
    check "$stage" "cargo test" cargo test --workspace
    if [ "$QUICK" -eq 0 ]; then check "$stage" "cargo test (MiniStack)" rust_e2e; fi
    results="$results; Rust checked"
  fi
  if [ -n "$(git -C "$SRC" status --porcelain)" ]; then
    printf '  ✗ the checks left changes:\n'
    git -C "$SRC" status --porcelain | sed 's/^/    | /'
    exit 1
  fi
  SUMMARY="$SUMMARY
  stage $stage ($(git -C "$SRC" rev-parse --short HEAD))${results}"
done
printf '\nall %s stage(s) pass:%s\n' "$stage" "$SUMMARY"

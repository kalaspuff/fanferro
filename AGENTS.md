# AGENTS.md - working on fanferro

This file is for AI agents (and humans) changing fanferro itself. If you are *using* fanferro in an
application, read `python/fanferro/AGENT_GUIDE.md` (`fanferro guide`) instead.

fanferro is an SNS + SQS messaging library: a Rust core (`crates/fanferro-core`), a thin PyO3
binding (`crates/fanferro-py` → `fanferro._core`) and the Python API (`python/fanferro`). Read
[docs/architecture.md](docs/architecture.md) before any non-trivial change - it defines the layers,
the message pipeline and the invariants you must preserve.

## Repository map

```text
crates/fanferro-core/src/      Rust core (no Python): envelope/, transport/, publish.rs, consumer/,
                               provision/, backend/{aws,memory}/, events.rs, metrics.rs, retry.rs
crates/fanferro-core/tests/    Rust end-to-end tests against an emulator (aws_emulator.rs)
crates/fanferro-py/src/        PyO3 bindings: runtime.rs (Tokio + asyncio bridge), errors.rs, convert.rs,
                               types.rs, envelopes.rs, client.rs, consumer.rs
python/fanferro/               Python API (client, resources, consumer, routing, codecs, events, ...)
python/fanferro/_core.pyi      Type stubs of the native module - keep in sync with crates/fanferro-py
python/fanferro/AGENT_GUIDE.md Usage guide shipped in the wheel (`fanferro guide`)
tests/unit|integration|e2e     pytest suites (integration runs on the memory broker AND MiniStack)
examples/                      Runnable examples (executed by tests/unit/test_examples.py)
docs/                          Guides, reference (events/attributes generated), ADRs
skills/fanferro/SKILL.md       Skill for agents of applications using fanferro (copy into a project)
.claude/skills/                Skills for agents developing fanferro (this repository)
scripts/                       check_consistency.py, generate_reference.py, release.py, release_notes.py,
                               smoke_test.py (run against every built wheel), benchmark.py
.github/workflows/             ci.yml, prepare-release.yml, tag-release.yml, release.yml, pr-title.yml
```

## Setup and commands

Requirements: Rust (stable, see `rust-toolchain.toml`), [uv](https://docs.astral.sh/uv/). No Docker.

| Task | Command |
| --- | --- |
| Install/refresh everything (builds the extension) | `uv sync` |
| All checks (what CI runs) | `make check` |
| Python tests (unit + integration on memory and MiniStack + e2e) | `uv run pytest` |
| Only fast tests (no emulator) | `uv run pytest -m "not e2e"` |
| Rust tests | `cargo test --workspace` |
| Rust e2e against an emulator | `FANFERRO_TEST_ENDPOINT_URL=http://localhost:4566 cargo test -p fanferro-core --test aws_emulator` |
| Lint/format Python | `uv run ruff check --fix python tests examples scripts && uv run ruff format python tests examples scripts` |
| Type check Python | `uv run pyrefly check` |
| Lint/format Rust | `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings` |
| Docs drift checks | `uv run python scripts/check_consistency.py` |
| Regenerate event/attribute reference | `uv run python scripts/generate_reference.py` |
| Release wheels for all Linux targets | `make wheels` (maturin + zig, `--profile dist`, like release.yml; `make wheels-fast` uses `build-wheels`) |
| Smoke-test an installed wheel | `python -I scripts/smoke_test.py` |
| Throughput per message (wall and CPU time) | `uv run python scripts/benchmark.py` (`--endpoint` for MiniStack) |
| Lint GitHub workflows | `uvx --from actionlint-py actionlint` (part of `make lint`) |
| Run MiniStack manually | `uv run ministack` (port 4566) |

`uv run ...` rebuilds the native extension automatically when Rust sources change (see
`[tool.uv] cache-keys`); a fresh build takes about a minute. The pytest fixture starts MiniStack
on a free port by itself unless `FANFERRO_TEST_ENDPOINT_URL` is set.

## Golden rules

1. **Respect the layer split.** Protocol, timing and I/O → Rust core. Application objects (codecs,
   handlers, hooks, OpenTelemetry) → Python. The binding crate contains conversions only.
2. **Never break the invariants** in docs/architecture.md (at-least-once settlement, bounded
   in-flight, leases protected until settled, stable message ids, transport details invisible,
   fail fast before the network, no I/O on import, graceful shutdown, cheap events, fork safety,
   clean interpreter exit).
3. **Every behaviour change ships with tests, docs and a changelog-worthy commit message.**
   CI enforces formatting, lint, types (pyrefly), clippy `-D warnings`, tests on Python 3.12-3.15,
   and `scripts/check_consistency.py`.
4. **Both backends.** A new SQS/SNS/S3 operation must be implemented in `backend/aws` *and*
   `backend/memory` and tested against both (integration tests are parametrized).
5. **Errors are actionable.** Validation errors say what to change; AWS errors keep code, request id
   and a retry classification.
6. **Public API is typed and documented.** Google-style docstrings, PEP 695 generics, `from
   __future__ import annotations`, no `Any` where a real type exists. Rust public items need docs
   (`missing_docs` is on); `unsafe` is forbidden.
7. **Do not add required runtime dependencies** to the Python package. Optional integrations are
   imported lazily (see codecs and telemetry).
8. **Wire format changes are breaking changes** (envelopes, reserved attributes, pointer formats):
   keep reading the old format, write an ADR, mention it in the changelog.

## Change checklists

Use the matching checklist; the skills in `.claude/skills/` walk through them in detail.

**New or changed option** (client, resource, consumer, policy):
- [ ] Python dataclass/parameter with docstring and validation (`__post_init__` or constructor)
- [ ] Rust config field (serde, `deny_unknown_fields`) + validation, if the core needs it
- [ ] `to_core()` mapping and tests for both
- [ ] `docs/reference/configuration.md` (CI checks every parameter/field is listed) and the guide
- [ ] `python/fanferro/AGENT_GUIDE.md` if it changes how applications should be written

**New event kind**:
- [ ] `EventKind` in `crates/fanferro-core/src/events.rs` (variant, name, level, description)
- [ ] `EventKind` in `python/fanferro/events.py` (same order) + `_DEFAULT_LEVELS`
- [ ] emit it (Rust `events.emit(kind, || Event::new(...))` or Python `client.events.emit(...)`)
- [ ] `uv run python scripts/generate_reference.py`; test that it is emitted

**New reserved attribute**: `types/reserved.rs` (and `ALL`), description in
`scripts/generate_reference.py`, regenerate the reference, hide it from `message.attributes` if internal.

**New native API**: Rust binding + `python/fanferro/_core.pyi` (CI compares names and methods) +
Python wrapper + tests.

**New backend operation**: trait method in `backend/mod.rs`, AWS adapter (map errors with
`map_sdk_error`), memory adapter (realistic semantics and error codes), tests in
`backend/memory/tests.rs`, `tests/aws_emulator.rs` or Python e2e.

**New codec / envelope**: implement the protocol, register in defaults only if it needs no
dependency at import, document in `docs/guides/message-formats.md`, round-trip tests incl. errors.

**Bug fix**: reproduce with a failing test first (memory backend if possible, MiniStack if it is a
protocol issue), fix, keep the test.

## Testing guidance

* Rust unit tests live next to the code (`#[cfg(test)] mod tests;`). Use the in-memory broker and
  `#[tokio::test(start_paused = true)]` for anything time-related: tests run instantly and deterministically.
* Python integration tests use the `client_factory` fixture → each scenario runs on the memory
  broker and on MiniStack. Use unique resource names (`name` fixture) and `wait_time=1`.
* Prefer asserting on events, `consumer.stats()`, `queue.approximate_count()` over sleeping.
  Helpers: `tests/helpers.py` (`consume_until`, `drain_queue`, `wait_for`).
* Time-sensitive tests: keep visibility timeouts ≥ 2 s and use generous `wait_for` timeouts; a test
  that is flaky on MiniStack usually reveals a real protocol subtlety (see ADR 0007).
* Examples are tests: if you change the API, update `examples/` (they run in CI).

## Documentation duties

Update in the same change: the relevant guide in `docs/guides/`, `docs/reference/*`
(generated files via the script), `python/fanferro/AGENT_GUIDE.md` and `skills/fanferro/SKILL.md`
for user-facing behaviour, `docs/architecture.md` for structural changes, a new ADR for decisions,
docstrings everywhere. README stays short and links to the docs.

## Commits, PRs and releases

* Conventional commits (`feat:`, `fix:`, `perf:`, `refactor:`, `docs:`, `test:`, `ci:`, `chore:`;
  `feat!:`/`BREAKING CHANGE:` for breaking changes). PR titles are checked; squash merges use them;
  the changelog is generated from them by git-cliff (`cliff.toml`).
* Pull requests are squash-merged: the title becomes the subject of the commit on main and its
  changelog line, the description its body. Write descriptions for that history, with no
  instructions for whoever merges, and each paragraph and list item on a single line: GitHub wraps
  them at 72 columns in the squash commit. Split large changes into stacked pull requests that each
  pass CI.
* Agents that can push to GitHub work like any contributor: they branch, commit, push, open their
  pull requests with `gh pr create`, follow the checks and answer reviews themselves, without
  handing anything over. Pushed commits are never amended, rebased or force-pushed; branches are
  brought up to date by merging. `.claude/skills/fanferro-pull-requests` covers pull requests,
  stacks, updates after reviews and merges, and failed checks; an agent that cannot push hands
  over a publish kit instead (`kits.md` in that skill).
* Versions: one version for the workspace (`Cargo.toml` `[workspace.package]`) - the Python package
  takes it from the binding crate. 0.x: breaking changes bump the minor version.
* Release: run the "Prepare release" workflow (or `uv run --no-project scripts/release.py <version>`)
  → polish the generated CHANGELOG.md section and squash-merge the release PR → the `v<version>` tag
  is created → `release.yml` builds wheels (Linux x86_64/aarch64 glibc+musl, macOS, Windows) and the
  sdist, smoke-tests them, creates the GitHub release with the changelog section as notes, and
  publishes to PyPI (trusted publishing). See `.claude/skills/fanferro-release`.

## Things to avoid

* Calling into Python from Rust threads (events are pulled, see ADR 0004; results are handed to
  the event loop, see ADR 0009).
* Threads or Tokio runtimes outside `crates/fanferro-py/src/runtime.rs`, or awaitables not created
  with `runtime::future_into_py` (or `runtime::pending_future` for settlements): they would
  outlive the `atexit` shutdown of the runtime.
* Holding a `parking_lot` lock across `.await`.
* Long `wait_time` in tests; `time.sleep` in async code; global state in Python modules.
* Logging message bodies (they may contain personal data) - log ids and types.
* Weakening validation to make a test pass; skipping the memory backend implementation.

---
name: fanferro-dev-workflow
description: Default workflow for any change to the fanferro library (Rust core, PyO3 bindings, Python API) - where code belongs, test-first steps, docs to update, and the checks to run before finishing. Use for every feature, fix or refactor in this repository.
---

# Changing fanferro

1. **Orient.** Read `AGENTS.md` and the parts of `docs/architecture.md` you touch. Decide the layer:
   protocol/timing/I/O → `crates/fanferro-core`; Python objects, handlers, hooks, OpenTelemetry →
   `python/fanferro`; conversions only → `crates/fanferro-py`.
2. **Write the failing test first.**
   - Rust logic: unit test next to the code using the in-memory broker
     (`MemoryBroker::new(...).backend()`) and `#[tokio::test(start_paused = true)]` for timing.
   - Python behaviour: `tests/integration/...` with the `client_factory` fixture (runs on memory and
     MiniStack), or `tests/unit/...` for pure logic.
3. **Implement** following the surrounding style (docstrings, typed, small functions, clear errors).
   Rust: no `unsafe`, no locks across `.await`, map AWS errors with `map_sdk_error`.
   Python: PEP 695 generics, `from __future__ import annotations`, no new required dependencies.
4. **Wire through every layer** the change crosses:
   - native API → `crates/fanferro-py` + `python/fanferro/_core.pyi`
   - config → Rust serde struct + Python dataclass `to_core()` + `docs/reference/configuration.md`
   - events → see the `fanferro-add-event-kind` skill
5. **Document**: guide in `docs/guides/`, `python/fanferro/AGENT_GUIDE.md` + `skills/fanferro/SKILL.md`
   for user-facing behaviour, ADR in `docs/adr/` for decisions, examples if the API changed.
6. **Verify** (all must pass):
   ```bash
   cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
   uv run ruff format python tests examples scripts && uv run ruff check python tests examples scripts
   uv run pyrefly check
   uv run python scripts/check_consistency.py
   uv run pytest
   ```
   (`make check` runs all of them.)
7. **Commit** with a conventional commit message (`feat(consumer): ...`, `fix(transport): ...`);
   breaking changes use `feat!:` and an explanation - they feed the changelog. Open the pull
   request (or a stack of them, or a publish kit when you cannot push) with
   `fanferro-pull-requests`.

# Development tasks. All Python commands run through uv, which rebuilds the native
# extension when Rust sources change.

.PHONY: help sync check lint fmt typecheck test test-fast test-rust e2e-rust docs-check reference wheels wheels-fast clean

help:  ## List targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

sync:  ## Install dependencies and build the extension
	uv sync

check: lint typecheck docs-check test-rust test  ## Everything CI runs

lint:  ## Format check and lint (Rust + Python)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	uv run ruff format --check python tests examples scripts
	uv run ruff check python tests examples scripts
	uvx --from actionlint-py actionlint

fmt:  ## Format and auto-fix
	cargo fmt --all
	uv run ruff format python tests examples scripts
	uv run ruff check --fix python tests examples scripts

typecheck:  ## Python type checking
	uv run pyrefly check

test:  ## Python tests (unit, integration on memory + MiniStack, e2e), one worker per CPU
	uv run pytest -n auto --dist worksteal

test-fast:  ## Python tests without the emulator
	uv run pytest -n auto --dist worksteal -m "not e2e"

test-rust:  ## Rust unit tests
	cargo test --workspace

e2e-rust:  ## Rust end-to-end tests (needs FANFERRO_TEST_ENDPOINT_URL)
	cargo test -p fanferro-core --test aws_emulator

docs-check:  ## Code/docs consistency checks
	uv run python scripts/check_consistency.py

reference:  ## Regenerate generated reference docs
	uv run python scripts/generate_reference.py

WHEEL_TARGETS ?= x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-unknown-linux-musl aarch64-unknown-linux-musl

wheels:  ## Release wheels (dist profile) for x86_64/aarch64 × glibc/musl, built like release.yml (needs zig)
	@for target in $(WHEEL_TARGETS); do \
		case $$target in *-musl) compat=musllinux_1_2 ;; *) compat=manylinux2014 ;; esac; \
		echo "==> $$target ($$compat)"; \
		maturin build --locked --profile dist --zig --target $$target --compatibility $$compat --out target/wheels || exit 1; \
	done

wheels-fast:  ## Quicker Linux wheels with the release profile (build-wheels)
	build-wheels -o target/wheels

clean:  ## Remove build artifacts
	cargo clean
	rm -rf target/wheels .pytest_cache .ruff_cache

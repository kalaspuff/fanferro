# Contributing

Thanks for helping! The full contributor guide - repository map, commands, rules and checklists - is
[AGENTS.md](AGENTS.md) (written for AI agents and humans alike). In short:

1. Install [Rust](https://rustup.rs) and [uv](https://docs.astral.sh/uv/), then `uv sync`.
2. Make your change with tests and docs; run `make check`.
3. Open a PR with a [conventional commit](https://www.conventionalcommits.org) title
   (`feat: ...`, `fix: ...`); the changelog is generated from it.

Design decisions are recorded in [docs/adr](docs/adr/README.md). Please open an issue first for
large changes.

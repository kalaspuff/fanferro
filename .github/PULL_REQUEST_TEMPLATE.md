<!-- Title: conventional commit, e.g. "feat(consumer): add per-route timeouts". It becomes the changelog entry. -->

## What and why

## Checklist

- [ ] Tests cover the change (memory broker and MiniStack where behaviour touches AWS)
- [ ] Docs updated (guide, `docs/reference/*`, `AGENT_GUIDE.md`/`skills/fanferro` if usage changed, ADR if a decision was made)
- [ ] `make check` passes locally (lint, types, clippy, consistency, tests)
- [ ] Breaking change? Marked with `!` in the title and explained above

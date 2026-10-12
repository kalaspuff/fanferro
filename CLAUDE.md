@AGENTS.md

## Claude Code notes

- Skills for working on this repository live in `.claude/skills/` (development workflow, adding an
  event kind or option, backend operations, end-to-end testing, releases). Use them.
- The usage guide for applications is `python/fanferro/AGENT_GUIDE.md`; `skills/fanferro/` is the
  skill application repositories can copy.
- Prefer `uv run` for every Python command; it keeps the native extension in sync with Rust changes.

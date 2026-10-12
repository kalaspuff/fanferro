---
name: fanferro-add-event-kind
description: Add a new event kind to fanferro (Rust EventKind, Python EventKind, default level, emission, generated reference docs, tests). Use when instrumenting a new action or failure with an event.
---

# Adding an event kind

Event kinds are dotted names `category.action` (`message.acked`, `lease.lost`). The Rust and Python
enums must list the same kinds **in the same order** (checked by `scripts/check_consistency.py`).

1. `crates/fanferro-core/src/events.rs`: add a line to the `event_kinds!` macro:
   `MyThing => ("category.my_thing", Info, "One sentence describing what happened."),`
   Levels: `Debug` (per-message chatter), `Info` (lifecycle), `Warning` (handled problem),
   `Error` (needs attention).
2. `python/fanferro/events.py`: add `MY_THING = "category.my_thing"` at the same position in
   `EventKind` and its level in `_DEFAULT_LEVELS`.
3. Emit it:
   - Rust: `client.events().emit(EventKind::MyThing, || Event::new(EventKind::MyThing, "text").field("queue", url))`
     (the closure only runs when the kind is enabled).
   - Python: `client.events.emit(EventKind.MY_THING, "text", queue=name, exception=error)` or
     `consumer._emit(...)` inside the consumer (adds consumer/queue/message context).
   Use the conventional field names: `queue`, `topic`, `destination`, `message_id`, `sqs_message_id`,
   `message_type`, `attempt`, `receive_count`, `duration`, `reason`, `error`.
4. `uv run python scripts/generate_reference.py` (updates `docs/reference/events.md`).
5. Test that it is emitted (e.g. `client.on("category.my_thing", seen.append)` in an integration test,
   or `events.drain()` in a Rust test after `set_enabled`).
6. If applications should react to it, mention it in `docs/guides/observability.md`.

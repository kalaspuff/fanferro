---
name: fanferro-add-option
description: Add or change a configuration option of the fanferro Client, a resource, a consumer or a policy end to end (Python API, Rust config, validation, docs reference, tests). Use when introducing any new knob.
---

# Adding an option

1. **Python surface.** Add the parameter (keyword-only) or dataclass field with a sensible default,
   type hint and docstring entry. Validate in `__post_init__` / the constructor with a message that
   says what is allowed. Policies are frozen dataclasses with `slots=True`.
2. **Core mapping (if Rust needs it).** Add the field to the serde struct (`#[serde(default)]`,
   `deny_unknown_fields` is on, so names must match exactly), validate it in the Rust config
   (`ConsumerConfig::validate`, `OffloadSettings::validate`, ...) and map it in the Python
   `to_core()`/config dict (`Consumer._start`, `Client._client_config`, `Client._aws_config`).
   Durations cross the boundary as float seconds.
3. **Behaviour + tests.** Unit test validation; integration test the effect (both backends).
4. **Docs.** Add a row to `docs/reference/configuration.md` - CI fails if any parameter or field is
   missing there. Update the guide that explains the feature and, if it changes recommended usage,
   `python/fanferro/AGENT_GUIDE.md`.
5. **Defaults are API.** Changing a default is a behaviour change: note it in the commit message
   (`feat!:` if it can break applications) and consider an ADR.

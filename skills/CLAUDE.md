# CLAUDE.md — Standing Instructions for Claude Code

## Protected Files

The following files must NOT be modified by Claude Code under any circumstances,
including when making pipeline changes, fixing bugs, or responding to user requests:

- `SKILL.md` — pipeline skill definition. Changes to this file must be made by
  the user directly, or explicitly requested with the exact text:
  **"update SKILL.md"** or **"edit SKILL.md"**.

If a change would logically require updating `SKILL.md`, flag it to the user
and describe what the update should contain — but do not make the edit.

---

## General Behaviour

- Always confirm before modifying any file in `pipeline_specs/`.
- Do not reorganise, rename, or delete files without explicit instruction.
- When in doubt about scope, ask before acting.

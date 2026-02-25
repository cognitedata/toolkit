---
name: stage-it
description: Stage all git changes and run pre-commit hooks, fixing any failures automatically. Use when the user says "stage it".
---

# Stage It

When the user says **"stage it"**:

## 1. Stage changes

- Run `git add .` to stage all changes.

## 2. Run pre-commit hooks

- Run `uv run pre-commit run --all-files` to validate staged changes.
- If pre-commit hooks fail, fix every reported issue, re-stage the fixes, and re-run. Repeat until all hooks pass.

# Coding Conventions

- **Never** use `from __future__ import annotations`.
- Use **Pydantic** for data classes where appropriate.
- Keep **imports at the top** of the file.
- Be **pragmatic and minimalistic** about tests — cover the bug or feature, don't over-test.

## Development setup

Always sync with optional dependencies before running linters or tests — CI does the same:

```bash
uv sync --all-extras
```

Plain `uv sync` omits `pyarrow` (under the `[table]` extra). That changes the type checker's view of optional code
such as `cognite_toolkit/_cdf_tk/utils/fileio/_writers.py`. Use `uv sync --all-extras` in every clone and worktree.

Type checking is done by `ty` through pre-commit, using the project venv (`.venv`):

```bash
uv run pre-commit run ty --all-files
```

## Troubleshooting

### Never use `--no-verify` to bypass pre-commit

Do **not** commit with `git commit --no-verify` to bypass a type-checking failure. Fix the error instead.

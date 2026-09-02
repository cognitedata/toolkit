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

Plain `uv sync` omits `pyarrow` (under the `[table]` extra). That changes mypy's view of optional code such as
`cognite_toolkit/_cdf_tk/utils/fileio/_writers.py`: ignores can look unused locally while CI still needs them,
or the other way around. Use `uv sync --all-extras` in every clone and worktree.

Run mypy only via the project venv (same invocation as pre-commit):

```bash
uv run mypy cognite_toolkit/ tests_smoke/ --config-file pyproject.toml
```

Do not use a global `mypy` from `~/.local/bin` or single-file mypy on `_writers.py` — results differ from CI.

## Troubleshooting

### mypy: command not found

If pre-commit fails with `mypy: command not found`, run it explicitly via the project venv:

```bash
uv run mypy cognite_toolkit/ tests_smoke/ --config-file pyproject.toml
```

### Never use `--no-verify` to bypass mypy

Do **not** commit with `git commit --no-verify` to bypass a mypy failure. Fix the error instead.
If the error is on a line that already has a `# type: ignore` comment that CI needs, do not remove it.

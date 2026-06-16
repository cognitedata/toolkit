# Coding Conventions

- **Never** use `from __future__ import annotations`.
- Use **Pydantic** for data classes where appropriate.
- Keep **imports at the top** of the file.
- Be **pragmatic and minimalistic** about tests — cover the bug or feature, don't over-test.

## Troubleshooting

### mypy: command not found

If pre-commit fails with `mypy: command not found`, run it explicitly via the project venv:

```bash
uv run mypy cognite_toolkit/ tests_smoke/ --config-file pyproject.toml
```

### Never use `--no-verify` to bypass mypy

Do **not** commit with `git commit --no-verify` to bypass a mypy failure. Fix the error instead.
If the error is on a line that already has a `# type: ignore` comment that CI needs, do not remove it.

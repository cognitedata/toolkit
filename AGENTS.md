# Coding Conventions

- **Never** use `from __future__ import annotations`.
- Use **Pydantic** for data classes where appropriate.
- Keep **imports at the top** of the file.
- Be **pragmatic and minimalistic** about tests — cover the bug or feature, don't over-test.

## Troubleshooting

### pyenv / dmypy: command not found

If pre-commit fails with `dmypy: command not found` (e.g. when pyenv global is Python 3.14 and mypy
isn't installed there), run mypy via the project venv instead:

```bash
uv run mypy cognite_toolkit/ tests_smoke/ --config-file pyproject.toml
```

To bypass the failing hook and commit: `git commit --no-verify`.

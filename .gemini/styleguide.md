# Cognite Python Style Guide

## Key Principles

- **Strong Typing**: Use type hints extensively, checked with `ty`. Avoid `Any` when possible
- **Type Safety**: Use Pydantic models (preferred) or dataclasses for complex data
  structures instead of untyped dictionaries
- **IO Safety**: Always use typed data structures for file operations and data parsing
- **Readability**: Code should be immediately understandable
- **Maintainability**: Write code that is easy to modify and extend
- **Consistency**: Follow established patterns across the codebase

## Principles on doing pull request reviews

- **Main point first.** Start with the key feedback or required action.
- **Be concise.** Use short, direct comments. Avoid unnecessary explanations.
- **Actionable suggestions.** If something needs fixing, state exactly what and how.
- **One issue per comment.** Separate unrelated feedback for clarity.
- **Code, not prose.** Prefer code snippets or examples over long text.
- **Background only if needed.** Add context only if the main point isn't obvious.

## How to do pull request summaries

- **Short recap.** Summarize the main point of the PR in one or two sentences.
- **Don't repeat the PR description.** Only add new or clarifying information.
- **Be brief unless needed.** Only write a longer summary if the PR description
  is missing crucial details.
- **Extend, don't duplicate.** If more detail is needed, clearly state what is
  missing from the PR description and add only the necessary context.

## Line Length and Formatting

- **Maximum line length**: 120 characters (configured in ruff)
- **Target Python version**: 3.11+ (`requires-python = ">=3.11"`). Python 3.11 features such as
  `datetime.UTC`, `typing.Self`, and `StrEnum` are allowed. Don't suggest Python 3.10 fallbacks such as
  `timezone.utc`
- **Indentation**: 4 spaces per level

## Type Hints

- **Required**: All functions, methods, and class attributes must have type hints
- **No `from __future__ import annotations`**: Never use it
- **Avoid `Any`**: Use specific types whenever possible
- **Complex data**: Use Pydantic models (preferred) or dataclasses instead of `dict[str, Any]`
- **File operations**: Always parse file content into typed structures

```python
# Good - typed data structure
@dataclass
class Config:
    model_name: str
    temperature: float
    max_tokens: int

def load_config(path: Path) -> Config:
    data = json.loads(path.read_text())
    return Config(**data)

# Bad - untyped dictionary
def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())
```

## Imports

- **Group imports**: Standard library, third-party, local application
- **Absolute imports**: Use absolute imports by default
- **Relative imports**: Allowed only for sibling modules within the same package (single dot), e.g.
  `from ._base import ToolkitRuleSet` inside `rules/`, or re-exports in a package's `__init__.py`.
  Never import from a parent package (`from ..x import y`)
- **Imports at the top** of the file
- **Sorting** is handled by ruff (`I` rules), don't comment on import order
- **Type checking imports**: `TYPE_CHECKING` is optional, mainly used to avoid circular imports
- **`_cdf_tk` imports**: Always use the full path `cognite_toolkit._cdf_tk`
when importing from `_cdf_tk`, never use `from _cdf_tk import ...`

```python
import json
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel
from rich.console import Console

# Good - full path for _cdf_tk imports
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.commands.repo import RepoCommand

# Good - relative import of a sibling module in the same package
from ._base import ToolkitRuleSet

# Bad - don't import _cdf_tk without the cognite_toolkit prefix
# from _cdf_tk.feature_flags import Flags  # ❌

# Bad - don't use relative imports from a parent package
# from ..feature_flags import Flags  # ❌

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client import ToolkitClient
```

## Naming Conventions

- **Variables/functions**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Classes**: `PascalCase`
- **Modules**: `snake_case`
- **Private members**: Single leading underscore `_private`

## Docstrings

Use concise docstrings with Args/Returns format. Don't repeat the types in the docstring, they are
already in the type hints.

```python
def retrieve(self, items: Sequence[InternalId], ignore_unknown_ids: bool = False) -> list[FunctionResponse]:
    """Retrieve functions from CDF.

    Args:
        items: The IDs of the functions to retrieve.
        ignore_unknown_ids: Whether to ignore IDs that are not found.

    Returns:
        The retrieved functions.
    """

def walk_sdk_documentation(content: Tag, parser: Parser[T]) -> Iterable[T]:
    """Parse the content of a file and yields documents. The parser controls how
    the sections are transformed into documents."""
    # Implementation here
```

**Docstring patterns**:

- Start with a concise description
- Use `Args:` and `Returns:` for complex functions
- Omit obvious parameter descriptions
- Keep descriptions brief and factual
- Ok for `__init__` methods to omit docstring

## Error Handling

- **Specific exceptions**: Avoid broad `Exception` catches. Catch, for example, `ToolkitAPIError`
- **User-facing errors**: Raise a subclass of `ToolkitError` (`cognite_toolkit._cdf_tk.exceptions`)
  with a meaningful message
- **Type safety**: Return `None` or use Union types for fallible operations

```python
def load_config(path: Path) -> Config | None:
    try:
        return Config.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError) as e:
        MediumSeverityWarning(f"Failed to load config from {path}: {e}").print_warning()
        return None
```

## Tooling

Sync with all optional dependencies before running any checks, CI does the same:

```bash
uv sync --all-extras
```

- **All checks**: `uv run pre-commit run --all-files` runs ruff check, ruff format, `ty`, and markdownlint
- **Type checker**: `ty` through pre-commit, `uv run pre-commit run ty --all-files`
- **Tests**: `pytest`
- Never commit with `--no-verify` to bypass the checks

## Dependencies

- **`httpx2`**: Toolkit uses `httpx2`, which has replaced `httpx`. `import httpx2` is correct and not a
  typo for `httpx`. Don't suggest changing it to `httpx`
- In tests, `import httpx` also works, as `tests/httpx2_alias_plugin.py` aliases `httpx` to `httpx2`
  (`httpx2.alias_httpx()`) such that libraries typed against `httpx`, e.g. `respx`, accept `httpx2` objects

## Ruff Configuration Deviations

Ruff runs through pre-commit with the arguments in `.pre-commit-config.yaml`:

- **Line length**: 120 characters (vs PEP 8's 79). Ruff format does not split long strings, so these
  can still exceed the limit
- **Target version**: `py311`
- **Selected rules**: `E`, `W`, `F`, `I` (isort), `T`, `RUF`, `TID` (tidy imports), `UP` (pyupgrade)
- **Ignored rules**: `E731` (lambda assignment), `E501` (line too long), `W605` (invalid escape
  sequence), `T201` (print), `UP007` (`X | Y` unions, not auto-fixed, but `X | None` is still preferred over
  `Optional[X]`)
- `UP017` is enforced, use `datetime.UTC` instead of `datetime.timezone.utc`

## Data Structures

**Prefer typed structures**, Pydantic models are preferred:

```python
# Good
class FunctionError(BaseModel):
    function_name: str
    message: str

# Good
@dataclass
class FunctionError:
    function_name: str
    message: str

# Avoid
error_data = {"function_name": "foo", "message": "bar"}
```

## Tests

- Use `pytest`
- Be pragmatic and minimal: cover the bug or feature, don't over-test
- Don't test Pydantic models or dataclasses unless they have custom logic

## User Output and Logging

- **User-facing output**: Use `rich` for console output
- **Warnings to the user**: Use the classes in `cognite_toolkit._cdf_tk.tk_warnings`, e.g.
  `MediumSeverityWarning("...").print_warning()`
- **`logging`**: Only for internal diagnostics, such as in the HTTP client. Don't suggest replacing
  `rich` output or Toolkit warnings with `logging`

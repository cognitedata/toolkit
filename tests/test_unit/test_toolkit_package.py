import ast
import re
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pytest

from tests.constants import REPO_ROOT

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

CDF_TK_PATH = REPO_ROOT / "cognite_toolkit" / "_cdf_tk"

# Mapping from PyPI package names to Python import names
# Only needed when they differ
_PACKAGE_TO_IMPORT_NAME: dict[str, str] = {
    "cognite-sdk": "cognite",
    "python-dotenv": "dotenv",
    "pyyaml": "yaml",
    "python-dateutil": "dateutil",
    "sentry-sdk": "sentry_sdk",
    "cognite-neat": "cognite",  # cognite.neat is part of cognite namespace
}


Violation = tuple[int, str]  # (lineno, human-readable reason)


@dataclass(frozen=True)
class ImportInfo:
    """A normalized view of a single import statement.

    - `module` is the dotted module path being imported from
      (e.g. `cognite.client.data_classes` for `from cognite.client.data_classes import X`,
       or `cognite.client` for `import cognite.client`).
    - `names` is the tuple of imported names for `from ... import a, b` statements,
      or an empty tuple for plain `import X` statements.
    - `lineno` is the source line.
    - `is_from` distinguishes `from X import ...` from `import X`.

    A single `from X import a, b, c` statement is represented as ONE ImportInfo so
    predicates naturally produce one violation per statement.
    """

    module: str
    names: tuple[str, ...]
    lineno: int
    is_from: bool

    def format(self) -> str:
        """Format the import as it would appear in source code."""
        if self.is_from:
            return f"from {self.module} import {', '.join(self.names)}"
        return f"import {self.module}"


def test_no_private_third_party_imports() -> None:
    """
    Test that checks for imports of private modules/classes/functions from third-party packages.

    Private imports are fragile as they may break between minor/patch versions of dependencies.
    This test identifies all such imports so they can be tracked and potentially refactored.
    """
    # We are not copying over protobuf files, so private imports from cognite.client._proto are currently acceptable.
    # We also need to look up the version of CogniteSDK as we dynamically create requirement.txt files for
    # Streamlit apps.
    # cognite.neat is owned by the same team, so private imports are acceptable there.
    third_party = _get_third_party_packages()
    exceptions = ("cognite.client._proto", "cognite.client._version", "cognite.neat")

    def check(imp: ImportInfo) -> Violation | None:
        parts = imp.module.split(".")
        if parts[0] not in third_party:
            return None
        if any(imp.module.startswith(exc) for exc in exceptions):
            return None
        # Private module component?
        for i, part in enumerate(parts[1:], start=1):
            if _is_private_name(part):
                private_module = ".".join(parts[: i + 1])
                return imp.lineno, f"imports from private module '{private_module}'"
        # Private imported name (only meaningful for `from ... import name`)?
        if imp.is_from:
            for name in imp.names:
                if _is_private_name(name):
                    return imp.lineno, f"imports private name '{name}' from '{imp.module}'"
        return None

    _assert_import_violations(
        check,
        "private imports from third-party packages",
        expected_total=1,
    )


def test_no_cognite_sdk_imports() -> None:
    """
    Test that checks for any imports from cognite.client (both public and private).

    The goal is to fully remove the cognite-sdk dependency from the toolkit (with the exception of Auth and protobuf files).
    This test tracks progress toward that goal.
    """

    def check(imp: ImportInfo) -> Violation | None:
        if imp.module == "cognite.client" or imp.module.startswith("cognite.client."):
            return imp.lineno, imp.format()
        return None

    _assert_import_violations(check, "cognite.client imports", expected_total=90)


def test_utils_module_independent() -> None:
    """
    Test that `cognite_toolkit._cdf_tk.utils` does not depend on other modules within
    `cognite_toolkit._cdf_tk`, with the exception of `cognite_toolkit._cdf_tk.client`.

    The utils module should be a leaf module in the package dependency graph so it can
    be safely imported from anywhere without causing circular imports.
    """
    allowed_prefixes = (
        "cognite_toolkit._cdf_tk.utils",
        "cognite_toolkit._cdf_tk.client",
        "cognite_toolkit._cdf_tk.tk_warnings",
        "cognite_toolkit._cdf_tk.constants",
        "cognite_toolkit._cdf_tk.exceptions",
        "cognite_toolkit._cdf_tk.cdf_toml",
    )
    package_prefix = "cognite_toolkit._cdf_tk"

    def check(imp: ImportInfo) -> Violation | None:
        if not imp.module.startswith(package_prefix) or imp.module.startswith(allowed_prefixes):
            return None
        return imp.lineno, imp.format()

    _assert_import_violations(
        check,
        "disallowed intra-package imports in utils module",
        root=CDF_TK_PATH / "utils",
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _assert_import_violations(
    check: Callable[[ImportInfo], Violation | None],
    description: str,
    root: Path = CDF_TK_PATH,
    expected_total: int | None = None,
) -> None:
    """Walk all Python files under `root`, apply `check` to every import, and fail if
    there are any violations (or if the count doesn't match `expected_total`)."""
    violations_by_file: dict[str, list[Violation]] = {}
    for py_file in _get_all_python_files(root):
        tree = _parse_file(py_file)
        if tree is None:
            continue
        file_violations = [v for imp in _iter_imports(tree, py_file) if (v := check(imp)) is not None]
        if file_violations:
            violations_by_file[py_file.relative_to(REPO_ROOT).as_posix()] = file_violations
    _fail_with_violations(violations_by_file, description, expected_total)


def _fail_with_violations(
    violations_by_file: dict[str, list[Violation]],
    description: str,
    expected_total: int | None = None,
) -> None:
    """Fail the current test with a formatted report of violations.

    If `expected_total` is given, asserts the count matches (used to track progress
    on a known list of violations). Otherwise fails if there are any violations.
    """
    total = sum(len(v) for v in violations_by_file.values())
    if expected_total is not None:
        assert total == expected_total, f"Expected {expected_total} {description}, found {total}"
        return
    if not violations_by_file:
        return

    lines = [f"Found {total} {description}:", ""]
    for file_path, items in sorted(violations_by_file.items()):
        lines.append(f"  {file_path}:")
        for lineno, reason in items:
            lines.append(f"    Line {lineno}: {reason}")
        lines.append("")
    pytest.fail("\n".join(lines))


def _parse_file(file_path: Path) -> ast.AST | None:
    """Read and parse a Python file, returning None on syntax/decoding errors."""
    try:
        return ast.parse(file_path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return None


def _iter_imports(tree: ast.AST, file_path: Path) -> Iterator[ImportInfo]:
    """Yield an ImportInfo for every import statement in `tree`.

    Relative imports (`from . import x`, `from ..y import z`) are resolved to their
    absolute dotted path based on `file_path`'s location within the repo.
    Plain `import a, b` statements yield one ImportInfo per name (matching Python's
    own semantics of separate binding), while `from X import a, b` yields a single
    ImportInfo with `names=("a", "b")`.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = _resolve_module(node, file_path)
            if module is None:
                continue
            yield ImportInfo(
                module=module,
                names=tuple(alias.name for alias in node.names),
                lineno=node.lineno,
                is_from=True,
            )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                yield ImportInfo(module=alias.name, names=(), lineno=node.lineno, is_from=False)


def _resolve_module(node: ast.ImportFrom, file_path: Path) -> str | None:
    """Resolve an `ast.ImportFrom` node's module to an absolute dotted path."""
    if not node.level:
        return node.module
    # Relative import: resolve against the file's package.
    package_parts = file_path.relative_to(REPO_ROOT).with_suffix("").parts
    base_parts = list(package_parts[:-1])  # drop the file name
    if node.level > 1:
        base_parts = base_parts[: -(node.level - 1)]
    if not base_parts:
        return node.module
    return ".".join([*base_parts, node.module]) if node.module else ".".join(base_parts)


def _parse_package_name(dependency: str) -> str:
    """Extract package name from a dependency string like 'package>=1.0.0'."""
    # Match the package name (before any version specifier, semicolon for markers, or whitespace)
    match = re.match(r"^([a-zA-Z0-9_-]+)", dependency.strip())
    return match.group(1).lower() if match else ""


def _package_to_import_name(package_name: str) -> str:
    """Convert a PyPI package name to its Python import name."""
    if package_name in _PACKAGE_TO_IMPORT_NAME:
        return _PACKAGE_TO_IMPORT_NAME[package_name]
    # Default: replace hyphens with underscores
    return package_name.replace("-", "_")


@lru_cache(maxsize=1)
def _get_third_party_packages() -> frozenset[str]:
    """
    Read dependencies from pyproject.toml and return the set of import names.

    This reads the main dependencies and optional dependencies to build a complete
    set of third-party packages that the project depends on.
    """
    pyproject_path = REPO_ROOT / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        pyproject = tomllib.load(f)

    import_names: set[str] = set()

    # Get main dependencies
    dependencies = pyproject.get("project", {}).get("dependencies", [])
    for dep in dependencies:
        package_name = _parse_package_name(dep)
        if package_name:
            import_names.add(_package_to_import_name(package_name))

    # Get optional dependencies
    optional_deps = pyproject.get("project", {}).get("optional-dependencies", {})
    for deps in optional_deps.values():
        for dep in deps:
            package_name = _parse_package_name(dep)
            if package_name:
                import_names.add(_package_to_import_name(package_name))

    return frozenset(import_names)


def _is_private_name(name: str) -> bool:
    """Check if a name is private (starts with underscore but not dunder)."""
    return name.startswith("_") and not name.startswith("__")


def _get_all_python_files(directory: Path) -> list[Path]:
    """Get all Python files in a directory recursively."""
    return list(directory.rglob("*.py"))

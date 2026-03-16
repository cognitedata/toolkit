import ast
import re
import sys
from collections.abc import Callable
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

# Modules to exclude from private import checks
# cognite.neat is owned by the same team, so private imports are acceptable
_EXCEPTION_MODULES: frozenset[str] = frozenset({"cognite.neat"})


def _assert_import_violations(
    extract_fn: Callable[[Path], list[tuple[str, int, str]]],
    description: str,
    expected_total: int | None = None,
    exceptions: set[str] | None = None,
) -> None:
    """Walk all Python files in CDF_TK_PATH and fails if there are any violations or if the
    total number of violations doesn't match expected_total (if provided).
    """
    all_violations: dict[str, list[tuple[str, int, str]]] = {}
    exceptions = exceptions or set()
    for py_file in _get_all_python_files(CDF_TK_PATH):
        violations = extract_fn(py_file)
        violations = [
            (module, lineno, reason)
            for module, lineno, reason in violations
            if not any(module.startswith(exc) for exc in exceptions)
        ]

        if violations:
            relative_path = py_file.relative_to(REPO_ROOT).as_posix()
            all_violations[relative_path] = violations

    total = sum(len(v) for v in all_violations.values())
    if expected_total is not None:
        assert total == expected_total
    elif all_violations:
        lines = [f"Found {total} {description}:", ""]
        for file_path, imports in sorted(all_violations.items()):
            lines.append(f"  {file_path}:")
            for module, lineno, reason in imports:
                lines.append(f"    Line {lineno}: {reason}")
            lines.append("")

        pytest.fail("\n".join(lines))


def test_no_private_third_party_imports() -> None:
    """
    Test that checks for imports of private modules/classes/functions from third-party packages.

    Private imports are fragile as they may break between minor/patch versions of dependencies.
    This test identifies all such imports so they can be tracked and potentially refactored.
    """
    # We are not copying over protobuf files, so private imports from cognite.client._proto are currently acceptable.
    # We also need to look up the version of CogniteSDK as we dynamically create requirement.txt files for
    # Streamlit apps
    _assert_import_violations(
        _extract_private_imports,
        "private imports from third-party packages",
        exceptions={"cognite.client._proto", "cognite.client._version"},
        expected_total=3,
    )


def test_no_cognite_sdk_imports() -> None:
    """
    Test that checks for any imports from cognite.client (both public and private).

    The goal is to fully remove the cognite-sdk dependency from the toolkit (with the exception of Auth and protobuf files).
    This test tracks progress toward that goal.
    """
    _assert_import_violations(_extract_cognite_sdk_imports, "cognite.client imports", 131)


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


def _is_exception_module(module: str) -> bool:
    """Check if the module is in the exception list."""
    return any(module.startswith(exc) for exc in _EXCEPTION_MODULES)


def _get_all_python_files(directory: Path) -> list[Path]:
    """Get all Python files in a directory recursively."""
    return list(directory.rglob("*.py"))


def _extract_private_imports(file_path: Path) -> list[tuple[str, int, str]]:
    """
    Extract private imports from a Python file.

    Returns a list of tuples: (import_statement, line_number, reason)
    """
    private_imports: list[tuple[str, int, str]] = []
    third_party_packages = _get_third_party_packages()

    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError):
        return private_imports

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue

            module_parts = node.module.split(".")

            # Check if this is a third-party import
            root_package = module_parts[0]
            if root_package not in third_party_packages:
                continue

            # Skip modules in the exception list
            if _is_exception_module(node.module):
                continue

            # Check if any module part is private (except root package)
            for i, part in enumerate(module_parts[1:], start=1):
                if _is_private_name(part):
                    private_module = ".".join(module_parts[: i + 1])
                    reason = f"imports from private module '{private_module}'"
                    private_imports.append((node.module, node.lineno, reason))
                    break
            else:
                # Check if any imported name is private
                for alias in node.names:
                    if _is_private_name(alias.name):
                        reason = f"imports private name '{alias.name}' from '{node.module}'"
                        private_imports.append((node.module, node.lineno, reason))

        elif isinstance(node, ast.Import):
            for alias in node.names:
                module_parts = alias.name.split(".")
                root_package = module_parts[0]

                if root_package not in third_party_packages:
                    continue

                # Skip modules in the exception list
                if _is_exception_module(alias.name):
                    continue

                # Check if any module part is private
                for i, part in enumerate(module_parts[1:], start=1):
                    if _is_private_name(part):
                        private_module = ".".join(module_parts[: i + 1])
                        reason = f"imports private module '{private_module}'"
                        private_imports.append((alias.name, node.lineno, reason))
                        break

    return private_imports


def _extract_cognite_sdk_imports(file_path: Path) -> list[tuple[str, int, str]]:
    """
    Extract all cognite.client imports (both public and private) from a Python file.

    Returns a list of tuples: (module_path, line_number, reason)
    """
    cognite_imports: list[tuple[str, int, str]] = []

    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError):
        return cognite_imports

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            if node.module == "cognite.client" or node.module.startswith("cognite.client."):
                names = ", ".join(a.name for a in node.names)
                reason = f"from {node.module} import {names}"
                cognite_imports.append((node.module, node.lineno, reason))

        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "cognite.client" or alias.name.startswith("cognite.client."):
                    reason = f"import {alias.name}"
                    cognite_imports.append((alias.name, node.lineno, reason))

    return cognite_imports

import ast
import re
import sys
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


@pytest.mark.xfail(
    reason="This test is currently expected to fail as there are known private imports that need to be refactored."
)
def test_no_private_third_party_imports() -> None:
    """
    Test that checks for imports of private modules/classes/functions from third-party packages.

    Private imports are fragile as they may break between minor/patch versions of dependencies.
    This test identifies all such imports so they can be tracked and potentially refactored.
    """
    all_private_imports: dict[str, list[tuple[str, int, str]]] = {}

    for py_file in _get_all_python_files(CDF_TK_PATH):
        private_imports = _extract_private_imports(py_file)
        if private_imports:
            relative_path = py_file.relative_to(REPO_ROOT).as_posix()
            all_private_imports[relative_path] = private_imports

    if all_private_imports:
        # Format the error message
        lines = [f"Found {len(all_private_imports)} private imports from third-party packages:", ""]
        for file_path, imports in sorted(all_private_imports.items()):
            lines.append(f"  {file_path}:")
            for module, lineno, reason in imports:
                lines.append(f"    Line {lineno}: {reason}")
            lines.append("")

        pytest.fail("\n".join(lines))


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

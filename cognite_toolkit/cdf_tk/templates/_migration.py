from __future__ import annotations

from pathlib import Path

from rich import print
from rich.markdown import Markdown

from cognite_toolkit._version import __version__ as current_version
from cognite_toolkit.cdf_tk.templates._constants import COGNITE_MODULES
from cognite_toolkit.cdf_tk.templates._utils import _get_cognite_module_version, iterate_modules
from cognite_toolkit.cdf_tk.templates.data_classes._migration_yaml import MigrationYAML


def print_changes(project_dir: Path) -> None:
    previous_version = _get_cognite_module_version(project_dir)

    _print_difference(project_dir, previous_version)


def _print_difference(project_dir: Path, previous_version: str) -> None:
    migration_yaml = MigrationYAML.load()

    from_version = next((no for no, entry in enumerate(migration_yaml) if entry.version == previous_version), None)
    if from_version is None:
        print(f"Failed to find migration from version '{previous_version}'.")
        exit(1)

    migrations = migration_yaml[:from_version]

    all_changes = migrations.as_one_change()

    suffix = f"from version {previous_version!r} to {current_version!r}"
    if all_changes.tool:
        print(Markdown(f"# Found {len(all_changes.tool)} changes to the 'cdf-tk' {suffix}:"))
        for change in all_changes.tool:
            change.print()

    used_resources = {
        file_path.relative_to(module_path).parts[0]
        for module_path, file_paths in iterate_modules(project_dir)
        for file_path in file_paths
    }

    resources = {resource: changes for resource, changes in all_changes.resources.items() if resource in used_resources}
    if resources:
        print(Markdown(f"# Found {len(resources)} changes to resources {suffix}:"))
        for resource, changes in resources.items():
            print(Markdown(f"# Resource: {resource}"))
            for change in changes:
                change.print()

    cognite_modules_dir = project_dir / COGNITE_MODULES
    used_cognite_modules = {
        ".".join(module_path.relative_to(cognite_modules_dir).parts)
        for module_path, file_paths in iterate_modules(cognite_modules_dir)
    }
    cognite_modules = {
        module: changes for module, changes in all_changes.cognite_modules.items() if module in used_cognite_modules
    }
    if cognite_modules:
        print(Markdown(f"# Found {len(cognite_modules)} changes to cognite modules {suffix}:"))

        for module, changes in cognite_modules.items():
            print(Markdown(f"# Module: {module}"))
            for change in changes:
                change.print()


if __name__ == "__main__":
    _print_difference(Path(__file__).parent.parent.parent, "0.1.0b1")

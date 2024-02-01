from __future__ import annotations

from collections import UserList, defaultdict
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from rich import print
from rich.markdown import Markdown
from rich.panel import Panel

from cognite_toolkit._version import __version__ as current_version
from cognite_toolkit.cdf_tk.templates._constants import COGNITE_MODULES
from cognite_toolkit.cdf_tk.templates._utils import _get_cognite_module_version, iterate_modules
from cognite_toolkit.cdf_tk.utils import load_yaml_inject_variables


@dataclass
class Change:
    title: str
    steps: list[str]

    @classmethod
    def load(cls, data: dict[str, Any]) -> Change:
        return cls(
            title=data["title"],
            steps=data["steps"],
        )

    def print(self) -> None:
        print(Panel.fit(self.title, title="Change"))
        for no, step in enumerate(self.steps, 1):
            if step.startswith("<Panel>"):
                print(Panel.fit(step.removeprefix("<Panel>"), title=""))
            else:
                print(Markdown(f" - Step {no}: {step}"))


@dataclass
class VersionChanges:
    version: str
    cognite_modules: dict[str, list[Change]]
    resources: dict[str, list[Change]]
    tool: list[Change]

    @classmethod
    def load(cls, data: dict[str, Any]) -> VersionChanges:
        return cls(
            version=data["version"],
            cognite_modules={
                key: [Change.load(change) for change in changes] for key, changes in data[COGNITE_MODULES].items()
            },
            resources={key: [Change.load(change) for change in changes] for key, changes in data["resources"].items()},
            tool=[Change.load(change) for change in data["tool"]],
        )


class MigrationYAML(UserList):
    filename: str = "_migration.yaml"

    def __init__(self, collection: Collection[VersionChanges] | None = None) -> None:
        super().__init__(collection or [])

    @classmethod
    def load(cls) -> MigrationYAML:
        filepath = Path(__file__).parent / cls.filename
        loaded = load_yaml_inject_variables(filepath, {"VERSION": current_version})
        return cls([VersionChanges.load(cast(dict[str, Any], version_changes)) for version_changes in loaded])

    def as_one_change(self) -> VersionChanges:
        tool: list[Change] = []
        resources: dict[str, list[Change]] = defaultdict(list)
        cognite_modules: dict[str, list[Change]] = defaultdict(list)
        for version_changes in self:
            tool.extend(version_changes.tool)
            for resource, changes in version_changes.resources.items():
                resources[resource].extend(changes)
            for module, changes in version_changes.cognite_modules.items():
                cognite_modules[module].extend(changes)

        return VersionChanges(
            version=f"{self[0].version} - {self[-1].version}",
            tool=tool,
            resources=resources,
            cognite_modules=cognite_modules,
        )


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

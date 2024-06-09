from __future__ import annotations

from collections import UserList, defaultdict
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print
from rich.markdown import Markdown
from rich.panel import Panel

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils import iterate_modules, load_yaml_inject_variables
from cognite_toolkit._version import __version__ as current_version


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
        print()  # Add a newline before each change
        print(Panel.fit(self.title, title="Change"))
        for no, step in enumerate(self.steps, 1):
            if step.startswith("<Panel>"):
                print(Panel.fit(step.removeprefix("<Panel>"), title=""))
            else:
                print(Markdown(f" - Step {no}: {step}"))


@dataclass
class VersionChanges:
    version: Version
    cognite_modules_hash: str
    cognite_modules: dict[str, list[Change]]
    resources: dict[str, list[Change]]
    tool: list[Change]
    from_version: Version | None = None

    @classmethod
    def load(cls, data: dict[str, Any]) -> VersionChanges:
        return cls(
            version=parse_version(data["version"]),
            cognite_modules_hash=data["cognite_modules_hash"],
            cognite_modules={
                key: [Change.load(change) for change in changes] for key, changes in data[COGNITE_MODULES].items()
            },
            resources={key: [Change.load(change) for change in changes] for key, changes in data["resources"].items()},
            tool=[Change.load(change) for change in data["tool"]],
        )

    def print(self, project_dir: Path, previous_version: str, print_cognite_module_changes: bool = True) -> None:
        used_resources = {
            file_path.relative_to(module_path).parts[0]
            for module_path, file_paths in iterate_modules(project_dir)
            for file_path in file_paths
        }

        changes_resources = {
            resource: changes for resource, changes in self.resources.items() if resource in used_resources
        }

        cognite_modules_dir = project_dir / COGNITE_MODULES
        used_cognite_modules = {
            ".".join(module_path.relative_to(cognite_modules_dir).parts)
            for module_path, file_paths in iterate_modules(cognite_modules_dir)
        }
        changes_cognite_modules = {
            module: changes for module, changes in self.cognite_modules.items() if module in used_cognite_modules
        }

        suffix = f"from version {previous_version!r} to {current_version!r}"
        if self.tool:
            print(Markdown(f"# Found {len(self.tool)} changes to 'cdf-tk' CLI and it's functionality {suffix}:"))
            for change in self.tool:
                change.print()

        if changes_resources:
            print(Markdown(f"# Found {len(changes_resources)} changes to resources {suffix}:"))
            for resource, changes in changes_resources.items():
                print(Markdown(f"# Resource: {resource}"))
                for change in changes:
                    change.print()

        if changes_cognite_modules and print_cognite_module_changes:
            print(Markdown(f"# Found {len(changes_cognite_modules)} changes to cognite modules {suffix}:"))

            for module, changes in changes_cognite_modules.items():
                print(Markdown(f"# Module: {module}"))
                for change in changes:
                    change.print()


class MigrationYAML(UserList[VersionChanges]):
    filename: str = "_migration.yaml"

    def __init__(self, collection: Collection[VersionChanges] | None = None) -> None:
        super().__init__(collection or [])

    @classmethod
    def load(cls) -> MigrationYAML:
        filepath = Path(__file__).parent.parent / cls.filename
        loaded = load_yaml_inject_variables(filepath, {"VERSION": current_version})
        return cls([VersionChanges.load(cast(dict[str, Any], version_changes)) for version_changes in loaded])

    @classmethod
    def load_from_version(cls, previous_version: str) -> VersionChanges:
        migration_yaml = cls.load()

        return migration_yaml.slice_from(previous_version).as_one_change()

    def slice_from(self, previous_version: str | Version) -> MigrationYAML:
        previous_version_parsed = (
            parse_version(previous_version) if isinstance(previous_version, str) else previous_version
        )
        from_version = max(
            (no + 1 for no, entry in enumerate(self) if entry.version >= previous_version_parsed), default=0
        )
        from_version = min(from_version, len(self) - 1)
        if from_version is None:
            raise ToolkitMigrationError(f"Failed to find migration from version '{previous_version}'.")

        return self[:from_version]

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
            version=self[-1].version,
            tool=tool,
            resources=resources,
            cognite_modules=cognite_modules,
            cognite_modules_hash=self[-1].cognite_modules_hash,
            from_version=self[0].version,
        )


if __name__ == "__main__":
    # This is a simple convinced to print the content of ../_migration.yaml
    # to the console. It is not used in the toolkit itself, but can be used
    # in the development of the toolkit.
    m = MigrationYAML.load_from_version("0.2.b3")
    m.print(Path(__file__).parent.parent, "0.3.b3")

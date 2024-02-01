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
from cognite_toolkit.cdf_tk.utils import load_yaml_inject_variables, read_yaml_file


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
                key: [Change.load(change) for change in changes] for key, changes in data["cognite_modules"].items()
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


def print_help(project_dir: Path) -> None:
    cognite_modules = project_dir / "cognite_modules"
    if (cognite_modules / "_system.yaml").exists():
        system_yaml = read_yaml_file(cognite_modules / "_system.yaml")
        try:
            previous_version = system_yaml["cdf_toolkit_version"]
        except KeyError:
            previous_version = None
    elif (project_dir / "environments.yaml").exists():
        environments_yaml = read_yaml_file(project_dir / "environments.yaml")
        try:
            previous_version = environments_yaml["__system__"]["cdf_toolkit_version"]
        except KeyError:
            previous_version = None
    else:
        previous_version = None

    if previous_version is None:
        print(
            "Failed to load previous version, have you changed the "
            "'_system.yaml' or 'environments.yaml' (before 0.1.0b6) file?"
        )
        exit(1)

    if previous_version == current_version:
        print("No changes to the toolkit detected.")
        exit(0)

    _print_difference(project_dir, previous_version)


def _print_difference(project_dir: Path, previous_version: str) -> None:
    migration_yaml = MigrationYAML.load()

    from_version = next((no for no, entry in enumerate(migration_yaml) if entry.version == previous_version), None)
    if from_version is None:
        print(f"Failed to find migration from version '{previous_version}'.")
        exit(1)

    migrations = migration_yaml[:from_version]

    all_changes = migrations.as_one_change()

    print(
        Markdown(
            f"# Found {len(all_changes.tool)} changes to the 'cdf-tk' from version '{previous_version}' to '{current_version}':"
        )
    )
    for change in all_changes.tool:
        change.print()

    # Todo filter out only resources that are found in the user's project

    print(
        Markdown(
            f"# Found {len(all_changes.resources)} changes to resources from version '{previous_version}' to '{current_version}':"
        )
    )
    for resource, changes in all_changes.resources.items():
        print(Markdown(f"# Resource: {resource}"))
        for change in changes:
            change.print()

    # Todo filter out only modules that are found in the user's project

    print(
        Markdown(
            f"# Found {len(all_changes.cognite_modules)} changes to cognite modules from version '{previous_version}' to '{current_version}':"
        )
    )

    for module, changes in all_changes.cognite_modules.items():
        print(Markdown(f"# Module: {module}"))
        for change in changes:
            change.print()


if __name__ == "__main__":
    _print_difference(Path(__file__).parent.parent, "0.1.0b1")

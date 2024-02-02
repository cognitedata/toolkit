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
from cognite_toolkit.cdf_tk.templates import COGNITE_MODULES
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
        filepath = Path(__file__).parent.parent / cls.filename
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

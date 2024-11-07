from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


@dataclass(frozen=True)
class ExampleData:
    repo_type: str
    repo: str
    source: str
    destination: Path

    @classmethod
    def load(cls, data: dict[str, Any]) -> ExampleData:
        return cls(
            repo_type=data["repoType"],
            repo=data["repo"],
            source=data["source"],
            destination=data["destination"],
        )


@dataclass(frozen=True)
class ModuleToml:
    filename: ClassVar[str] = "module.toml"
    title: str | None
    tags: frozenset[str] = field(default_factory=frozenset)
    dependencies: frozenset[str] = field(default_factory=frozenset)
    is_selected_by_default: bool = False
    data: list[ExampleData] = field(default_factory=list)

    @classmethod
    def load(cls, data: dict[str, Any] | Path) -> ModuleToml:
        if isinstance(data, Path):
            return cls.load(toml.loads(data.read_text()))

        if "dependencies" in data:
            dependencies = frozenset(data["dependencies"].get("modules", set()))
        else:
            dependencies = frozenset()

        example_data: list[ExampleData] = []
        if "data" in data and isinstance(data["data"], list):
            example_data = [ExampleData.load(d) for d in data["data"]]

        return cls(
            title=data["module"].get("title"),
            tags=frozenset(data["packages"].get("tags", set())),
            dependencies=dependencies,
            is_selected_by_default=data["module"].get("is_selected_by_default", False),
            data=example_data,
        )

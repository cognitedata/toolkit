from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError

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
    extra_resources: list[Path] = field(default_factory=list)

    def __post_init__(self) -> None:
        for extra in self.extra_resources:
            if extra.is_absolute():
                raise ToolkitFileExistsError(f"Extra resource {extra} must be a relative path")

    @classmethod
    def load(cls, data: dict[str, Any] | Path) -> ModuleToml:
        if isinstance(data, Path):
            return cls.load(toml.loads(data.read_text(encoding="utf-8")))

        if "dependencies" in data:
            dependencies = frozenset(data["dependencies"].get("modules", set()))
        else:
            dependencies = frozenset()

        example_data: list[ExampleData] = []
        if "data" in data and isinstance(data["data"], list):
            example_data = [ExampleData.load(d) for d in data["data"]]

        extra_resources: list[Path] = []
        if "extra_resources" in data and isinstance(data["extra_resources"], list):
            extra_resources = [Path(item["location"]) for item in data["extra_resources"] if "location" in item]

        tags: frozenset[str] = frozenset()
        if "packages" in data:
            if "tags" in data["packages"]:
                tags = frozenset(data["packages"]["tags"])
        title: str | None = None
        is_selected_by_default: bool = False
        if "module" in data:
            title = data["module"].get("title")
            is_selected_by_default = data["module"].get("is_selected_by_default", False)

        return cls(
            title=title,
            tags=tags,
            dependencies=dependencies,
            is_selected_by_default=is_selected_by_default,
            data=example_data,
            extra_resources=extra_resources,
        )

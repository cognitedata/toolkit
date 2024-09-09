from __future__ import annotations

import sys
from collections.abc import Iterable, MutableSequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, overload

from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleDirectories
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


@dataclass
class SelectableModule:
    definition: ModuleToml
    path: Path

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def title(self) -> str | None:
        return self.definition.description

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SelectableModule):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return self.name


@dataclass
class Package:
    """A package represents a bundle of modules.
    Args:
        name: the unique identifier of the package.
        title: The display name of the package.
        description: A description of the package.
        modules: The modules that are part of the package.
    """

    name: str
    title: str
    description: str | None = None
    modules: list[SelectableModule] = field(default_factory=list)

    @classmethod
    def load(cls, name: str, package_definition: dict) -> Package:
        return cls(
            name=name,
            title=package_definition["title"],
            description=package_definition.get("description"),
        )


@dataclass
class Packages(list, MutableSequence[Package]):
    @overload
    def __init__(self, packages: Iterable[Package]) -> None: ...

    @overload
    def __init__(self) -> None: ...

    def __init__(self, packages: Optional[Iterable[Package]] = None) -> None:
        super().__init__(packages or [])

    def get_by_name(self, name: str) -> Package:
        for package in self:
            if package.name == name:
                return package
        raise KeyError(f"Package {name} not found")

    @classmethod
    def load(
        cls,
        root_module_dir: Path,  # todo: relative to org dir
    ) -> Packages:
        """Loads the packages in the source directory.

        Args:
            root_module_dir: The module directories to load the packages from.
        """

        package_definition_path = root_module_dir / "package.toml"
        if not package_definition_path.exists():
            raise ToolkitFileNotFoundError(f"Package manifest toml not found at {package_definition_path}")
        package_definitions = toml.loads(package_definition_path.read_text())["packages"]

        collected: dict[str, Package] = {}
        for package_name, package_definition in package_definitions.items():
            if isinstance(package_definition, dict):
                collected[package_name] = Package.load(package_name, package_definition)

        module_directories = ModuleDirectories.load(root_module_dir, set())
        selectable_modules = list(
            {m for m in (cls.get_module(module.dir, root_module_dir) for module in module_directories) if m is not None}
        )
        for selectable_module in selectable_modules:
            if selectable_module is None:
                continue
            for tag in selectable_module.definition.tags or []:
                if tag in collected:
                    collected[tag].modules.append(selectable_module)
                else:
                    raise ValueError(f"Tag {tag} not found in package manifest toml")

        return cls(list(collected.values()))

    @classmethod
    def get_module(cls, module_dir: Path, root: Path) -> SelectableModule | None:
        dir = module_dir
        while dir != root:
            module_toml_file = dir / "module.toml"
            if module_toml_file.exists():
                definition = ModuleToml.load(module_toml_file)
                return SelectableModule(definition, dir)
            dir = dir.parent
        return None

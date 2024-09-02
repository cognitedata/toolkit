from __future__ import annotations

import sys
from collections.abc import Iterable, MutableSequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, overload

from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleDirectories

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


@dataclass
class Package:
    """A package represents a bundle of modules.
    Args:
        name: the unique identifier of the package.
    """

    name: str
    title: str
    description: str | None = None
    _modules: dict[str, str | None] = field(default_factory=dict)

    @property
    def modules(self) -> dict[str, str | None]:
        return self._modules or {}

    @classmethod
    def load(cls, name: str, package_definition: dict) -> Package:
        return cls(
            name=name,
            title=package_definition["title"],
            description=package_definition.get("description"),
        )

    def add_module(self, name: str, description: str | None = None) -> None:
        self._modules[name] = description


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
        path: Path,  # todo: relative to org dir
    ) -> Packages:
        """Loads the packages in the source directory.

        Args:
            modules: The module directories to load the packages from.
        """

        package_definition_path = path / "package.toml"
        if not package_definition_path.exists():
            raise FileNotFoundError(f"Package manifest toml not found at {package_definition_path}")
        package_definitions = toml.loads(package_definition_path.read_text())["packages"]

        collected: dict[str, Package] = {}
        for package_name, package_definition in package_definitions.items():
            if isinstance(package_definition, dict):
                collected[package_name] = Package.load(package_name, package_definition)

        module_directories = ModuleDirectories.load(path, set())
        for module in module_directories:
            if module.module_toml:
                for tag in module.module_toml.tags or []:
                    if tag in collected:
                        collected[tag].add_module(module.name, module.module_toml.description)
                    else:
                        raise ValueError(f"Tag {tag} not found in package manifest toml")

        return cls(list(collected.values()))

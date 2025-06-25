from __future__ import annotations

import sys
from collections.abc import ItemsView, Iterable, Iterator, KeysView, Mapping, MutableMapping, ValuesView
from dataclasses import dataclass, field
from pathlib import Path

from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitValueError

from ._module_directories import ModuleDirectories, ModuleLocation

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


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
    can_cherry_pick: bool = True
    modules: list[ModuleLocation] = field(default_factory=list)

    @property
    def module_names(self) -> set[str]:
        """The names of the modules in the package."""
        return {module.name for module in self.modules}

    @classmethod
    def load(cls, name: str, package_definition: dict) -> Package:
        return cls(
            name=name,
            title=package_definition["title"],
            description=package_definition.get("description"),
            can_cherry_pick=package_definition.get("canCherryPick", True),
        )


class Packages(dict, MutableMapping[str, Package]):
    def __init__(self, packages: Iterable[Package] | Mapping[str, Package] | None = None) -> None:
        if packages is None:
            super().__init__()
        elif isinstance(packages, Mapping):
            super().__init__(packages)
        else:
            super().__init__({p.name: p for p in packages})

    @classmethod
    def load(
        cls,
        root_module_dir: Path,  # todo: relative to org dir
    ) -> Packages:
        """Loads the packages in the source directory.

        Args:
            root_module_dir: The module directories to load the packages from.
        """

        package_definition_path = root_module_dir / "packages.toml"
        if not package_definition_path.exists():
            raise ToolkitFileNotFoundError(f"Package manifest not found at {package_definition_path}")

        library_definition = toml.loads(package_definition_path.read_text(encoding="utf-8"))
        package_definitions = library_definition.get("packages", {})

        module_directory_by_name = {
            module_location.name: module_location for module_location in ModuleDirectories.load(root_module_dir)
        }

        packages_with_modules: dict[str, Package] = {}

        for package_name, package_definition in package_definitions.items():
            packages_with_modules[package_name] = Package.load(package_name, package_definition)
            if (
                "modules" not in package_definition
                or not isinstance(package_definition["modules"], list)
                or len(package_definition["modules"]) == 0
            ):
                continue

            for module_name in package_definition["modules"]:
                if module_name not in module_directory_by_name:
                    raise ToolkitValueError(f"Module {module_name} not found in the module directories.")
                packages_with_modules[package_name].modules.append(module_directory_by_name[module_name])

        return cls(packages_with_modules)

    # The methods are overloads to provide type hints for the methods.
    def items(self) -> ItemsView[str, Package]:  # type: ignore[override]
        return super().items()

    def keys(self) -> KeysView[str]:  # type: ignore[override]
        return super().keys()

    def values(self) -> ValuesView[Package]:  # type: ignore[override]
        return super().values()

    def __iter__(self) -> Iterator[str]:
        yield from super().__iter__()

    def __getitem__(self, package_name: str) -> Package:
        return super().__getitem__(package_name)

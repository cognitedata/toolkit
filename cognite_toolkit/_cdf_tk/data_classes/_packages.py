import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, cast

from cognite_toolkit._cdf_tk.commands.build_v2._module_parser import ModuleParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ModuleDirectory
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.tk_warnings.base import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.tk_warnings.other import LowSeverityWarning

if sys.version_info >= (3, 11):
    from typing import Self

    import toml
else:
    import tomli as toml
    from typing_extensions import Self
if TYPE_CHECKING:
    pass


@dataclass
class Package:
    """A package represents a bundle of modules.
    Args:
        name: the unique identifier of the package.
        title: The display name of the package.
        description: A description of the package.
        id: An optional identifier for package statistics.
        modules: The modules that are part of the package.
    """

    name: str
    title: str
    description: str | None = None
    id: str | None = None
    can_cherry_pick: bool = True
    modules: list[ModuleDirectory] = field(default_factory=list)

    @property
    def module_names(self) -> set[str]:
        """The names of the modules in the package."""
        return {module.name for module in self.modules}

    @classmethod
    def load(cls, name: str, package_definition: dict) -> Self:
        return cls(
            name=name,
            title=package_definition["title"],
            description=package_definition.get("description"),
            id=package_definition.get("id"),
            can_cherry_pick=package_definition.get("canCherryPick", True),
        )


class Packages(dict[str, Package]):
    warnings: WarningList[ToolkitWarning] | None = None

    def __init__(
        self,
        packages: Iterable[Package] | Mapping[str, Package] | None = None,
        warnings: WarningList[ToolkitWarning] | None = None,
    ) -> None:
        if packages is None:
            super().__init__()
        elif isinstance(packages, Mapping):
            by_name = cast(Mapping[str, Package], packages)
            super().__init__({name: package for name, package in by_name.items()})
        else:
            super().__init__({package.name: package for package in packages})

        if warnings:
            self.warnings = warnings

    @classmethod
    def load(
        cls,
        root_module_dir: Path,
    ) -> Self:
        """Loads the packages in the source directory.

        Args:
            root_module_dir: The module directories to load the packages from.
        """

        package_definition_path = next(root_module_dir.rglob("packages.toml"), None)
        if not package_definition_path or not package_definition_path.exists():
            raise ToolkitFileNotFoundError(f"Package manifest toml not found at {package_definition_path}")

        library_definition = toml.loads(package_definition_path.read_text(encoding="utf-8"))
        package_definitions = library_definition.get("packages", {})

        module_by_relative_path, _ = ModuleParser.find_modules(root_module_dir)

        packages_with_modules: dict[str, Package] = {}

        warnings = WarningList[ToolkitWarning]()

        for package_name, package_definition in package_definitions.items():
            packages_with_modules[package_name] = Package.load(package_name, package_definition)
            if modules := package_definition.get("modules"):
                if isinstance(modules, list) and modules:
                    for module_path in modules:
                        if (module_or_none := module_by_relative_path.get(Path(module_path))) is None:
                            warnings.append(
                                LowSeverityWarning(
                                    f"Unable to load module '{module_path}'. The path may be wrong or the module may require an alpha flag that is not set."
                                )
                            )
                            continue
                        packages_with_modules[package_name].modules.append(module_or_none)

        return cls(packages_with_modules, warnings)

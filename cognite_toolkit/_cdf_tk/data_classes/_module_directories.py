from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from cognite_toolkit._cdf_tk.exceptions import ToolkitDuplicatedModuleError, ToolkitMissingModuleError
from cognite_toolkit._cdf_tk.utils import iterate_modules

from ._config_yaml import Environment
from ._system_yaml import SystemYAML


@dataclass(frozen=True)
class ModuleLocation:
    """This represents the location of a module in a directory structure."""

    relative_path: Path
    source_absolute_path: Path

    @property
    def path(self) -> Path:
        return self.source_absolute_path / self.relative_path

    @property
    def module_name(self) -> str:
        return self.relative_path.name

    @property
    def module_references(self) -> Iterable[str | tuple[str, ...]]:
        """Ways of selecting this module."""
        yield self.module_name
        module_parts = self.relative_path.parts
        for i in range(1, len(module_parts) + 1):
            yield module_parts[:i]


@dataclass
class ModuleDirectories(tuple, Sequence[ModuleLocation]):
    # Subclassing tuple to make the class immutable. ModuleDirectories is expected to be initialized and
    # then used as a read-only object.
    def __new__(cls, collection: Collection[ModuleLocation]) -> ModuleDirectories:
        # Need to override __new__ to as we are subclassing a tuple:
        #   https://stackoverflow.com/questions/1565374/subclassing-tuple-with-multiple-init-arguments
        return super().__new__(cls, tuple(collection))

    def __init__(self, collection: Collection[ModuleLocation]) -> None: ...

    @cached_property
    def available(self) -> set[str | tuple[str, ...]]:
        return {ref for module_location in self for ref in module_location.module_references}

    @classmethod
    def load(
        cls, source_dir: Path, environment: Environment, packages: dict[str, list[str | tuple[str, ...]]]
    ) -> ModuleDirectories:
        """Loads and validates the modules in the source directory."""
        module_parts_by_name: dict[str, list[tuple[str, ...]]] = defaultdict(list)
        module_locations: list[ModuleLocation] = []
        for module, _ in iterate_modules(source_dir):
            module_locations.append(ModuleLocation(module.relative_to(source_dir), source_dir))
            module_parts_by_name[module.name].append(module.relative_to(source_dir).parts)

        cls._check_ambiguous_modules(environment, module_parts_by_name)

        modules = cls(module_locations)

        cls._check_package_modules_exists(modules.available, packages, set(environment.selected))

        return modules

    @classmethod
    def _check_ambiguous_modules(
        cls, environment: Environment, module_parts_by_name: dict[str, list[tuple[str, ...]]]
    ) -> None:
        """
        If the user has selected a module by name, and there are multiple modules with that name, raise an error.
        Note, if the user uses a path to select a module, this error will not be raised.
        """
        selected_names = {s for s in environment.selected if isinstance(s, str)}
        if duplicate_modules := {
            module_name: paths
            for module_name, paths in module_parts_by_name.items()
            if len(paths) > 1 and module_name in selected_names
        }:
            raise ToolkitDuplicatedModuleError(
                f"Ambiguous module selected in config.{environment.name}.yaml:", duplicate_modules
            )

    @classmethod
    def _check_package_modules_exists(
        cls,
        available_modules: set[str | tuple[str, ...]],
        packages: dict[str, list[str | tuple[str, ...]]],
        selected: set[str | tuple[str, ...]],
    ) -> None:
        selected_packages = {package for package in selected if isinstance(package, str) and package in packages}
        for package, modules in packages.items():
            if package not in selected_packages:
                # We do not check packages that are not selected.
                # Typically, the user will delete the modules that are irrelevant for them;
                # thus we only check the selected packages.
                continue
            if missing := set(modules) - available_modules:
                ToolkitMissingModuleError(
                    f"Package {package} defined in {SystemYAML.file_name!s} is referring "
                    f"the following missing modules {missing}."
                )

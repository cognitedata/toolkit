from __future__ import annotations

from collections.abc import Iterator, MutableSequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Generic, SupportsIndex, overload

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID
from cognite_toolkit._cdf_tk.utils import tmp_build_directory

from ._base import ConfigCore
from ._config_yaml import BuildConfigYAML
from ._module_directories import ModuleDirectories


@dataclass
class Location:
    path: Path
    hash: str


@dataclass
class ResourceInfo(Generic[T_ID]):
    identifier: T_ID
    location: Location
    kind: str


class ResourceList(list, MutableSequence[ResourceInfo]): ...


@dataclass
class ModuleBuildInfo:
    name: str
    location: Location
    build_variables: dict[str, str | int | float | bool | None]
    resources: dict[str, ResourceList]


@dataclass
class ModuleList(list, MutableSequence[ModuleBuildInfo]):
    # Implemented to get correct type hints
    def __iter__(self) -> Iterator[ModuleBuildInfo]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> ModuleBuildInfo: ...

    @overload
    def __getitem__(self, index: slice) -> ModuleList: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> ModuleBuildInfo | ModuleList:
        if isinstance(index, slice):
            return ModuleList(super().__getitem__(index))
        return super().__getitem__(index)


@dataclass
class ModulesInfo:
    version: str
    modules: ModuleList

    @classmethod
    def load(cls, data: dict[str, Any]) -> ModulesInfo:
        raise NotImplementedError


@dataclass
class BuildInfo(ConfigCore):
    filename: ClassVar[str] = "build_info.{build_env}.yaml"
    modules: ModulesInfo

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> BuildInfo:
        return cls(filepath, ModulesInfo.load(data))

    @classmethod
    def rebuild(cls, project_dir: Path, build_env: str, needs_rebuild: set[Path] | None = None) -> BuildInfo:
        # To avoid circular imports
        from cognite_toolkit._cdf_tk.commands.build import BuildCommand

        if needs_rebuild is None:
            raise NotImplementedError()

        with tmp_build_directory() as build_dir:
            cdf_toml = CDFToml.load()
            config = BuildConfigYAML.load_from_directory(project_dir, build_env)
            config.set_environment_variables()
            # Todo Remove once the new modules in `_cdf_tk/prototypes/_packages` are finished.
            config.variables.pop("_cdf_tk", None)
            if needs_rebuild is None:
                # Use path syntax to select all modules in the source directory
                config.environment.selected = [Path("")]
            else:
                # Use path syntax to select only the modules that need to be rebuilt
                config.environment.selected = list(needs_rebuild)
            source_by_build_path = BuildCommand().build_config(
                build_dir=build_dir,
                source_dir=project_dir,
                config=config,
                packages=cdf_toml.modules.packages,
                clean=True,
                verbose=False,
            )
        # Need to reuse the build_info.{}.yaml if needs_rebuild is not none.
        # Also remember to dump the build_info.{}.yaml to the project_dir when rebuild is done.
        return cls._from_build(source_by_build_path, build_env)

    @classmethod
    def _from_build(cls, source_by_build_path: dict[Path, Path], build_env: str) -> BuildInfo:
        raise NotImplementedError()

    def compare_modules(self, current_modules: ModuleDirectories) -> set[Path]:
        raise NotImplementedError()


class ModuleResources:
    """This class is used to retrieve resource information from the build info.

    It is responsible for ensuring that the build info is up-to-date with the
    latest changes in the source directory.
    """

    def __init__(self, project_dir: Path, build_env: str) -> None:
        self._project_dir = project_dir
        self._build_env = build_env
        self._build_info: BuildInfo
        try:
            self._build_info = BuildInfo.load_from_directory(project_dir, build_env)
        except FileNotFoundError:
            self._build_info = BuildInfo.rebuild(project_dir, build_env)

    def list(self) -> ModuleList:
        current_modules = ModuleDirectories.load(self._project_dir, set())
        # Check if the build info is up-to-date
        if needs_rebuild := self._build_info.compare_modules(current_modules):
            self._build_info = BuildInfo.rebuild(self._project_dir, self._build_env, needs_rebuild)
        return self._build_info.modules.modules

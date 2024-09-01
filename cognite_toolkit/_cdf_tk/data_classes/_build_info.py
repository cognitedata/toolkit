from __future__ import annotations

from abc import abstractmethod
from collections.abc import Collection, Iterator, MutableSequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar, Generic, SupportsIndex, overload

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, calculate_str_or_file_hash, tmp_build_directory

from ._base import ConfigCore
from ._build_variables import BuildVariables
from ._config_yaml import BuildConfigYAML
from ._module_directories import ModuleDirectories


@dataclass
class BuildLocation:
    """This represents the location of a build resource in a directory structure.

    Args:
        path: The relative path to the resource from the project directory.
    """

    path: Path

    @property
    @abstractmethod
    def hash(self) -> str:
        """The hash of the resource file."""
        raise NotImplementedError()


@dataclass
class BuildLocationLazy(BuildLocation):
    absolute_path: Path

    @cached_property
    def hash(self) -> str:
        if self.absolute_path.is_dir():
            return calculate_directory_hash(self.absolute_path, shorten=True)
        else:
            return calculate_str_or_file_hash(self.absolute_path, shorten=True)


@dataclass
class BuildLocationEager(BuildLocation):
    _hash: str

    @property
    def hash(self) -> str:
        return self._hash


@dataclass
class ResourceBuildInfo(Generic[T_ID]):
    identifier: T_ID
    location: BuildLocation
    kind: str


class ResourceBuildList(list, MutableSequence[ResourceBuildInfo]): ...


@dataclass
class ModuleBuildInfo:
    name: str
    location: BuildLocation
    build_variables: BuildVariables
    resources: dict[str, ResourceBuildList]


@dataclass
class ModuleBuildList(list, MutableSequence[ModuleBuildInfo]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[ModuleBuildInfo] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[ModuleBuildInfo]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> ModuleBuildInfo: ...

    @overload
    def __getitem__(self, index: slice) -> ModuleBuildList: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> ModuleBuildInfo | ModuleBuildList:
        if isinstance(index, slice):
            return ModuleBuildList(super().__getitem__(index))
        return super().__getitem__(index)


@dataclass
class ModulesInfo:
    version: str
    modules: ModuleBuildList

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
        # Ideally, this class should be in a separate module
        from cognite_toolkit._cdf_tk.commands.build import BuildCommand

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
            build, _ = BuildCommand().build_config(
                build_dir=build_dir,
                source_dir=project_dir,
                config=config,
                packages=cdf_toml.modules.packages,
                clean=True,
                verbose=False,
            )

        new_build = cls(
            filepath=project_dir / cls.get_filename(build_env),
            modules=ModulesInfo(version=_version.__version__, modules=build),
        )
        if needs_rebuild is not None and (existing := cls._get_existing(project_dir, build_env)):
            # Merge the existing modules with the new modules
            new_modules_by_path = {module.location.path: module for module in new_build.modules.modules}
            module_list = ModuleBuildList(
                [
                    new_modules_by_path[existing_module.location.path]
                    if existing_module.location.path in new_modules_by_path
                    else existing_module
                    for existing_module in existing.modules.modules
                ]
            )
            new_build.modules.modules = module_list

        new_build.dump_to_file()
        return new_build

    @classmethod
    def _get_existing(cls, project_dir: Path, build_env: str) -> BuildInfo | None:
        try:
            existing = cls.load_from_directory(project_dir, build_env)
        except FileNotFoundError:
            return None
        if existing.modules.modules != _version.__version__:
            return None
        return existing

    def dump_to_file(self) -> None: ...

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

    def list(self) -> ModuleBuildList:
        current_modules = ModuleDirectories.load(self._project_dir, set())
        # Check if the build info is up-to-date
        if needs_rebuild := self._build_info.compare_modules(current_modules):
            self._build_info = BuildInfo.rebuild(self._project_dir, self._build_env, needs_rebuild)
        return self._build_info.modules.modules

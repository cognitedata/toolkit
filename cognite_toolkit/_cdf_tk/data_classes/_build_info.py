from __future__ import annotations

from abc import abstractmethod
from collections.abc import Collection, Iterator, MutableSequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar, Generic, SupportsIndex, cast, overload

import yaml
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import DEFAULT_ENV
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.loaders import ResourceTypes, get_loader
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, ResourceLoader, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils import (
    calculate_directory_hash,
    calculate_str_or_file_hash,
    load_yaml_inject_variables,
    safe_read,
    safe_write,
    tmp_build_directory,
)

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

    def dump(self) -> dict[str, Any]:
        return {
            "path": self.path.as_posix(),
            "hash": self.hash,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> BuildLocation:
        return BuildLocationEager(
            path=Path(data["path"]),
            _hash=data["hash"],
        )


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

    @classmethod
    def load(cls, data: dict[str, Any], resource_folder: str) -> ResourceBuildInfo:
        from cognite_toolkit._cdf_tk.loaders import ResourceLoader, get_loader

        kind = data["kind"]
        loader = cast(ResourceLoader, get_loader(resource_folder, kind))
        identifier = loader.get_id(data["identifier"])

        return cls(
            location=BuildLocation.load(data["location"]),
            kind=kind,
            identifier=identifier,
        )

    def dump(self, resource_folder: str) -> dict[str, Any]:
        from cognite_toolkit._cdf_tk.loaders import ResourceLoader, get_loader

        loader = cast(ResourceLoader, get_loader(resource_folder, self.kind))
        dumped = loader.dump_id(self.identifier)

        return {
            "identifier": dumped,
            "location": self.location.dump(),
            "kind": self.kind,
        }

    def create_full(self, module: BuiltModule, resource_dir: str) -> ResourceBuildInfoFull[T_ID]:
        return ResourceBuildInfoFull(
            identifier=self.identifier,
            location=self.location,
            kind=self.kind,
            build_variables=module.build_variables,
            module_name=module.name,
            module_location=module.location.path,
            resource_dir=resource_dir,
        )


@dataclass
class ResourceBuildInfoFull(ResourceBuildInfo[T_ID]):
    build_variables: BuildVariables
    module_name: str
    module_location: Path
    resource_dir: str

    def load_resource_dict(
        self, environment_variables: dict[str, str | None], validate: bool = False
    ) -> dict[str, Any]:
        content = self.build_variables.replace(safe_read(self.location.path))
        loader = cast(ResourceLoader, get_loader(self.resource_dir, self.kind))
        raw = load_yaml_inject_variables(content, environment_variables, validate=validate)
        if isinstance(raw, dict):
            return raw
        elif isinstance(raw, list):
            for item in raw:
                if loader.get_id(item) == self.identifier:
                    return item
        raise ToolkitMissingResourceError(f"Resource {self.identifier} not found in {self.location.path}")

    def load_resource(
        self,
        environment_variables: dict[str, str | None],
        loader: type[
            ResourceLoader[
                T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
            ]
        ]
        | None = None,
    ) -> T_WriteClass:
        """Load the resource from the build info.

        Args:
            environment_variables: The environment variables to inject into the resource file.
            loader: The loader to use to load the resource. If not provided, the loader will be inferred from the resource directory and kind.
                The motivation to explicitly provide the loader is to get the correct type hints.
        """
        loader = loader or get_loader(self.resource_dir, self.kind)  # type: ignore[assignment]
        return loader.resource_write_cls.load(self.load_resource_dict(environment_variables))  # type: ignore[misc, union-attr]


class ResourceBuiltList(list, MutableSequence[ResourceBuildInfo[T_ID]], Generic[T_ID]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[ResourceBuildInfo[T_ID]] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[ResourceBuildInfo[T_ID]]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> ResourceBuildInfo[T_ID]: ...

    @overload
    def __getitem__(self, index: slice) -> ResourceBuiltList[T_ID]: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> ResourceBuildInfo[T_ID] | ResourceBuiltList[T_ID]:
        if isinstance(index, slice):
            return ResourceBuiltList[T_ID](super().__getitem__(index))
        return super().__getitem__(index)

    @property
    def identifiers(self) -> list[T_ID]:
        return [resource.identifier for resource in self]


class ResourceBuiltFullList(ResourceBuiltList[T_ID]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[ResourceBuildInfoFull[T_ID]] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[ResourceBuildInfoFull[T_ID]]:
        return cast(Iterator[ResourceBuildInfoFull[T_ID]], super().__iter__())

    @overload
    def __getitem__(self, index: SupportsIndex) -> ResourceBuildInfoFull[T_ID]: ...

    @overload
    def __getitem__(self, index: slice) -> ResourceBuiltFullList[T_ID]: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> ResourceBuildInfoFull[T_ID] | ResourceBuiltFullList[T_ID]:
        if isinstance(index, slice):
            return ResourceBuiltFullList[T_ID](super().__getitem__(index))
        return cast(ResourceBuildInfoFull[T_ID], super().__getitem__(index))


@dataclass
class BuiltModule:
    name: str
    location: BuildLocation
    build_variables: BuildVariables
    resources: dict[str, ResourceBuiltList]
    warning_count: int
    status: str

    @classmethod
    def load(cls, data: dict[str, Any]) -> BuiltModule:
        return cls(
            name=data["name"],
            location=BuildLocation.load(data["location"]),
            build_variables=BuildVariables.load(data["build_variables"]),
            resources={
                key: ResourceBuiltList([ResourceBuildInfo.load(resource_data, key) for resource_data in resources_data])
                for key, resources_data in data["resources"].items()
            },
            warning_count=data.get("warning_count", 0),
            status=data.get("status", "Success"),
        )

    def dump(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "location": self.location.dump(),
            "build_variables": self.build_variables.dump(),
            "resources": {
                key: [resource.dump(key) for resource in resources] for key, resources in self.resources.items()
            },
            "warning_count": self.warning_count,
            "status": self.status,
        }


@dataclass
class BuiltModuleList(list, MutableSequence[BuiltModule]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[BuiltModule] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[BuiltModule]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> BuiltModule: ...

    @overload
    def __getitem__(self, index: slice) -> BuiltModuleList: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> BuiltModule | BuiltModuleList:
        if isinstance(index, slice):
            return BuiltModuleList(super().__getitem__(index))
        return super().__getitem__(index)

    def get_resources(self, id_type: type[T_ID], resource_dir: ResourceTypes, kind: str) -> ResourceBuiltFullList[T_ID]:
        return ResourceBuiltFullList[T_ID](
            [
                resource.create_full(module, resource_dir)
                for module in self
                for resource in module.resources.get(resource_dir, [])
                if resource.kind == kind
            ]
        )


@dataclass
class ModulesInfo:
    version: str
    modules: BuiltModuleList

    @classmethod
    def load(cls, data: dict[str, Any]) -> ModulesInfo:
        return cls(
            version=data["version"],
            modules=BuiltModuleList([BuiltModule.load(module_data) for module_data in data["modules"]]),
        )

    def dump(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "modules": [module.dump() for module in self.modules],
        }


@dataclass
class BuildInfo(ConfigCore):
    filename: ClassVar[str] = "build_info.{build_env}.yaml"
    top_warning: ClassVar[str] = "# DO NOT MODIFY THIS FILE MANUALLY. IT IS AUTO-GENERATED BY THE COGNITE TOOLKIT."
    modules: ModulesInfo

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> BuildInfo:
        return cls(filepath, ModulesInfo.load(data["modules"]))

    @classmethod
    def rebuild(
        cls, organization_dir: Path, build_env: str | None, needs_rebuild: set[Path] | None = None
    ) -> BuildInfo:
        # To avoid circular imports
        # Ideally, this class should be in a separate module
        from cognite_toolkit._cdf_tk.commands.build import BuildCommand

        with tmp_build_directory() as build_dir:
            cdf_toml = CDFToml.load()
            if build_env is None:
                config = BuildConfigYAML.load_default(organization_dir)
            else:
                config = BuildConfigYAML.load_from_directory(organization_dir, build_env)
            config.set_environment_variables()
            # Todo Remove once the new modules in `_cdf_tk/prototypes/_packages` are finished.
            config.variables.pop("_cdf_tk", None)
            if needs_rebuild is None:
                # Use path syntax to select all modules in the source directory
                config.environment.selected = [Path("")]
            else:
                # Use path syntax to select only the modules that need to be rebuilt
                config.environment.selected = list(needs_rebuild)
            build, _ = BuildCommand(silent=True, skip_tracking=True).build_config(
                build_dir=build_dir,
                organization_dir=organization_dir,
                config=config,
                packages=cdf_toml.modules.packages,
                clean=True,
                verbose=False,
                progress_bar=True,
            )

        new_build = cls(
            filepath=organization_dir / cls.get_filename(build_env or DEFAULT_ENV),
            modules=ModulesInfo(version=_version.__version__, modules=build),
        )
        if needs_rebuild is not None and (existing := cls._get_existing(organization_dir, build_env or DEFAULT_ENV)):
            # Merge the existing modules with the new modules
            new_modules_by_path = {module.location.path: module for module in new_build.modules.modules}
            existing_modules_by_path = {module.location.path: module for module in existing.modules.modules}
            all_module_paths = set(new_modules_by_path) | set(existing_modules_by_path)

            module_list = BuiltModuleList(
                [
                    new_modules_by_path[path] if path in new_modules_by_path else existing_modules_by_path[path]
                    for path in all_module_paths
                ]
            )
            new_build.modules.modules = module_list

        new_build.dump_to_file()
        return new_build

    @classmethod
    def _get_existing(cls, organization_dir: Path, build_env: str) -> BuildInfo | None:
        try:
            existing = cls.load_from_directory(organization_dir, build_env)
        except FileNotFoundError:
            return None
        if existing.modules.version != _version.__version__:
            return None
        return existing

    def dump(self) -> dict[str, Any]:
        return {
            "modules": self.modules.dump(),
        }

    def dump_to_file(self) -> None:
        dumped = self.dump()
        # Avoid dumping pointer references: https://stackoverflow.com/questions/51272814/python-yaml-dumping-pointer-references
        yaml.Dumper.ignore_aliases = lambda *args: True  # type: ignore[method-assign]
        content = yaml.safe_dump(dumped, sort_keys=False)
        content = f"{self.top_warning}\n{content}"
        safe_write(self.filepath, content)

    def compare_modules(
        self,
        current_modules: ModuleDirectories,
        current_variables: BuildVariables,
        resource_dirs: set[str] | None = None,
    ) -> set[Path]:
        current_module_by_path = {module.relative_path: module for module in current_modules}
        cached_module_by_path = {module.location.path: module for module in self.modules.modules}
        needs_rebuild = set()
        for path, current_module in current_module_by_path.items():
            if resource_dirs is not None and all(
                resource_dir not in current_module.resource_directories for resource_dir in resource_dirs
            ):
                # The module does not contain any of the specified resources, so it does not need to be rebuilt.
                continue

            if path not in cached_module_by_path:
                needs_rebuild.add(path)
                continue
            cached_module = cached_module_by_path[path]
            if current_module.hash != cached_module.location.hash:
                needs_rebuild.add(path)
            current_module_variables = current_variables.get_module_variables(current_module)
            if set(current_module_variables) != set(cached_module.build_variables):
                needs_rebuild.add(path)
        return needs_rebuild


class ModuleResources:
    """This class is used to retrieve resource information from the build info.

    It is responsible for ensuring that the build info is up-to-date with the
    latest changes in the source directory.
    """

    def __init__(self, organization_dir: Path, build_env: str | None) -> None:
        self._organization_dir = organization_dir
        self._build_env = build_env
        self._build_info: BuildInfo
        try:
            self._build_info = BuildInfo.load_from_directory(organization_dir, build_env)
            self._has_rebuilt = False
        except FileNotFoundError:
            self._build_info = BuildInfo.rebuild(organization_dir, build_env)
            self._has_rebuilt = True

    @cached_property
    def _current_modules(self) -> ModuleDirectories:
        return ModuleDirectories.load(self._organization_dir, {Path("")})

    @cached_property
    def _current_variables(self) -> BuildVariables:
        config_yaml = BuildConfigYAML.load_from_directory(self._organization_dir, self._build_env)
        return BuildVariables.load_raw(
            config_yaml.variables, self._current_modules.available_paths, self._current_modules.selected.available_paths
        )

    def list_resources(
        self, id_type: type[T_ID], resource_dir: ResourceTypes, kind: str
    ) -> ResourceBuiltFullList[T_ID]:
        if not self._has_rebuilt:
            if needs_rebuild := self._build_info.compare_modules(
                self._current_modules, self._current_variables, {resource_dir}
            ):
                self._build_info = BuildInfo.rebuild(self._organization_dir, self._build_env, needs_rebuild)
        return self._build_info.modules.modules.get_resources(id_type, resource_dir, kind)

    def list(self) -> BuiltModuleList:
        # Check if the build info is up to date
        if not self._has_rebuilt:
            if needs_rebuild := self._build_info.compare_modules(self._current_modules, self._current_variables):
                self._build_info = BuildInfo.rebuild(self._organization_dir, self._build_env, needs_rebuild)
            self._has_rebuilt = True
        return self._build_info.modules.modules

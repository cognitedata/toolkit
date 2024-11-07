from __future__ import annotations

import shutil
from collections import defaultdict
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, SupportsIndex, overload

from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, iterate_modules, resource_folder_from_path

from ._module_toml import ModuleToml


@dataclass(frozen=True)
class ModuleLocation:
    """This represents the location of a module in a directory structure.
    Args:
        dir: The absolute path to the module directory.
        source_absolute_path: The absolute path to the source directory.
        is_selected: Whether the module is selected by the user.
        source_paths: The paths to all files in the module.
    """

    dir: Path
    source_absolute_path: Path
    source_paths: list[Path]
    is_selected: bool = False
    definition: ModuleToml | None = None

    @property
    def has_example_data(self) -> bool:
        return bool(self.definition and self.definition.data)

    @property
    def name(self) -> str:
        """The name of the module."""
        return self.dir.name

    @property
    def title(self) -> str | None:
        """The title of the module."""
        if self.definition:
            return self.definition.title
        return None

    @property
    def relative_path(self) -> Path:
        """The relative path to the module."""
        return self.dir.relative_to(self.source_absolute_path)

    @property
    def module_selections(self) -> set[str | Path]:
        """Ways of selecting this module."""
        return {self.name, self.relative_path, *self.parent_relative_paths}

    @property
    def is_selected_by_default(self) -> bool:
        """Whether the module is default selected."""
        if self.definition:
            return self.definition.is_selected_by_default
        return False

    @cached_property
    def parent_relative_paths(self) -> set[Path]:
        """All relative parent paths of the module."""
        return set(self.relative_path.parents)

    @cached_property
    def hash(self) -> str:
        """The hash of the module."""
        return calculate_directory_hash(self.dir, shorten=True)

    @cached_property
    def resource_directories(self) -> set[str]:
        """The resource directories in the module."""
        source_path_by_resource_folder, _ = self._source_paths_by_resource_folder
        return set(source_path_by_resource_folder.keys())

    @property
    def _source_paths_by_resource_folder(self) -> tuple[dict[str, list[Path]], set[str]]:
        """The source paths grouped by resource folder."""
        source_paths_by_resource_folder = defaultdict(list)
        # The directories in the module that are not resource directories.
        invalid_resource_directory: set[str] = set()
        for filepath in self.source_paths:
            try:
                resource_folder = resource_folder_from_path(filepath)
            except ValueError:
                relative_to_module = filepath.relative_to(self.dir)
                is_file_in_resource_folder = relative_to_module.parts[0] == filepath.name
                if not is_file_in_resource_folder:
                    invalid_resource_directory.add(relative_to_module.parts[0])
                continue
            if filepath.is_file():
                source_paths_by_resource_folder[resource_folder].append(filepath)
        return source_paths_by_resource_folder, invalid_resource_directory

    @cached_property
    def source_paths_by_resource_folder(self) -> dict[str, list[Path]]:
        """The source paths grouped by resource folder."""
        source_paths_by_resource_folder, _ = self._source_paths_by_resource_folder

        # Sort to support 1., 2. etc prefixes
        def sort_key(p: Path) -> tuple[int, int, str]:
            first = {
                ".yaml": 0,
                ".yml": 0,
            }.get(p.suffix.lower(), 1)
            # We ensure that the YAML files are sorted before other files.
            # This is when we add indexes to files. We want to ensure that, for example, a .sql file
            # with the same name as a .yaml file gets the same index as the .yaml file.
            if result := INDEX_PATTERN.search(p.stem):
                return first, int(result.group()[:-1]), p.name
            else:
                return first, len(filepaths) + 1, p.name

        for filepaths in source_paths_by_resource_folder.values():
            # The builder of a module can control the order that resources are deployed by prefixing a number
            # The custom key 'sort_key' is to get the sort on integer and not the string.
            filepaths.sort(key=sort_key)

        return source_paths_by_resource_folder

    @cached_property
    def not_resource_directories(self) -> set[str]:
        """The directories in the module that are not resource directories."""
        return self._source_paths_by_resource_folder[1]

    @cached_property
    def dependencies(self) -> set[str]:
        """The dependencies of the module."""
        if self.definition:
            return set(self.definition.dependencies)
        return set()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, is_selected={self.is_selected}, file_count={len(self.source_paths)})"

    def __str__(self) -> str:
        return self.name

    def as_read_module(self) -> ReadModule:
        return ReadModule(
            dir=self.dir,
            resource_directories=tuple(self.resource_directories),
        )


@dataclass(frozen=True)
class ReadModule:
    """This is a short representation of a module.

    Args:
        dir: The absolute path to the module directory.
        resource_directories: The resource directories in the module.
    """

    dir: Path
    resource_directories: tuple[str, ...]

    def resource_dir_path(self, resource_folder: str) -> Path | None:
        """Returns the path to a resource in the module.

        Args:
            resource_folder: The name of the resource.

        Returns:
            The path to the resource if it exists, otherwise None.
        """
        for resource_dir in self.resource_directories:
            if resource_dir == resource_folder and (resource_path := self.dir / resource_folder).exists():
                return resource_path
        return None

    @classmethod
    def load(cls, data: dict[str, Any]) -> ReadModule:
        return cls(
            dir=Path(data["dir"]),
            resource_directories=tuple(data["resource_directories"]),
        )

    def dump(self) -> dict[str, Any]:
        return {
            "dir": self.dir.as_posix(),
            "resource_directories": list(self.resource_directories),
        }


class ModuleDirectories(tuple, Sequence[ModuleLocation]):
    """This is an internal representation of the module directories in a source directory.

    The motivation for this class is to provide helper functions for the user to interact with the module directories.
    """

    # Subclassing tuple to make the class immutable. ModuleDirectories is expected to be initialized and
    # then used as a read-only object.
    def __new__(cls, collection: Collection[ModuleLocation] | None) -> ModuleDirectories:
        # Need to override __new__ to as we are subclassing a tuple:
        #   https://stackoverflow.com/questions/1565374/subclassing-tuple-with-multiple-init-arguments
        return super().__new__(cls, tuple(collection or []))

    def __init__(self, collection: Collection[ModuleLocation] | None) -> None: ...

    @cached_property
    def available(self) -> set[str | Path]:
        return {selection for module_location in self for selection in module_location.module_selections}

    @cached_property
    def selected(self) -> ModuleDirectories:
        return ModuleDirectories([module for module in self if module.is_selected])

    @cached_property
    def available_paths(self) -> set[Path]:
        return {item for item in self.available if isinstance(item, Path)}

    @cached_property
    def available_names(self) -> set[str]:
        return {item for item in self.available if isinstance(item, str)}

    @classmethod
    def load(
        cls,
        organization_dir: Path,
        user_selected_modules: set[str | Path] | None = None,
    ) -> ModuleDirectories:
        """Loads the modules in the source directory.

        Args:
            organization_dir: The absolute path to the source directory.
            user_selected_modules: The modules selected by the user either by name or by path.

        """
        # Assume all modules are selected if no selection is given.
        user_selected_modules = user_selected_modules or {Path("")}

        module_locations: list[ModuleLocation] = []
        for module, source_paths in iterate_modules(organization_dir):
            relative_module_dir = module.relative_to(organization_dir)
            module_toml: ModuleToml | None = None
            tags: set[str] = set()
            if (module / ModuleToml.filename).exists():
                module_toml = ModuleToml.load(module / ModuleToml.filename)
                tags = set(module_toml.tags)

            module_locations.append(
                ModuleLocation(
                    module,
                    organization_dir,
                    source_paths,
                    cls._is_selected_module(relative_module_dir, user_selected_modules, tags),
                    module_toml,
                )
            )

        return cls(module_locations)

    def dump(self, organization_dir: Path) -> None:
        """Dumps the module directories to the source directory.

        Args:
            organization_dir: The absolute path to the source directory.
        """
        for module in self:
            module_dir = organization_dir / module.relative_path
            module_dir.mkdir(parents=True, exist_ok=True)
            for source_file in module.source_paths:
                relative_file_path = source_file.relative_to(module.dir)
                absolute_file_path = module_dir / relative_file_path
                absolute_file_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source_file, absolute_file_path)

    @classmethod
    def _is_selected_module(
        cls, relative_module_dir: Path, user_selected: set[str | Path], module_tags: set[str]
    ) -> bool:
        """Checks whether a module is selected by the user."""
        return (
            relative_module_dir.name in user_selected
            or relative_module_dir in user_selected
            or any(parent in user_selected for parent in relative_module_dir.parents)
            # Check if the module has any tags that the user has selected,
            # i.e., that the intersection of the module tags and the user selected tags is not empty.
            or bool(module_tags & user_selected)
        )

    def as_path_by_name(self) -> dict[str, list[Path]]:
        module_path_by_name: dict[str, list[Path]] = defaultdict(list)
        for module in self:
            module_path_by_name[module.name].append(module.relative_path)
        return module_path_by_name

    # Implemented to get correct type hints
    def __iter__(self) -> Iterator[ModuleLocation]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> ModuleLocation: ...

    @overload
    def __getitem__(self, index: slice) -> ModuleDirectories: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> ModuleLocation | ModuleDirectories:
        if isinstance(index, slice):
            return ModuleDirectories(super().__getitem__(index))
        return super().__getitem__(index)

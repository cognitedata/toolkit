from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import SupportsIndex, overload

from cognite_toolkit._cdf_tk.utils import iterate_modules


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
    is_selected: bool
    source_paths: list[Path]

    @property
    def name(self) -> str:
        """The name of the module."""
        return self.dir.name

    @property
    def relative_path(self) -> Path:
        """The relative path to the module."""
        return self.dir.relative_to(self.source_absolute_path)

    @property
    def module_selections(self) -> set[str | Path]:
        """Ways of selecting this module."""
        return {self.name, *self.relative_parent_paths}

    @cached_property
    def relative_parent_paths(self) -> set[Path]:
        """All relative parent paths of the module."""
        module_parts = self.relative_path
        return {module_parts.parents[i] for i in range(len(module_parts.parts))}


class ModuleDirectories(tuple, Sequence[ModuleLocation]):
    """This is an internal representation of the module directories in a source directory.

    The motivation for this class is to provide helper functions for the user to interact with the module directories.
    """

    # Subclassing tuple to make the class immutable. ModuleDirectories is expected to be initialized and
    # then used as a read-only object.
    def __new__(cls, collection: Collection[ModuleLocation]) -> ModuleDirectories:
        # Need to override __new__ to as we are subclassing a tuple:
        #   https://stackoverflow.com/questions/1565374/subclassing-tuple-with-multiple-init-arguments
        return super().__new__(cls, tuple(collection))

    def __init__(self, collection: Collection[ModuleLocation]) -> None: ...

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
        source_dir: Path,
        user_selected_modules: set[str | Path],
    ) -> ModuleDirectories:
        """Loads the modules in the source directory.

        Args:
            source_dir: The absolute path to the source directory.
            user_selected_modules: The modules selected by the user either by name or by path.

        """

        module_locations: list[ModuleLocation] = []
        for module, source_paths in iterate_modules(source_dir):
            relative_module_dir = module.relative_to(source_dir)
            module_locations.append(
                ModuleLocation(
                    module,
                    source_dir,
                    cls._is_selected_module(relative_module_dir, user_selected_modules),
                    source_paths,
                )
            )

        return cls(module_locations)

    @classmethod
    def _is_selected_module(cls, relative_module_dir: Path, user_selected: set[str | Path]) -> bool:
        """Checks whether a module is selected by the user."""
        return (
            relative_module_dir.name in user_selected
            or relative_module_dir in user_selected
            or any(parent in user_selected for parent in relative_module_dir.parents)
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

from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator, Sequence
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
    def module_references(self) -> Iterable[str | tuple[str, ...]]:
        """Ways of selecting this module."""
        yield self.name
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

    @cached_property
    def selected(self) -> ModuleDirectories:
        return ModuleDirectories([module for module in self if module.is_selected])

    def as_paths(self) -> set[tuple[str, ...]]:
        return {module.relative_path.parts[:i] for module in self for i in range(len(module.relative_path.parts) + 1)}

    @classmethod
    def load(
        cls,
        source_dir: Path,
        selected_modules: set[str | tuple[str, ...]],
    ) -> ModuleDirectories:
        """Loads the modules in the source directory."""
        module_locations: list[ModuleLocation] = []
        for module, source_paths in iterate_modules(source_dir):
            relative_module_dir = module.relative_to(source_dir)
            module_locations.append(
                ModuleLocation(
                    module,
                    source_dir,
                    cls._is_selected_module(relative_module_dir, selected_modules),
                    source_paths,
                )
            )

        return cls(module_locations)

    @classmethod
    def _is_selected_module(cls, relative_module_dir: Path, selected: set[str | tuple[str, ...]]) -> bool:
        """Checks whether a module is selected by the user."""
        module_parts = relative_module_dir.parts
        in_selected = relative_module_dir.name in selected or module_parts in selected
        is_parent_in_selected = any(
            parent in selected for parent in (module_parts[:i] for i in range(1, len(module_parts)))
        )
        return is_parent_in_selected or in_selected

    def as_parts_by_name(self) -> dict[str, list[tuple[str, ...]]]:
        module_parts_by_name: dict[str, list[tuple[str, ...]]] = defaultdict(list)
        for module in self:
            module_parts_by_name[module.name].append(module.relative_path.parts)
        return module_parts_by_name

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

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import overload


@dataclass(frozen=True)
class Package:
    """This represents a bundle of modules.
    Args:
        name: the unique identifier of the package.
        source_absolute_path: The absolute path to the source directory.
        is_selected: Whether the module is selected by the user.
        source_paths: The paths to all files in the module.
    """

    name: str
    description: str | None = None


@dataclass(frozen=True)
class Packages(Sequence[Package]):
    """This is an internal representation of the packages in a source directory."""

    @overload
    def __init__(self, collection: Collection[Package]) -> None: ...

    @overload
    def __init__(self) -> None: ...

    def __init__(self, collection: Collection[Package] | None = None) -> None:
        if collection is None:
            collection = []
        super().__init__()

    @classmethod
    def load(
        cls,
        source_dir: Path,
    ) -> Packages:
        """Loads the packages in the source directory.

        Args:
            source_dir: The absolute path to the source directory.
        """

        toml_files = list(source_dir.rglob("*.toml"))
        return cls([Package(name=file.stem) for file in toml_files])

    def __len__(self) -> int:
        return super().__len__()

    @overload
    def __getitem__(self, index: int) -> Package: ...

    @overload
    def __getitem__(self, index: slice) -> Packages: ...

    def __getitem__(self, index: int | slice, /) -> Package | Packages:
        if isinstance(index, slice):
            return Packages(super().__getitem__(index))
        return super().__getitem__(index)

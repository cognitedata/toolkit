from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from typing import overload

import toml
from cognite.client.utils._text import to_camel_case

from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleDirectories


@dataclass
class Package:
    """A package represents a bundle of modules.
    Args:
        name: the unique identifier of the package.
        source_absolute_path: The absolute path to the source directory.
        is_selected: Whether the module is selected by the user.
        source_paths: The paths to all files in the module.
    """

    name: str
    _modules: dict[str, str | None] = field(default_factory=dict)

    @property
    def description(self) -> str | None:
        return to_camel_case(self.name)

    @property
    def modules(self) -> dict[str, str | None]:
        return self._modules or {}

    def add_module(self, name: str, description: str | None = None) -> None:
        self._modules[name] = description


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
        modules: ModuleDirectories,
    ) -> Packages:
        """Loads the packages in the source directory.

        Args:
            modules: The module directories to load the packages from.
        """

        collected: dict[str, Package] = {}
        for module in modules:
            if manifest := next((file for file in module.source_paths if file.name == "module.toml"), None):
                try:
                    config = toml.load(manifest)
                    description = config["module"].get("description", None)
                    for tag in config["packages"]["tags"]:
                        if tag not in collected:
                            collected[tag] = Package(name=tag)
                        collected[tag].add_module(module.name, description)

                except Exception as e:
                    print(f"Error loading module config for: {module.name}: {e}")
                    raise e

        return cls(list(collected.values()))

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

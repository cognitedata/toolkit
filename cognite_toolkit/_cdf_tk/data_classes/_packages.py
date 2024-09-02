from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Optional, overload

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


@dataclass
class Packages(list, MutableSequence[Package]):
    @overload
    def __init__(self, packages: Iterable[Package]) -> None: ...

    @overload
    def __init__(self) -> None: ...

    def __init__(self, packages: Optional[Iterable[Package]] = None) -> None:
        super().__init__(packages or [])

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
            if module.manifest:
                for tag in module.manifest.tags or []:
                    if tag not in collected:
                        collected[tag] = Package(name=tag)
                    collected[tag].add_module(module.name, module.manifest.description)

        return cls(list(collected.values()))

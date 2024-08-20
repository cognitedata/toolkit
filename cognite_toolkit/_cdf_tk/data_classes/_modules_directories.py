from collections.abc import MutableSequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass
class ModuleLocation:
    """This represents the location of a module in a directory structure."""

    name: str
    relative_path: Path
    root_module: Literal["cognite_modules", "custom_modules", "modules"]
    root_absolute_path: Path

    @property
    def path(self) -> Path:
        return self.root_absolute_path / self.relative_path


class ModulesDirectories(list, MutableSequence[ModuleLocation]):
    @classmethod
    def load(cls, source_dir: Path) -> "ModulesDirectories":
        raise NotImplementedError()

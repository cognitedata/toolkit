import sys
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Module(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    path: Path
    definition: ModuleToml | None = None

    @classmethod
    def load(cls, path: Path) -> Self:
        definition = ModuleToml.load(path / ModuleToml.filename) if (path / ModuleToml.filename).exists() else None
        return cls(path=path, definition=definition)

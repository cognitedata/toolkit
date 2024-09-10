from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


@dataclass(frozen=True)
class ModuleToml:
    filename: ClassVar[str] = "module.toml"
    description: str | None
    tags: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def load(cls, data: dict[str, Any] | Path) -> ModuleToml:
        if isinstance(data, Path):
            return cls.load(toml.loads(data.read_text()))
        return cls(
            description=data["module"].get("description"),
            tags=frozenset(data["packages"].get("tags", set())),
        )

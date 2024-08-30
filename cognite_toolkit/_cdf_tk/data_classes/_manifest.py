from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


@dataclass(frozen=True)
class Manifest:
    version: str
    description: str | None
    tags: list[str] | None = field(default_factory=list)

    @classmethod
    def load(cls, manifest_path: Path) -> Manifest:
        manifest = toml.loads(manifest_path.read_text())
        return cls(
            version=manifest["module"]["version"],
            description=manifest["module"].get("description"),
            tags=manifest["packages"]["tags"],
        )

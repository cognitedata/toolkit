from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import toml


@dataclass(frozen=True)
class Manifest:
    version: str
    description: str | None
    tags: list[str] | None = field(default_factory=list)

    @classmethod
    def load(cls, manifest_path: Path) -> Manifest:
        manifest = toml.load(manifest_path)
        return cls(
            version=manifest["module"]["version"],
            description=manifest["module"].get("description"),
            tags=manifest["packages"]["tags"],
        )

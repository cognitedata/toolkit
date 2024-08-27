from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print

from cognite_toolkit._cdf_tk.data_classes._base import ConfigCore, _load_version_variable


@dataclass
class SystemYAML(ConfigCore):
    file_name: ClassVar[str] = "_system.yaml"
    cdf_toolkit_version: str
    packages: dict[str, list[str]] = field(default_factory=dict)

    @property
    def module_version(self) -> Version:
        return parse_version(self.cdf_toolkit_version)

    @classmethod
    def _file_name(cls, build_env_name: str) -> str:
        return cls.file_name

    @classmethod
    def load(cls, data: dict[str, Any], build_env_name: str, filepath: Path) -> SystemYAML:
        version = _load_version_variable(data, filepath.name)
        packages = data.get("packages", {})
        if not packages:
            print(f"  [bold yellow]Warning:[/] No packages defined in {cls.file_name}.")
        return cls(
            filepath=filepath,
            cdf_toolkit_version=version,
            packages=packages,
        )

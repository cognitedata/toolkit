from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print

from cognite_toolkit._cdf_tk.constants import ROOT_MODULES
from cognite_toolkit._cdf_tk.data_classes._base import ConfigCore, _load_version_variable
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingModulesError


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

    @staticmethod
    def validate_module_dir(source_path: Path) -> list[Path]:
        sources = [module_dir for root_module in ROOT_MODULES if (module_dir := source_path / root_module).exists()]
        if not sources:
            directories = "\n".join(f"   ┣ {name}" for name in ROOT_MODULES[:-1])
            raise ToolkitMissingModulesError(
                f"Could not find the source modules directory.\nExpected to find one of the following directories\n"
                f"{source_path.name}\n{directories}\n   ┗  {ROOT_MODULES[-1]}"
            )
        return sources

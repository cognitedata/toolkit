from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitRequiredValueError, ToolkitVersionError

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@dataclass
class CLIConfig:
    project_dir: Path | None = None

    @classmethod
    def load(cls, raw: dict[str, Any], source_dir: Path) -> CLIConfig:
        project_dir = source_dir / raw["project_dir"] if "project_dir" in raw else None
        return cls(project_dir=project_dir)


@dataclass
class ModulesConfig:
    version: str
    packages: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def load(cls, raw: dict[str, Any]) -> ModulesConfig:
        version = raw["version"]
        packages = raw.get("packages", {})
        if version != _version.__version__:
            raise ToolkitVersionError(
                f"The version of the modules ({version}) does not match the version of the installed CLI "
                f"({_version.__version__}). Please either run `cdf-tk modules upgrade` to upgrade the modules OR "
                f"run `pip install cognite-toolkit=={version}` to downgrade cdf-tk CLI."
            )
        return cls(version=version, packages=packages)


@dataclass
class CDFToml:
    """This is the configuration for the CLI and Modules"""

    file_name: ClassVar[str] = "cdf.toml"

    cdf: CLIConfig
    modules: ModulesConfig

    @classmethod
    def load(cls, path: Path | None = None) -> CDFToml:
        path = path or Path.cwd()
        file_path = path / cls.file_name
        if not file_path.exists():
            raise ToolkitFileNotFoundError(
                f"Could not find {cls.file_name} in {path}. " f"This file is required to run the toolkit."
            )
        # TOML files are required to be UTF-8 encoded
        raw = tomllib.loads(file_path.read_text(encoding="utf-8"))
        # No required fields in the cdf section
        cdf = CLIConfig.load(raw["cdf"], path) if "cdf" in raw else CLIConfig()
        try:
            modules = ModulesConfig.load(raw["modules"])
        except KeyError as e:
            raise ToolkitRequiredValueError(f"Missing required value in {cls.file_name}: {e.args}")
        return cls(cdf=cdf, modules=modules)

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.constants import clean_name
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
    ToolkitVersionError,
)

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@dataclass
class CLIConfig:
    default_organization_dir: Path
    default_env: str = "dev"
    has_user_set_default_org: bool = False

    @classmethod
    def load(cls, raw: dict[str, Any], cwd: Path) -> CLIConfig:
        has_user_set_default_org = "default_organization_dir" in raw
        default_organization_dir = cwd / raw["default_organization_dir"] if has_user_set_default_org else Path.cwd()
        return cls(
            default_organization_dir=default_organization_dir,
            default_env=raw.get("default_env", "dev"),
            has_user_set_default_org=has_user_set_default_org,
        )


@dataclass
class ModulesConfig:
    version: str
    packages: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def load(cls, raw: dict[str, Any]) -> ModulesConfig:
        version = raw["version"]
        packages = raw.get("packages", {})
        if version != _version.__version__ and (len(sys.argv) > 2 and sys.argv[1:3] != ["modules", "upgrade"]):
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
    feature_flags: dict[str, bool] = field(default_factory=dict)
    plugins: dict[str, bool] = field(default_factory=dict)

    is_loaded_from_file: bool = False

    @classmethod
    def load(cls, cwd: Path | None = None, use_singleton: bool = True) -> CDFToml:
        """Loads the cdf.toml file from the given path. If use_singleton is True, the instance will be stored as a
        singleton and returned on subsequent calls."""
        global _CDF_TOML
        if use_singleton and _CDF_TOML:
            return _CDF_TOML
        cwd = cwd or Path.cwd()
        file_path = cwd / cls.file_name
        if file_path.exists():
            # TOML files are required to be UTF-8 encoded
            raw = tomllib.loads(file_path.read_text(encoding="utf-8"))
            # No required fields in the cdf section
            cdf = CLIConfig.load(raw["cdf"], cwd) if "cdf" in raw else CLIConfig(cwd)
            try:
                modules = ModulesConfig.load(raw["modules"])
            except KeyError as e:
                raise ToolkitRequiredValueError(f"Missing required value in {cls.file_name}: {e.args}")

            if "feature_flags" in raw:
                feature_flags = {clean_name(k): v for k, v in raw["feature_flags"].items()}
            if "plugins" in raw:
                plugins = {clean_name(k): v for k, v in raw["feature_flags"].items()}

            instance = cls(
                cdf=cdf, modules=modules, feature_flags=feature_flags, plugins=plugins, is_loaded_from_file=True
            )
            if use_singleton:
                _CDF_TOML = instance
            return instance
        else:
            return cls(
                cdf=CLIConfig(cwd),
                modules=ModulesConfig.load({"version": _version.__version__}),
                feature_flags={},
                plugins={},
                is_loaded_from_file=False,
            )


_CDF_TOML: CDFToml | None = None

if __name__ == "__main__":
    # This is a test to quickly check that the code works.
    # also useful to check that when you change cdf.toml it is loaded correctly
    _ROOT = Path(__file__).parent.parent.parent
    print(CDFToml.load(_ROOT))

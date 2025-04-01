from __future__ import annotations

import re
import sys
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from rich import print

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.constants import clean_name
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
    ToolkitTOMLFormatError,
    ToolkitVersionError,
)
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning

if sys.version_info >= (3, 11):
    import tomllib
    from tomllib import TOMLDecodeError
else:
    import tomli as tomllib
    from tomli import TOMLDecodeError


@dataclass
class CLIConfig:
    default_organization_dir: Path
    default_env: str = "dev"
    has_user_set_default_org: bool = False
    has_user_set_default_env: bool = False

    @classmethod
    def load(cls, raw: dict[str, Any], cwd: Path) -> CLIConfig:
        has_user_set_default_org = "default_organization_dir" in raw
        has_user_set_default_env = "default_env" in raw
        default_organization_dir = cwd / raw["default_organization_dir"] if has_user_set_default_org else Path.cwd()
        return cls(
            default_organization_dir=default_organization_dir,
            default_env=raw.get("default_env", "dev"),
            has_user_set_default_org=has_user_set_default_org,
            has_user_set_default_env=has_user_set_default_env,
        )


@dataclass
class ModulesConfig:
    version: str
    packages: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def load(cls, raw: dict[str, Any]) -> ModulesConfig:
        version = raw["version"]
        packages = raw.get("packages", {})
        if (
            version != _version.__version__
            and (len(sys.argv) > 2 and sys.argv[1:3] != ["modules", "upgrade"])
            and "--help" not in sys.argv
        ):
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
    alpha_flags: dict[str, bool] = field(default_factory=dict)
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
            raw = _read_toml(file_path)

            # No required fields in the cdf section
            cdf = CLIConfig.load(raw["cdf"], cwd) if "cdf" in raw else CLIConfig(cwd)
            try:
                modules = ModulesConfig.load(raw["modules"])
            except KeyError as e:
                raise ToolkitRequiredValueError(f"Missing required value in {cls.file_name}: {e.args}")
            alpha_flags = {}
            if "alpha_flags" in raw:
                alpha_flags = {clean_name(k): v for k, v in raw["alpha_flags"].items()}
            if not alpha_flags and "feature_flags" in raw:
                MediumSeverityWarning(
                    "The 'feature_flags' section has been renamed to 'alpha_flags'. Please update your cdf.toml file."
                ).print_warning()
                alpha_flags = {clean_name(k): v for k, v in raw["feature_flags"].items()}

            plugins = {}
            if "plugins" in raw:
                plugins = {clean_name(k): v for k, v in raw["plugins"].items()}

            instance = cls(cdf=cdf, modules=modules, alpha_flags=alpha_flags, plugins=plugins, is_loaded_from_file=True)
            if use_singleton:
                _CDF_TOML = instance
            return instance
        else:
            return cls(
                cdf=CLIConfig(cwd),
                modules=ModulesConfig.load({"version": _version.__version__}),
                alpha_flags={},
                plugins={},
                is_loaded_from_file=False,
            )


def _read_toml(file_path: Path) -> dict[str, Any]:
    # TOML files are required to be UTF-8 encoded
    content = file_path.read_text(encoding="utf-8")
    try:
        return tomllib.loads(content)
    except TOMLDecodeError as e:
        if file_path.is_relative_to(Path.cwd()):
            file_path = file_path.relative_to(Path.cwd())
        extra = ""
        with suppress(Exception):
            # If any errors is raised here, we ignore it and continue
            if result := re.search(r"\(at line (\d+), column (\d+)\)", str(e)):
                line_no = int(result.group(1))
                column_location = int(result.group(2))
                lines = content.splitlines()
                line = lines[line_no - 1]
                prefix = "\n    [blue]HINT: [/blue]"
                if column_location >= len(line):
                    extra = f"{prefix}This is near the end of the line '{line}'"
                elif column_location == 0:
                    extra = f"{prefix}This is near the beginning of the line '{line}'"
                else:
                    extra = f"{prefix}This is near the '{line[:column_location]}' in '{line[column_location:]}'"

        err = ToolkitTOMLFormatError(f"Error reading {file_path.as_posix()!r}: {e!s}{extra}")
        print(f"  [bold red]ERROR ([/][red]{type(err).__name__}[/][bold red]):[/] {err}")
        # We read the CDF.TML file from the module level, so the app has not been loaded yet.
        # Therefore, we raise SystemExit to stop the execution
        raise SystemExit(1)


_CDF_TOML: CDFToml | None = None

if __name__ == "__main__":
    from pprint import pprint

    # This is a test to quickly check that the code works.
    # also useful to check that when you change cdf.toml it is loaded correctly
    _ROOT = Path(__file__).parent.parent.parent
    pprint(CDFToml.load(_ROOT))  # noqa: T203

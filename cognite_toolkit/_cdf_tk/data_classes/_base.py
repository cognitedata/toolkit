from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, TypeVar

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.constants import BUILD_ENVIRONMENT_FILE
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitRequiredValueError, ToolkitVersionError
from cognite_toolkit._cdf_tk.utils import read_yaml_file


@dataclass
class ConfigCore(ABC):
    """Base class for config files."""

    filename: ClassVar[str]
    filepath: Path

    @classmethod
    def get_filename(cls, build_env: str) -> str:
        return cls.filename.format(build_env=build_env)

    @classmethod
    def load_from_directory(cls: type[T_BuildConfig], organization_dir: Path, build_env: str) -> T_BuildConfig:
        filename = cls.get_filename(build_env)
        filepath = organization_dir / filename
        filepath = filepath if filepath.is_file() else Path.cwd() / filename
        if not filepath.is_file():
            raise ToolkitFileNotFoundError(f"{filename!r} does not exist.")

        try:
            return cls.load(read_yaml_file(filepath), build_env, filepath)
        except KeyError as e:
            raise ToolkitRequiredValueError(f"Required field {e.args} is missing in {filename!r}.") from e

    @classmethod
    @abstractmethod
    def load(cls: type[T_BuildConfig], data: dict[str, Any], build_env: str, filepath: Path) -> T_BuildConfig:
        raise NotImplementedError


T_BuildConfig = TypeVar("T_BuildConfig", bound=ConfigCore)


def _load_version_variable(data: dict[str, Any], file_name: str) -> str:
    try:
        cdf_tk_version: str = data["cdf_toolkit_version"]
    except KeyError:
        err_msg = f"System variables are missing required field 'cdf_toolkit_version' in {file_name!s}. {{}}"
        if file_name == BUILD_ENVIRONMENT_FILE:
            raise ToolkitVersionError(
                err_msg.format("Rerun `cdf build` to build the modules again and create it correctly.")
            )
        raise ToolkitVersionError(
            err_msg.format("Run `cdf modules upgrade` to initialize the modules again to create a correct file.")
        )

    if cdf_tk_version != _version.__version__:
        raise ToolkitVersionError(
            f"The version of the modules ({cdf_tk_version}) does not match the version of the installed CLI "
            f"({_version.__version__}). Please either run `cdf modules upgrade` to upgrade the modules OR "
            f"run `pip install cognite-toolkit=={cdf_tk_version}` to downgrade cdf-tk CLI."
        )
    return cdf_tk_version

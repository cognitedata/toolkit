from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.constants import BUILD_ENVIRONMENT_FILE
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitVersionError
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, UnexpectedFileLocationWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_file


@dataclass
class ConfigCore(ABC):
    """Base class for the two build config files (global.yaml and [env].config.yaml)"""

    filepath: Path

    @classmethod
    @abstractmethod
    def _file_name(cls, build_env_name: str) -> str:
        raise NotImplementedError

    @classmethod
    def load_from_directory(
        cls: type[T_BuildConfig],
        source_path: Path,
        build_env_name: str,
        warn: Callable[[ToolkitWarning], None] | None = None,
        command: str | None = None,
    ) -> T_BuildConfig:
        file_name = cls._file_name(build_env_name)
        filepath = source_path / file_name
        filepath = filepath if filepath.is_file() else Path.cwd() / file_name
        if (
            (old_filepath := (source_path / "cognite_modules" / file_name)).is_file()
            and not filepath.is_file()
            and file_name == "_system.yaml"
        ):
            # This is a fallback for the old location of the system file
            warning = UnexpectedFileLocationWarning(filepath.name, f"cognite_toolkit/{old_filepath.name}")
            if warn is not None:
                warn(warning)
            else:
                print(warning.get_message())
            filepath = old_filepath
        elif not filepath.is_file():
            if not (result := next(source_path.glob(f"**/{file_name}"), None)):
                raise ToolkitFileNotFoundError(f"{file_name!r} does not exist.")

            relative = result.absolute().relative_to(Path.cwd())
            hint = "/".join(relative.parts[:-1])
            if command:
                hint = f"{command} {hint}"
            raise ToolkitFileNotFoundError(f"{file_name!r} does not exist. Did you mean {hint!r}?")

        return cls.load(read_yaml_file(filepath), build_env_name, filepath)

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
                err_msg.format("Rerun `cdf-tk build` to build the modules again and create it correctly.")
            )
        raise ToolkitVersionError(
            err_msg.format("Run `cdf-tk init --upgrade` to initialize the modules again to create a correct file.")
        )

    if cdf_tk_version != _version.__version__:
        raise ToolkitVersionError(
            f"The version of the modules ({cdf_tk_version}) does not match the version of the installed CLI "
            f"({_version.__version__}). Please either run `cdf-tk init --upgrade` to upgrade the modules OR "
            f"run `pip install cognite-toolkit=={cdf_tk_version}` to downgrade cdf-tk CLI."
        )
    return cdf_tk_version

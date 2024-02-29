from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from rich import print

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.templates import BUILD_ENVIRONMENT_FILE
from cognite_toolkit._cdf_tk.utils import read_yaml_file


@dataclass
class ConfigCore(ABC):
    """Base class for the two build config files (global.yaml and [env].config.yaml)"""

    filepath: Path

    @classmethod
    @abstractmethod
    def _file_name(cls, build_env: str) -> str:
        raise NotImplementedError

    @classmethod
    def load_from_directory(cls: type[T_BuildConfig], source_path: Path, build_env: str) -> T_BuildConfig:
        file_name = cls._file_name(build_env)
        filepath = source_path / file_name
        filepath = filepath if filepath.is_file() else Path.cwd() / file_name
        if not filepath.is_file():
            print(f"  [bold red]ERROR:[/] {filepath.name!r} does not exist")
            exit(1)
        return cls.load(read_yaml_file(filepath), build_env, filepath)

    @classmethod
    @abstractmethod
    def load(cls: type[T_BuildConfig], data: dict[str, Any], build_env: str, filepath: Path) -> T_BuildConfig:
        raise NotImplementedError


T_BuildConfig = TypeVar("T_BuildConfig", bound=ConfigCore)


def _load_version_variable(data: dict[str, Any], file_name: str) -> str:
    try:
        cdf_tk_version: str = data["cdf_toolkit_version"]
    except KeyError:
        print(
            f"  [bold red]ERROR:[/] System variables are missing required field 'cdf_toolkit_version' in {file_name!s}"
        )
        if file_name == BUILD_ENVIRONMENT_FILE:
            print(f"  rerun `cdf-tk build` to build the templates again and create `{file_name!s}` correctly.")
        else:
            print(
                f"  run `cdf-tk init --upgrade` to initialize the templates again and create a correct `{file_name!s}` file."
            )
        exit(1)
    if cdf_tk_version != _version.__version__:
        print(
            f"  [bold red]Error:[/] The version of the templates ({cdf_tk_version}) does not match the version of the installed package ({_version.__version__})."
        )
        print("  Please either run `cdf-tk init --upgrade` to upgrade the templates OR")
        print(f"  run `pip install cognite-toolkit==={cdf_tk_version}` to downgrade cdf-tk.")
        exit(1)
    return cdf_tk_version

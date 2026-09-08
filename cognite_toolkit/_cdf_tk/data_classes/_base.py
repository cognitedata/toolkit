import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.utils import read_yaml_file

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class ConfigCore(ABC):
    """Base class for config files."""

    filename: ClassVar[str]
    filepath: Path

    @classmethod
    def get_filename(cls, build_env: str) -> str:
        return cls.filename.format(build_env=build_env)

    @classmethod
    def load_from_directory(cls, organization_dir: Path, build_env: str) -> Self:
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
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> Self:
        raise NotImplementedError

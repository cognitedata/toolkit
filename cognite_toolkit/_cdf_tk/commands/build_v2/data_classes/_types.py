from pathlib import Path
from typing import Annotated, Literal, TypeAlias

from pydantic import AfterValidator


def _is_relative_file_path(p: Path) -> Path:
    if not p.suffix:
        raise ValueError(f"{p.as_posix()!r} is not a file.")
    if p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not a relative path.")
    return p


def _is_absolute_file_path(p: Path) -> Path:
    if not p.suffix:
        raise ValueError(f"{p.as_posix()!r} is not a file.")
    if not p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not an absolute path.")
    return p


def _is_relative_dir_path(p: Path) -> Path:
    if p.suffix:
        raise ValueError(f"{p.as_posix()!r} is not a directory.")
    if p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not a relative path.")
    return p


def _is_absolute_dir_path(p: Path) -> Path:
    if p.suffix:
        raise ValueError(f"{p.as_posix()!r} is not a directory.")
    if not p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not an absolute path.")
    return p


RelativeFilePath: TypeAlias = Annotated[Path, AfterValidator(_is_relative_file_path)]
AbsoluteFilePath: TypeAlias = Annotated[Path, AfterValidator(_is_absolute_file_path)]
RelativeDirPath: TypeAlias = Annotated[Path, AfterValidator(_is_relative_dir_path)]
AbsoluteDirPath: TypeAlias = Annotated[Path, AfterValidator(_is_absolute_dir_path)]


ValidationType: TypeAlias = Literal["dev", "prod"]

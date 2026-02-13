from pathlib import Path
from typing import Annotated, Literal, TypeAlias

from pydantic import PlainValidator


def _is_relative_dir_path(p: Path) -> Path:
    if not isinstance(p, Path):
        # Let pydantic handle the type error.
        return p
    if p.suffix:
        raise ValueError(f"{p.as_posix()!r} is not a directory.")
    if p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not a relative path.")
    return p


def _is_absolute_dir_path(p: Path) -> Path:
    if not isinstance(p, Path):
        # Let pydantic handle the type error.
        return p
    if p.suffix:
        raise ValueError(f"{p.as_posix()!r} is not a directory.")
    if not p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not an absolute path.")
    return p


RelativeDirPath: TypeAlias = Annotated[Path, PlainValidator(_is_relative_dir_path)]
AbsoluteDirPath: TypeAlias = Annotated[Path, PlainValidator(_is_absolute_dir_path)]


ValidationType: TypeAlias = Literal["dev", "prod"]

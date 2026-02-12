from pathlib import Path
from typing import Annotated, TypeAlias

from pydantic import PlainValidator


def _is_relative_dir_path(p: Path) -> Path:
    if not p.is_dir():
        raise ValueError(f"{p.as_posix()!r} is not a directory.")
    if p.is_absolute():
        raise ValueError(f"{p.as_posix()!r} is not a relative path.")
    return p


RelativeDirPath: TypeAlias = Annotated[Path, PlainValidator(_is_relative_dir_path)]

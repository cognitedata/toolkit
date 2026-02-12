from pathlib import Path
from typing import Annotated, TypeAlias

from pydantic import PlainValidator

RelativeDirPath: TypeAlias = Annotated[
    Path, PlainValidator(lambda p: p if p.is_dir() and p.is_relative() else ValueError(f"{p} is not a directory"))
]

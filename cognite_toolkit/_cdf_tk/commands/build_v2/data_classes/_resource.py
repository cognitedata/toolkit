import sys
from pathlib import Path

from pydantic import BaseModel, ConfigDict

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Resource(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    path: Path

    @classmethod
    def load(cls, path: Path) -> Self:
        return cls(path=path)

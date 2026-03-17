from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class ProgressObject(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)


class FromCDF(ProgressObject):
    type: Literal["cdf"] = "cdf"
    cursors: dict[str, str]


class FileLocation(ProgressObject):
    lineno: int
    filepath: Path


class FromFile(ProgressObject):
    type: Literal["file"] = "file"
    locations: FileLocation


class ProgressYAML(ProgressObject):
    status: Literal["in-progress", "completed", "failed"]
    progress: FromCDF | FromFile = Field(discriminator="type")
    files: list[Path]

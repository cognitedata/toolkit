import os
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, Field, JsonValue

from ._base import YAMLFile
from ._types import ValidationType


class Environment(BaseModel):
    name: str = "dev"
    project: str = Field(default_factory=lambda: os.environ.get("CDF_PROJECT", "UNKNOWN"))
    validation_type: ValidationType = Field("dev", alias="validation-type")
    selected: list[str] | None = None


class ConfigYAML(YAMLFile):
    filename: ClassVar[str] = "config.{name}.yaml"

    environment: Environment = Field(default_factory=Environment)
    variables: dict[str, JsonValue] | None = None

    @classmethod
    def get_filename(cls, name: str) -> str:
        return cls.filename.format(name=name)

    @classmethod
    def get_filepath(cls, organization_dir: Path, name: str) -> Path:
        return organization_dir / cls.get_filename(name)

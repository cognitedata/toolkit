import os
from typing import ClassVar, Literal

from pydantic import BaseModel, Field, JsonValue


class Environment(BaseModel):
    name: str = "dev"
    project: str = Field(default_factory=lambda: os.environ.get("CDF_PROJECT", "UNKNOWN"))
    validation_type: Literal["dev", "prod"] = "dev"
    selected: list[str] | None = None


class ConfigYAML(BaseModel):
    filename: ClassVar[str] = "config.{name}.yaml"

    environment: Environment = Field(default_factory=Environment)
    variables: dict[str, JsonValue] | None = None

    @classmethod
    def get_filename(cls, name: str) -> str:
        return cls.filename.format(name=name)

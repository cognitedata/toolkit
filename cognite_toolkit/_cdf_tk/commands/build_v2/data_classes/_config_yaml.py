from typing import ClassVar

from pydantic import BaseModel


class ConfigYAML(BaseModel):
    filename: ClassVar[str] = "config.{name}.yaml"

    @classmethod
    def get_filename(cls, name: str) -> str:
        return cls.filename.format(name=name)

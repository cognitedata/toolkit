from pathlib import Path
from typing import Annotated, ClassVar, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Discriminator, TypeAdapter
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.file import read_yaml_file, safe_write


class ProgressObject(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)


class Progress(ProgressObject):
    file_suffix: ClassVar[Literal["Progress"]] = "Progress"
    type: str
    status: Literal["in-progress", "completed", "failed", "stopped"]

    @classmethod
    def try_load(cls, directory: Path, filestem: str) -> "ProgressYAML | None":
        filepath = cls._get_filepath(directory, filestem)
        if not filepath.exists():
            return None
        return ProgressYAMLLoader.validate_python(read_yaml_file(filepath))

    @classmethod
    def _get_filepath(cls, directory: Path, filestem: str) -> Path:
        return directory / f"{filestem}.{cls.file_suffix}.yaml"

    def dump_to_file(self, directory: Path, filestem: str) -> None:
        filepath = self._get_filepath(directory, filestem)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        safe_write(filepath, yaml.safe_dump(self.model_dump(by_alias=True)))


class CDFProgressYAML(Progress):
    type: Literal["cdf"] = "cdf"
    cursors: dict[str, str]


class FileLocation(ProgressObject):
    lineno: int
    filepath: Path


class FileProgressYAML(Progress):
    type: Literal["file"] = "file"
    locations: dict[str, FileLocation]


ProgressYAML = Annotated[CDFProgressYAML | FileProgressYAML, Discriminator("type")]


ProgressYAMLLoader = TypeAdapter[ProgressYAML](ProgressYAML)

from abc import ABC
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, field_validator

from ._base import DataSelector, SelectorObject

FILENAME_VARIABLE = "$FILENAME"


class FileContentSelector(DataSelector, ABC):
    kind: Literal["FileContent"] = "FileContent"
    file_directory: Path


class FileMetadataTemplate(SelectorObject):
    model_config = ConfigDict(extra="allow")
    name: str
    external_id: str

    @field_validator("name", "external_id")
    @classmethod
    def _validate_filename_in_fields(cls, v: str) -> str:
        if FILENAME_VARIABLE not in v:
            raise ValueError(
                f"{FILENAME_VARIABLE!s} must be present in 'name' and 'external_id' fields. "
                f"This allows for dynamic substitution based on the file name."
            )
        return v


class FileMetadataTemplateSelector(FileContentSelector):
    type: Literal["fileMetadataTemplate"] = "fileMetadataTemplate"
    template: FileMetadataTemplate

    @property
    def group(self) -> str:
        return "FileMetadata"

    def __str__(self) -> str:
        return "metadata_template"


class FileDataModelingTemplateSelector(FileContentSelector):
    type: Literal["fileDataModelingTemplate"] = "fileDataModelingTemplate"
    template_name: str

    @property
    def group(self) -> str:
        return f"DataModelingTemplate_{self.template_name}"

    def __str__(self) -> str:
        return self.template_name

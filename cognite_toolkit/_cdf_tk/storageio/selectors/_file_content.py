from abc import ABC
from pathlib import Path
from typing import Literal

from ._base import DataSelector


class FileContentSelector(DataSelector, ABC):
    kind: Literal["FileContent"] = "FileContent"
    file_directory: Path


class FileMetadataTemplateSelector(FileContentSelector):
    type: Literal["fileMetadataTemplate"] = "fileMetadataTemplate"
    template_name: str

    @property
    def group(self) -> str:
        return f"FileMetadataTemplate_{self.template_name}"

    def __str__(self) -> str:
        return self.template_name


class FileDataModelingTemplateSelector(FileContentSelector):
    type: Literal["fileDataModelingTemplate"] = "fileDataModelingTemplate"
    template_name: str

    @property
    def group(self) -> str:
        return f"DataModelingTemplate_{self.template_name}"

    def __str__(self) -> str:
        return self.template_name

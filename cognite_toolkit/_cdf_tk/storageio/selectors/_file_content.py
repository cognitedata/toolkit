import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal

from pydantic import ConfigDict, field_validator, model_validator

from ._base import DataSelector, SelectorObject
from ._instances import SelectedView

FILENAME_VARIABLE = "$FILENAME"
FILEPATH = "$FILEPATH"


class FileContentSelector(DataSelector, ABC):
    kind: Literal["FileContent"] = "FileContent"
    file_directory: Path

    def find_data_files(self, input_dir: Path, manifest_file: Path) -> list[Path]:
        file_dir = input_dir / self.file_directory
        if not file_dir.is_dir():
            return []
        return [file for file in file_dir.iterdir() if file.is_file()]

    @abstractmethod
    def create_instance(self, filepath: Path) -> dict[str, Any]: ...


class FileTemplate(SelectorObject):
    model_config = ConfigDict(extra="allow")

    def create_instance(self, filename: str) -> dict[str, Any]:
        json_str = self.model_dump_json(by_alias=True)
        return json.loads(json_str.replace(FILENAME_VARIABLE, filename))


class FileMetadataTemplate(FileTemplate):
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

    def create_instance(self, filepath: Path) -> dict[str, Any]:
        return self.template.create_instance(filepath.name)


class TemplateNodeId(SelectorObject):
    space: str
    external_id: str

    @field_validator("external_id")
    @classmethod
    def _validate_filename_in_fields(cls, v: str) -> str:
        if FILENAME_VARIABLE not in v:
            raise ValueError(
                f"{FILENAME_VARIABLE!s} must be present in 'external_id' field. "
                f"This allows for dynamic substitution based on the file name."
            )
        return v


class FileDataModelingTemplate(FileTemplate):
    instance_id: TemplateNodeId

    @model_validator(mode="before")
    def _move_space_external_id(cls, data: dict[str, Any]) -> dict[str, Any]:
        if "space" in data and "externalId" in data:
            data["instanceId"] = {"space": data.pop("space"), "externalId": data.pop("external_id")}
        elif "space" in data and "external_id" in data:
            data["instance_id"] = {"space": data.pop("space"), "external_id": data.pop("external_id")}
        return data


class FileDataModelingTemplateSelector(FileContentSelector):
    type: Literal["fileDataModelingTemplate"] = "fileDataModelingTemplate"
    view_id: SelectedView = SelectedView(space="cdf_cdm", external_id="CogniteFile", version="v1")
    template: FileDataModelingTemplate

    @property
    def group(self) -> str:
        return "FileDataModeling"

    def __str__(self) -> str:
        return "data_modeling_template"

    def create_instance(self, filepath: Path) -> dict[str, Any]:
        return self.template.create_instance(filepath.name)

import json
import mimetypes
from abc import ABC
from pathlib import Path
from typing import Any, Literal

from pydantic import ConfigDict, DirectoryPath, Field, field_validator

from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FILEPATH

from ._base import DataSelector, SelectorObject

FILENAME_VARIABLE = "$FILENAME"


class InternalWithNameId(InternalId):
    name: str = Field(exclude=True, description="The name of the item.")

    @property
    def display_name(self) -> str:
        return f"{self.name} (id={self.id})"


class FileMetadataTemplateV2(SelectorObject):
    model_config = ConfigDict(extra="allow")
    name: str
    external_id: str

    def create_instance(self, filepath: Path, guess_mime_type: bool) -> dict[str, Any]:
        json_str = self.model_dump_json(by_alias=True)
        output = json.loads(json_str.replace(FILENAME_VARIABLE, filepath.name))
        output[FILEPATH] = filepath
        if "mimeType" not in output and guess_mime_type:
            me_type, _ = mimetypes.guess_type(filepath)
            output["mimeType"] = me_type
        return output

    @field_validator("name", "external_id")
    @classmethod
    def _validate_filename_in_fields(cls, v: str) -> str:
        if FILENAME_VARIABLE not in v:
            raise ValueError(
                f"{FILENAME_VARIABLE!s} must be present in 'name' and 'external_id' fields. "
                f"This allows for dynamic substitution based on the file name."
            )
        return v


class FileMetadataContentSelectorV2(DataSelector, ABC):
    kind: Literal["FileMetadataContent"] = "FileMetadataContent"


class FileMetadataTemplateSelectorV2(FileMetadataContentSelectorV2):
    type: Literal["FileMetadataTemplate"] = "FileMetadataTemplate"
    template: FileMetadataTemplateV2
    file_directory: DirectoryPath
    guess_mime_type: bool

    def __str__(self) -> str:
        return self.type

    def find_data_files(self, input_dir: Path, manifest_file: Path) -> list[Path]:
        return [file for file in self.file_directory.iterdir() if file.is_file()]


class FileMetadataFilesSelectorV2(FileMetadataContentSelectorV2):
    """Download/upload individual files.

    For download, the ids field must be set.
    For upload, all files in a csv/parquet file are uploaded.

    """

    type: Literal["FileMetadataFiles"] = "FileMetadataFiles"
    ids: tuple[InternalWithNameId, ...] | None = Field(None, exclude=True, description="Only used for download")

    def __str__(self) -> str:
        return self.type

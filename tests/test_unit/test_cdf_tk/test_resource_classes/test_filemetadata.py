from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import FileMetadataYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_filemetadata_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "MyFile"}, {"Missing required field: 'externalId'"}, id="Missing required field: externalId"
    )
    yield pytest.param(
        {"externalId": "my_file", "name": "TextFile", "mimeType": True},
        {
            "In field mimeType input should be a valid string. Got True of type bool. "
            "Hint: Use double quotes to force string."
        },
        id="Invalid mimeType type",
    )


class TestFileMetadataYAML:
    @pytest.mark.parametrize("data", list(find_resources("FileMetadata")))
    def test_load_valid_file_metadata(self, data: dict[str, object]) -> None:
        loaded = FileMetadataYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_filemetadata_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, FileMetadataYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

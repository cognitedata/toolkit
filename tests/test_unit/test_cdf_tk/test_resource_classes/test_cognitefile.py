from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.cognitefile import CogniteFileYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def valid_cognitefile_test_cases() -> Iterable:
    yield pytest.param({"space": "my_space", "externalId": "my_file_123"}, id="minimal-valid-file")
    yield pytest.param(
        {
            "space": "production_space",
            "externalId": "complete_file_456",
            "name": "Complete Test File",
            "description": "Test file with all properties",
            "tags": ["test", "documentation", "example"],
            "aliases": ["test_file", "sample_file"],
            "sourceId": "source_123",
            "sourceContext": "production_system",
            "source": {"space": "source_space", "externalId": "source_ref_123"},
            "sourceCreatedTime": datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            "sourceUpdatedTime": datetime(2025, 8, 1, 15, 45, 30, tzinfo=timezone.utc),
            "sourceCreatedUser": "user123",
            "sourceUpdatedUser": "user456",
            "assets": [
                {"space": "asset_space", "externalId": "asset_1"},
                {"space": "asset_space", "externalId": "asset_2"},
            ],
            "mimeType": "application/pdf",
            "directory": "/documents/reports/",
            "category": {"space": "category_space", "externalId": "document_category"},
            "existingVersion": 3,
            "type": {"space": "type_space", "externalId": "report_type"},
            "nodeSource": {"type": "view", "space": "view_space", "externalId": "file_view", "version": "v1"},
            "extraProperties": {"custom_field": "custom_value", "priority": 5, "is_public": True},
        },
        id="complete-file-with-all-fields",
    )
    yield pytest.param(
        {
            "space": "test_space",
            "externalId": "data_types_test",
            "name": "Data Types Test",
            "existingVersion": 0,
            "extraProperties": {"number": 42, "boolean": False, "nested": {"key": "value"}},
        },
        id="file-with-various-data-types",
    )


def invalid_cognitefile_test_cases() -> Iterable:
    yield pytest.param({"externalId": "my_file"}, {"Missing required field: 'space'"}, id="missing-required-field")
    yield pytest.param(
        {"space": "my_space", "externalId": "my_file", "sourceCreatedTime": "invalid_date"},
        {"In field sourceCreatedTime input should be a valid datetime. Got 'invalid_date' of type str."},
        id="invalid-sourceCreatedTime-format",
    )
    yield pytest.param(
        {"space": "my_space", "externalId": "my_file", "source": {"space": "source_space"}},
        {"In source missing required field: 'externalId'"},
        id="invalid-DirectRelationReference-missing-externalId",
    )
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "my_file",
            "nodeSource": {"type": "view", "space": "view_space", "externalId": "file_view"},
        },
        {"In nodeSource missing required field: 'version'"},
        id="invalid-ViewReference-missing-version",
    )
    yield pytest.param(
        {"space": "my_space", "externalId": "my_file", "existingVersion": "not_a_number"},
        {"In field existingVersion input should be a valid integer. Got 'not_a_number' of type str."},
        id="invalid-existingVersion-type",
    )


class TestCogniteFileYAML:
    @pytest.mark.parametrize("data", list(find_resources("CogniteFile")))
    def test_load_valid_cognitefile_file(self, data: dict) -> None:
        loaded = CogniteFileYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", list(valid_cognitefile_test_cases()))
    def test_load_valid_cognitefile(self, data: dict) -> None:
        loaded = CogniteFileYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_cognitefile_test_cases()))
    def test_invalid_cognitefile(self, data: dict, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, CogniteFileYAML, Path("test_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors

from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.migration import ResourceViewMappingYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic


def invalid_mapping_test_cases() -> Iterable:
    yield pytest.param(
        {"viewId": {"space": "cdf_cdm", "externalId": "CogniteAsset", "version": "v1"}},
        {"Missing required field: 'propertyMapping'", "Missing required field: 'resourceType'"},
        id="Missing required field: externalId",
    )
    yield pytest.param(
        {
            "resourceType": "asset",
            "viewId": {"externalId": "CogniteAsset", "version": 123},
            "propertyMapping": {
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "notA(Json).path": "invalid",
            },
        },
        {
            "In field propertyMapping invalid JSON paths: notA(Json).path",
            "In viewId missing required field: 'space'",
            "In viewId.version input should be a valid string. Got 123 of type int. Hint: "
            "Use double quotes to force string.",
        },
        id="Unused field: dataSetId and missing name",
    )


def valid_label_test_cases() -> Iterable:
    yield pytest.param(
        {
            "resourceType": "asset",
            "viewId": {"space": "cdf_cdm", "externalId": "CogniteAsset", "version": "v1"},
            "propertyMapping": {
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
            },
        },
        id="Default CogniteAsset mapping",
    )
    yield pytest.param(
        {
            "resourceType": "asset",
            "viewId": {"space": "my_schema_space", "externalId": "MyAsset", "version": "v1"},
            "propertyMapping": {
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "metadata.customField": "customField",
                "metadata.anotherField": "anotherField",
            },
        },
        id="Custom Asset mapping",
    )
    yield pytest.param(
        {
            "resourceType": "asset",
            "viewId": {"space": "my_schema_space", "externalId": "MyAsset", "version": "v1"},
            "propertyMapping": {
                "name": "name",
                "description": "description",
                "source": "source",
                "labels[0]": "category",
                "labels[1]": "subcategory",
                "metadata.customField": "customField",
            },
        },
        id="Custom Asset mapping with array JSON path",
    )


class TestResourceViewMappingYAML:
    @pytest.mark.parametrize("data", list(valid_label_test_cases()))
    def test_load_valid_mapping(self, data: dict[str, object]) -> None:
        loaded = ResourceViewMappingYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_mapping_test_cases()))
    def test_invalid_mapping_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, ResourceViewMappingYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

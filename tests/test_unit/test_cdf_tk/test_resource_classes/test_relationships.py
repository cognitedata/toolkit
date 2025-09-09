from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.resource_classes.relationship import RelationshipYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_relationship_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "myRelationship", "targetExternalId": "target", "sourceExternalId": "source"},
        {"Missing required field: 'sourceType'", "Missing required field: 'targetType'"},
        id="Missing required fields",
    )
    yield pytest.param(
        {"externalId": "equipment:pump", "dataSetId": 123},
        {
            "Missing required field: 'sourceExternalId'",
            "Missing required field: 'sourceType'",
            "Missing required field: 'targetExternalId'",
            "Missing required field: 'targetType'",
            "Unused field: 'dataSetId'",
        },
        id="Unused field: dataSetId and missing name",
    )


class TestRelationshipYAML:
    @pytest.mark.parametrize("data", list(find_resources("Relationship")))
    def test_load_valid_relationship(self, data: dict[str, Any]) -> None:
        loaded = RelationshipYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_relationship_test_cases()))
    def test_invalid_relationship_error_messages(self, data: dict[str, Any], expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, RelationshipYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

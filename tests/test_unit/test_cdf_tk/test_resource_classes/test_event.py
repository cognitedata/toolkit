from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import EventYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_event_test_cases() -> Iterable:
    yield pytest.param(
        {"type": "Workorder"}, {"Missing required field: 'externalId'"}, id="Missing required field: externalId"
    )
    yield pytest.param(
        {"externalId": "1230098u-12907903", "assetExternalIds": [123, 456], "startTime": "2023-10-01T00:00:00Z"},
        {
            "In assetExternalIds[1] input should be a valid string. Got 123 of type int. "
            "Hint: Use double quotes to force string.",
            "In assetExternalIds[2] input should be a valid string. Got 456 of type int. "
            "Hint: Use double quotes to force string.",
            "In field startTime input should be a valid integer. Got '2023-10-01T00:00:00Z' of type str.",
        },
        id="Invalid data type in assetExternalIds",
    )


class TestEventYAML:
    @pytest.mark.parametrize("data", list(find_resources("Event")))
    def test_load_valid_asset(self, data: dict[str, object]) -> None:
        loaded = EventYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_event_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, EventYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

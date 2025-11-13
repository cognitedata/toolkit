from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.streams import StreamYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic


def invalid_stream_test_cases() -> Iterable:
    yield pytest.param(
        {"settings": {"template": {"name": "ImmutableTestStream"}}},
        {"Missing required field: 'externalId'"},
        id="Missing required field: externalId",
    )
    yield pytest.param(
        {"externalId": "test-stream"},
        {"Missing required field: 'settings'"},
        id="Missing required field: settings",
    )
    yield pytest.param(
        {"externalId": "test-stream", "settings": {"template": {"name": "InvalidTemplate"}}},
        {
            "In settings.template.name input should be 'ImmutableTestStream', 'BasicArchive' or 'BasicLiveData'. Got 'InvalidTemplate'."
        },
        id="Invalid template name",
    )
    yield pytest.param(
        {
            "externalId": "test-stream",
            "settings": {"template": {"name": "ImmutableTestStream"}},
            "unknownField": "value",
        },
        {"Unused field: 'unknownField'"},
        id="Unused field",
    )
    yield pytest.param(
        {"externalId": "test-stream", "settings": {"template": {"names": "ImmutableTestStream"}}},
        {"In settings.template.names[key] input should be 'name'. Got 'names'."},
        id="wrong template dictionary format",
    )
    yield pytest.param(
        {"externalId": "Test-stream", "settings": {"template": {"name": "ImmutableTestStream"}}},
        {"In field externalId string should match pattern '^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$'"},
        id="Invalid external_id: starts with uppercase",
    )
    yield pytest.param(
        {"externalId": "test-stream-", "settings": {"template": {"name": "ImmutableTestStream"}}},
        {"In field externalId string should match pattern '^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$'"},
        id="Invalid external_id: ends with hyphen",
    )
    yield pytest.param(
        {"externalId": "test-stream_", "settings": {"template": {"name": "ImmutableTestStream"}}},
        {"In field externalId string should match pattern '^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$'"},
        id="Invalid external_id: ends with underscore",
    )
    yield pytest.param(
        {"externalId": "1test-stream", "settings": {"template": {"name": "ImmutableTestStream"}}},
        {"In field externalId string should match pattern '^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$'"},
        id="Invalid external_id: starts with number",
    )
    yield pytest.param(
        {"externalId": "t" * 101, "settings": {"template": {"name": "ImmutableTestStream"}}},
        {"In field externalId string should have at most 100 characters"},
        id="externalId-too-long",
    )


class TestStreamYAML:
    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(
                {"externalId": "test-stream", "settings": {"template": {"name": "ImmutableTestStream"}}},
                id="ImmutableTestStream template",
            ),
            pytest.param(
                {"externalId": "archive-stream", "settings": {"template": {"name": "BasicArchive"}}},
                id="BasicArchive template",
            ),
            pytest.param(
                {"externalId": "live-stream", "settings": {"template": {"name": "BasicLiveData"}}},
                id="BasicLiveData template",
            ),
        ],
    )
    def test_valid_streams(self, data: dict) -> None:
        loaded = StreamYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_stream_test_cases()))
    def test_invalid_stream_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, StreamYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

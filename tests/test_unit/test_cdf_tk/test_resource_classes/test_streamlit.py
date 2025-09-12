from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.streamlit_ import StreamlitYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_streamlit_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "MyApp", "name": "MyApp"},
        {"Missing required field: 'creator'"},
        id="Missing required field: creator",
    )
    yield pytest.param(
        {"externalId": "MyApp", "creator": "doctrino", "name": "MyApp", "published": "yes", "draft": "no"},
        {"In field published input should be a valid boolean. Got 'yes' of type str.", "Unused field: 'draft'"},
        id="Invalid boolean and unused field",
    )


class TestStreamlitYAML:
    @pytest.mark.parametrize("data", list(find_resources("Streamlit")))
    def test_load_valid_space(self, data: dict[str, object]) -> None:
        loaded = StreamlitYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_streamlit_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, StreamlitYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

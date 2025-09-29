from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_classes.graphql_model import GraphQLDataModelYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "my_model",
        },
        {"Missing required field: 'version'"},
        id="missing-required-field",
    )
    yield pytest.param(
        {
            "space": "",  # Empty space
            "externalId": "my_model",
            "version": "#1",
        },
        {
            "In field space string should have at least 1 character",
            "In field version string should match pattern '^[a-zA-Z0-9]([.a-zA-Z0-9_-]{0,41}[a-zA-Z0-9])?$'",
        },
        id="invalid space and version",
    )


class TestGraphQLDataModelYAML:
    @pytest.mark.parametrize("data", list(find_resources("GraphQLSchema", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES)))
    def test_load_valid(self, data: dict[str, object]) -> None:
        loaded = GraphQLDataModelYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_model_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, GraphQLDataModelYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

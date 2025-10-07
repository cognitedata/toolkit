from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.resource_classes.infield_v1 import InfieldV1YAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "InfieldApp"},
        {"Missing required field: 'externalId'"},
        id="Missing required field: externalId",
    )


class TestHostedExtractorDestinationYAML:
    @pytest.mark.parametrize("data", list(find_resources("InfieldV1", resource_dir="cdf_applications")))
    def test_load_valid(self, data: dict[str, Any]) -> None:
        loaded = InfieldV1YAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert dumped == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_error_messages(self, data: dict[str, Any], expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, InfieldV1YAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

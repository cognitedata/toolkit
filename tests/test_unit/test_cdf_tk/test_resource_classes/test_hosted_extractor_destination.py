from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.resource_classes.hosted_extractor_destination import HostedExtractorDestinationYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_hosted_extractor_destination_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "Pump"},
        {"Missing required field: 'externalId'", "Unused field: 'name'"},
        id="Missing required field: externalId",
    )
    yield pytest.param(
        {"externalId": "myDestination", "dataSetId": 123},
        {"Unused field: 'dataSetId'"},
        id="Unused field: dataSetId",
    )


class TestHostedExtractorDestinationYAML:
    @pytest.mark.parametrize("data", list(find_resources("Destination", resource_dir="hosted_extractors")))
    def test_load_valid_destination(self, data: dict[str, Any]) -> None:
        loaded = HostedExtractorDestinationYAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        if "credentials" in dumped:
            # Secret is not dumped as per design, so we add it back for comparison
            dumped["credentials"]["clientSecret"] = data["credentials"]["clientSecret"]
        assert dumped == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_hosted_extractor_destination_test_cases()))
    def test_invalid_destination_error_messages(self, data: dict[str, Any], expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, HostedExtractorDestinationYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

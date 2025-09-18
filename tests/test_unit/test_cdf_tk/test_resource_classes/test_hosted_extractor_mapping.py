from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.hosted_extractor_mapping import HostedExtractorMappingYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_hosted_extractor_mapping_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "myMapping", "mapping": "var->to->other", "published": True},
        {
            "In field mapping input must be an object of type Mapping. Got 'var->to->other' of type str.",
        },
        id="Incorrect variables type ",
    )
    yield pytest.param(
        {
            "externalId": "myMapping",
            "mapping": {"expression": "some_expression"},
            "published": True,
            "input": {
                "type": "csv",
                "delimiter": ":,",
            },
        },
        {"In input.delimiter string should have at most 1 character"},
        id="Invalid delimiter in CSV input",
    )


class TestHostedExtractorMappingYAML:
    @pytest.mark.parametrize("data", list(find_resources("Mapping", resource_dir="hosted_extractors")))
    def test_load_valid_hosted_extractor_mapping(self, data: dict[str, object]) -> None:
        loaded = HostedExtractorMappingYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_hosted_extractor_mapping_test_cases()))
    def test_invalid_hosted_extractor_mapping_error_messages(
        self, data: dict | list, expected_errors: set[str]
    ) -> None:
        warning_list = validate_resource_yaml_pydantic(data, HostedExtractorMappingYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

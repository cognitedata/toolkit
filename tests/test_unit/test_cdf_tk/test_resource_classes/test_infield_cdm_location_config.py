from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.resource_classes.infield_cdm_location_config import InFieldCDMLocationConfigYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "MyLocationConfig"},
        {"Missing required field: 'externalId'", "Missing required field: 'space'"},
        id="Missing required fields: externalId and space",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "unknownField": "invalid_value",
            "anotherUnknownField": 123,
            "featureToggles": {
                "threeD": True,
                "invalidToggle": "bad_value",
            },
        },
        {
            "In featureToggles unused field: 'invalidToggle'",
            "Unused field: 'anotherUnknownField'",
            "Unused field: 'unknownField'",
        },
        id="Multiple extra fields at different levels",
    )
    yield pytest.param(
        {
            "externalId": "my_config",
            "space": "my_space",
            "featureToggles": {
                "threeD": "not_a_boolean",
                "trends": 123,
                "observations": {
                    "isEnabled": "not_a_boolean",
                    "isWriteBackEnabled": ["invalid_type"],
                },
            },
            "accessManagement": {
                "templateAdmins": "should_be_a_list",
                "checklistAdmins": 456,
            },
            "dataFilters": {
                "files": {
                    "path": {
                        "space": 123,  # Should be string
                        "externalId": True,  # Should be string
                    },
                    "instanceSpaces": "should_be_list",  # Should be list
                },
            },
            "viewMappings": {
                "asset": {
                    "space": "my_space",
                    "version": "v1",
                    # Missing externalId
                },
            },
            "disciplines": [
                {
                    "name": "Engineering",
                    # Missing externalId
                },
            ],
        },
        {
            "In accessManagement.checklistAdmins input should be a valid list. Got 456.",
            "In accessManagement.templateAdmins input should be a valid list. Got 'should_be_a_list'.",
            "In dataFilters.files.instanceSpaces input should be a valid list. Got 'should_be_list'.",
            "In dataFilters.files.path.externalId input should be a valid string. Got True of type bool. Hint: Use double quotes to force string.",
            "In dataFilters.files.path.space input should be a valid string. Got 123 of type int. Hint: Use double quotes to force string.",
            "In disciplines[1] missing required field: 'externalId'",
            "In featureToggles.observations.isEnabled input should be a valid boolean. "
            "Got 'not_a_boolean' of type str.",
            "In featureToggles.observations.isWriteBackEnabled input should be a valid "
            "boolean. Got ['invalid_type'] of type list.",
            "In featureToggles.threeD input should be a valid boolean. Got 'not_a_boolean' of type str.",
            "In featureToggles.trends input should be a valid boolean. Got 123 of type int.",
            "In viewMappings.asset missing required field: 'externalId'",
        },
        id="Multiple type mismatches across nested structures",
    )


class TestInfieldCDMLocationConfigYAML:
    @pytest.mark.parametrize(
        "data",
        list(
            find_resources("InFieldCDMLocationConfig", resource_dir="cdf_applications", base=COMPLETE_ORG_ALPHA_FLAGS)
        ),
    )
    def test_load_valid(self, data: dict[str, Any]) -> None:
        loaded = InFieldCDMLocationConfigYAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert dumped == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_error_messages(self, data: dict[str, Any], expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, InFieldCDMLocationConfigYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

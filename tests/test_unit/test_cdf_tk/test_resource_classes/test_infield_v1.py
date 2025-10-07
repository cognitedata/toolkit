from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.resource_classes.infield_v1 import InfieldV1YAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "InfieldApp"},
        {"Missing required field: 'externalId'"},
        id="Missing required field: externalId",
    )
    yield pytest.param(
        {
            "externalId": 123,  # Should be string
            "name": 456,  # Should be string
            "appDataSpaceVersion": [],  # Should be string
            "dataSetId": "invalid",  # Unknown field
        },
        {
            "In field externalId input should be a valid string. Got 123 of type int. Hint: Use double quotes to force string.",
            "In field name input should be a valid string. Got 456 of type int. Hint: Use double quotes to force string.",
            "In field appDataSpaceVersion input should be a valid string. Got [] of type list. Hint: Use double quotes to force string.",
            "Unused field: 'dataSetId'",
        },
        id="Multiple type validation errors",
    )
    yield pytest.param(
        {
            "externalId": "test-config",
            "featureConfiguration": {
                "rootLocationConfigurations": [
                    {
                        "dataSetExternalId": True,  # Should be string
                        "templateAdmins": "not-a-list",  # Should be list
                        "threeDConfiguration": {
                            "fullWeightModels": [
                                {
                                    "revisionId": "not-an-int",  # Should be int
                                    "modelId": "also-not-an-int",  # Should be int
                                }
                            ]
                        },
                        "featureToggles": {
                            "threeD": "not-a-bool",  # Should be bool
                            "observations": {
                                "isEnabled": "not-a-bool",  # Should be bool
                            },
                        },
                    }
                ]
            },
        },
        {
            "In featureConfiguration.rootLocationConfigurations[1].dataSetExternalId "
            "input should be a valid string. Got True of type bool. Hint: Use double "
            "quotes to force string.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].featureToggles.observations.isEnabled "
            "input should be a valid boolean. Got 'not-a-bool' of type str.",
            "In featureConfiguration.rootLocationConfigurations[1].featureToggles.threeD "
            "input should be a valid boolean. Got 'not-a-bool' of type str.",
            "In featureConfiguration.rootLocationConfigurations[1].templateAdmins input "
            "should be a valid list. Got 'not-a-list'.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].threeDConfiguration.fullWeightModels[1].modelId "
            "input should be a valid integer. Got 'also-not-an-int' of type str.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].threeDConfiguration.fullWeightModels[1].revisionId "
            "input should be a valid integer. Got 'not-an-int' of type str.",
        },
        id="Nested structure validation errors",
    )
    yield pytest.param(
        {
            "externalId": "test-config",
            "featureConfiguration": {
                "rootLocationConfigurations": [
                    {
                        "observations": {
                            "type": {
                                "options": [
                                    {
                                        "id": 123,  # Should be string
                                        "value": [],  # Should be string
                                        "label": {"invalid": "object"},  # Should be string
                                    }
                                ]
                            }
                        },
                        "dataFilters": {
                            "general": {
                                "dataSetExternalIds": [1, 32],  # Wrong field name, should be data_set_ids
                                "spaces": 42,  # Should be list of strings
                            }
                        },
                    }
                ]
            },
        },
        {
            "In "
            "featureConfiguration.rootLocationConfigurations[1].dataFilters.general.dataSetExternalIds[1] "
            "input should be a valid string. Got 1 of type int. Hint: Use double quotes "
            "to force string.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].dataFilters.general.dataSetExternalIds[2] "
            "input should be a valid string. Got 32 of type int. Hint: Use double quotes "
            "to force string.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].dataFilters.general.spaces "
            "input should be a valid list. Got 42.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].observations.type.options[1].id "
            "input should be a valid string. Got 123 of type int. Hint: Use double quotes "
            "to force string.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].observations.type.options[1].label "
            "input should be a valid string. Got {'invalid': 'object'} of type dict. "
            "Hint: Use double quotes to force string.",
            "In "
            "featureConfiguration.rootLocationConfigurations[1].observations.type.options[1].value "
            "input should be a valid string. Got [] of type list. Hint: Use double quotes "
            "to force string.",
        },
        id="Deep nested validation with complex structures",
    )


class TestHostedExtractorDestinationYAML:
    @pytest.mark.parametrize(
        "data", list(find_resources("InfieldV1", resource_dir="cdf_applications", base=COMPLETE_ORG_ALPHA_FLAGS))
    )
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
